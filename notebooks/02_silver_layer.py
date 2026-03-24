# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 02 — Silver Layer · Triangulation Engine (Production)
# MAGIC **Purpose:** Process Bronze tables and triangulate 3 signals (Weather, Market, Social) to compute a high-confidence parametric insurance trigger.
# MAGIC 
# MAGIC ### 🐛 Bugfix Note (v2.2): 
# MAGIC Fixed alphabetical month sorting bug. Switched to `date_int` (epoch) for the 7-day rolling window to guarantee chronological integrity.
# MAGIC 
# MAGIC | Signal | Component | Source Table | Weight |
|--------|-----------|--------------|--------|
| Signal A | Weather Anomaly | `imd_rainfall` | 50% |
| Signal B | Market Stress | `mandi_prices` | 25% |
| Signal C | Social Distress | `kcc_2022` | 25% |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Constants ─────────────────────────────────────────────────────────────────
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"
SILVER_SINK = "dbfs:/FileStore/krishi_kavach/silver/confirmed_triggers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load & Pre-process Datasets (Harmonize District & Date)

# COMMAND ----------

# ── Load Bronze Tables ────────────────────────────────────────────────────────
df_rain_raw  = spark.read.format("delta").load(f"{BRONZE_BASE}/district_daily_rainfall")
df_mandi_raw = spark.read.format("delta").load(f"{BRONZE_BASE}/mandi_prices")
df_kcc_raw   = spark.read.format("delta").load(f"{BRONZE_BASE}/kcc_2022")

# ── Month Abbreviation Mapping ───────────────────────────────────────────────
month_map = F.create_map([F.lit(x) for x in [
    "Jan", "1", "Feb", "2", "Mar", "3", "Apr", "4", "May", "5", "Jun", "6",
    "Jul", "7", "Aug", "8", "Sep", "9", "Oct", "10", "Nov", "11", "Dec", "12"
]])

# ── Standardize Rainfall (Signal A) ──────────────────────────────────────────
# Bugfix: Construct full event_date and date_int (epoch) for chronological sorting
df_rain_processed = (
    df_rain_raw
    .withColumn("month_int", month_map[F.col("month")].cast("int"))
    .withColumn("event_date", F.make_date(F.col("year").cast("int"), F.col("month_int"), F.col("day").cast("int")))
    .withColumn("date_int",   F.col("event_date").cast("long")) # Epoch seconds for true sort
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

# ── Standardize Mandi Prices (Signal B) ───────────────────────────────────────
df_mandi_processed = (
    df_mandi_raw
    .withColumn("event_date", F.to_date(F.col("date")))
    .withColumn("date_int",   F.col("event_date").cast("long"))
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

# ── Standardize KCC Queries (Signal C) ────────────────────────────────────────
df_kcc_processed = (
    df_kcc_raw
    .withColumn("event_date", F.to_date(F.col("date")))
    .withColumn("date_int",   F.col("event_date").cast("long"))
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Signal A: Weather Score (50%)

# COMMAND ----------

# Window: Fixed Spec using date_int to avoid alphabetical month sorting bugs
window_7d = Window.partitionBy("state", "district").orderBy("date_int").rowsBetween(-6, 0)

df_signal_a = (
    df_rain_processed
    .withColumn("rain_7d_avg", F.avg("rainfall_mm").over(window_7d))
    .withColumn("rain_score", 
        F.when(F.col("rainfall_mm") > (F.col("rain_7d_avg") * 2.0), 1.0) # Extreme Spike
        .when(F.col("rainfall_mm") < (F.col("rain_7d_avg") * 0.1), 0.8) # Severe Dry Spell
        .otherwise(0.0)
    )
    .select("state", "district", "district_key", "event_date", "date_int", "rainfall_mm", "rain_7d_avg", "rain_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Signal B: Market Stress Score (25%)

# COMMAND ----------

# Window: partition by district_key and commodity
window_30d = Window.partitionBy("district_key", "commodity").orderBy("date_int").rowsBetween(-29, 0)

df_mandi_base = (
    df_mandi_processed
    .withColumn("price_30d_avg",   F.avg("modal_price").over(window_30d))
    .withColumn("arrival_30d_avg", F.avg("arrivals_tonnes").over(window_30d))
    .withColumn("price_spike_flag", F.when(F.col("modal_price")     > (F.col("price_30d_avg")   * 1.3), 1.0).otherwise(0.0))
    .withColumn("arrival_dip_flag", F.when(F.col("arrivals_tonnes") < (F.col("arrival_30d_avg") * 0.5), 1.0).otherwise(0.0))
    .withColumn("raw_price_score", (F.col("price_spike_flag") * 0.5) + (F.col("arrival_dip_flag") * 0.5))
)

df_signal_b = (
    df_mandi_base
    .groupBy("district_key", "event_date")
    .agg(F.max("raw_price_score").alias("price_score"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Signal C: Social Distress Score (25%)

# COMMAND ----------

df_signal_c = (
    df_kcc_processed
    .groupBy("district_key", "event_date")
    .agg(F.count("*").alias("stress_query_count"))
    .withColumn("kcc_stress_score", 
        F.when(F.col("stress_query_count") >= 10, 1.0)
        .when(F.col("stress_query_count") >= 5,  0.6)
        .when(F.col("stress_query_count") >= 2,  0.3)
        .otherwise(0.0)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fixed Join & Confidence Computation

# COMMAND ----------

df_triangulated = (
    df_signal_a.alias("a")
    .join(df_signal_b.alias("b"), ["district_key", "event_date"], "left")
    .join(df_signal_c.alias("c"), ["district_key", "event_date"], "left")
    .fillna(0.0, subset=["price_score", "kcc_stress_score", "stress_query_count"])
    .withColumn("confidence_score", 
        F.round(
            (F.col("rain_score") * 0.50) + 
            (F.col("price_score") * 0.25) + 
            (F.col("kcc_stress_score") * 0.25), 
        4)
    )
    .withColumn("is_valid_trigger", F.when(F.col("confidence_score") >= 0.60, True).otherwise(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Confirmed Triggers

# COMMAND ----------

df_confirmed = (
    df_triangulated
    .filter(F.col("is_valid_trigger") == True)
    .orderBy(F.col("confidence_score").desc())
)

(df_confirmed.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(SILVER_SINK))

print(f"✅  SILVER BUGFIX (v2.2) COMPLETE")
print(f"📊  TOTAL VALID TRIGGERS : {df_confirmed.count():,}")
display(df_confirmed.limit(20))
