# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 02 — Silver Layer · Triangulation Engine [UC Version]
# MAGIC **Purpose:** Process Bronze UC tables and triangulate 3 signals to compute a high-confidence parametric trigger.
# MAGIC 
# MAGIC ### 🐛 Update (v3.0): 
# MAGIC Migrated to **Unity Catalog** and implemented **Z-Score Anomaly Detection** for higher statistical precision.
# MAGIC 
# MAGIC | Signal | Component | Source UC Table | Weight |
# MAGIC |--------|-----------|-----------------|--------|
# MAGIC | Signal A | Weather Anomaly | `bronze_imd_rainfall` | 50% |
# MAGIC | Signal B | Market Stress | `bronze_mandi_prices` | 25% |
# MAGIC | Signal C | Social Distress | `bronze_kcc_2022` | 25% |

# COMMAND ----------

# ── UC & Statistical Parameter Widgets ───────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "krishi_kavach", "Schema Name")

dbutils.widgets.text("confidence_threshold", "0.6", "Min Confidence Score (0.0–1.0)")
dbutils.widgets.text("rain_spike_z", "1.5", "Rain Spike Z-Score (e.g. 1.5 sigma)")
dbutils.widgets.text("rain_drought_z", "1.0", "Dry Spell Z-Score (e.g. 1.0 sigma)")
dbutils.widgets.text("price_spike_z", "1.5", "Price Spike Z-Score (e.g. 1.5 sigma)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FULL_SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

CONF_THRESH  = float(dbutils.widgets.get("confidence_threshold"))
RAIN_SPIKE_Z = float(dbutils.widgets.get("rain_spike_z"))
RAIN_DRY_Z   = float(dbutils.widgets.get("rain_drought_z"))
PRICE_SPIKE_Z = float(dbutils.widgets.get("price_spike_z"))

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load & Pre-process Bronze UC Tables

# COMMAND ----------

df_rain_raw  = spark.table(f"{FULL_SCHEMA_PATH}.bronze_district_daily_rainfall")
df_mandi_raw = spark.table(f"{FULL_SCHEMA_PATH}.bronze_mandi_prices")
df_kcc_raw   = spark.table(f"{FULL_SCHEMA_PATH}.bronze_kcc_2022")

month_map = F.create_map([F.lit(x) for x in [
    "Jan", "1", "Feb", "2", "Mar", "3", "Apr", "4", "May", "5", "Jun", "6",
    "Jul", "7", "Aug", "8", "Sep", "9", "Oct", "10", "Nov", "11", "Dec", "12"
]])

df_rain_processed = (
    df_rain_raw
    .withColumn("month_int", month_map[F.col("month")].cast("int"))
    .withColumn("event_date", F.make_date(F.col("year").cast("int"), F.col("month_int"), F.col("day").cast("int")))
    .withColumn("date_int",   F.col("event_date").cast("long")) 
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

df_mandi_processed = (
    df_mandi_raw
    .withColumn("event_date", F.to_date(F.col("date")))
    .withColumn("date_int",   F.col("event_date").cast("long"))
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

df_kcc_processed = (
    df_kcc_raw
    .withColumn("event_date", F.to_date(F.col("date")))
    .withColumn("date_int",   F.col("event_date").cast("long"))
    .withColumn("district_key", F.upper(F.trim(F.col("district"))))
    .dropna(subset=["event_date", "district_key"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Signal A: Weather Score (50%) - Z-Score Anomaly

# COMMAND ----------

window_7d = Window.partitionBy("state", "district").orderBy("date_int").rowsBetween(-6, 0)

df_signal_a = (
    df_rain_processed
    .withColumn("rain_7d_avg", F.avg("rainfall_mm").over(window_7d))
    .withColumn("rain_7d_stddev", F.stddev("rainfall_mm").over(window_7d))
    .withColumn("rain_score", 
        F.when(
            (F.col("rain_7d_stddev").isNotNull()) & (F.col("rain_7d_stddev") > 0) & 
            (F.col("rainfall_mm") > (F.col("rain_7d_avg") + RAIN_SPIKE_Z * F.col("rain_7d_stddev"))), 1.0) # Extreme Spike
        .when(
            (F.col("rain_7d_stddev").isNotNull()) & (F.col("rain_7d_stddev") > 0) & 
            (F.col("rainfall_mm") < (F.col("rain_7d_avg") - RAIN_DRY_Z * F.col("rain_7d_stddev"))), 0.8) # Severe Dry Spell
        .otherwise(0.0)
    )
    .select("state", "district", "district_key", "event_date", "date_int", "rainfall_mm", "rain_7d_avg", "rain_7d_stddev", "rain_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Signal B: Market Stress Score (25%) - Z-Score Anomaly

# COMMAND ----------

window_30d = Window.partitionBy("district_key", "commodity").orderBy("date_int").rowsBetween(-29, 0)

df_mandi_base = (
    df_mandi_processed
    .withColumn("price_30d_avg",   F.avg("modal_price").over(window_30d))
    .withColumn("price_30d_stddev", F.stddev("modal_price").over(window_30d))
    .withColumn("arrival_30d_avg", F.avg("arrivals_tonnes").over(window_30d))
    .withColumn("arrival_30d_stddev", F.stddev("arrivals_tonnes").over(window_30d))
    
    .withColumn("price_spike_flag", F.when(
        (F.col("price_30d_stddev").isNotNull()) & (F.col("price_30d_stddev") > 0) & 
        (F.col("modal_price") > (F.col("price_30d_avg") + PRICE_SPIKE_Z * F.col("price_30d_stddev"))), 1.0).otherwise(0.0))
        
    .withColumn("arrival_dip_flag", F.when(
        (F.col("arrival_30d_stddev").isNotNull()) & (F.col("arrival_30d_stddev") > 0) & 
        (F.col("arrivals_tonnes") < (F.col("arrival_30d_avg") - 1.0 * F.col("arrival_30d_stddev"))), 1.0).otherwise(0.0))
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
# MAGIC ## 5. Signal Triangulation & Confidence Score

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
    .withColumn("is_valid_trigger", F.when(F.col("confidence_score") >= CONF_THRESH, True).otherwise(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Silver UC Table

# COMMAND ----------

df_confirmed = (
    df_triangulated
    .filter(F.col("is_valid_trigger") == True)
    .orderBy(F.col("confidence_score").desc())
)

table_silver = f"{FULL_SCHEMA_PATH}.silver_confirmed_triggers"

(df_confirmed.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(table_silver))

print(f"✅ SILVER UC MIGRATION COMPLETE: {table_silver}")
print(f"📊 TOTAL VALID TRIGGERS : {df_confirmed.count():,}")
display(df_confirmed.limit(20))
