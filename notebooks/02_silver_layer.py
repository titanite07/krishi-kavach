# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 02 — Silver Layer · Triangulation Engine (Production)
# MAGIC **Purpose:** Process Bronze tables and triangulate 3 signals (Weather, Market, Social) to compute a high-confidence parametric insurance trigger.
| Signal | Component | Source Table | Weight |
|--------|-----------|--------------|--------|
| Signal A | Weather Anomaly | `imd_rainfall` | 50% |
| Signal B | Market Stress | `mandi_prices` | 25% |
| Signal C | Social Distress | `kcc_2022` | 25% |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.functions import (
    col, when, avg, count, max as spark_max, 
    lit, round as spark_round, expr
)
from pyspark.sql.window import Window

# ── Constants ─────────────────────────────────────────────────────────────────
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"
SILVER_SINK = "dbfs:/FileStore/krishi_kavach/silver/confirmed_triggers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Signal A: Weather Score (50%)
# MAGIC *Logic: Detect relative rainfall spikes or drought events against a 7-day rolling baseline.*

# COMMAND ----------

df_imd = spark.read.format("delta").load(f"{BRONZE_BASE}/imd_rainfall")

# Window: partition by district, order by date, look back 6 days + current day
window_7d = Window.partitionBy("district").orderBy("date").rowsBetween(-6, 0)

df_signal_a = (
    df_imd
    .withColumn("rain_7d_avg", avg("rainfall_mm").over(window_7d))
    .withColumn("rain_score", 
        when(col("rainfall_mm") > (col("rain_7d_avg") * 2.0), 1.0)      # Extreme Spike
        .when(col("rainfall_mm") < (col("rain_7d_avg") * 0.1), 0.8)      # Severe Dry Spell
        .otherwise(0.0)
    )
    .select("district", "date", "rainfall_mm", "rain_7d_avg", "rain_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Signal B: Market Stress Score (25%)
# MAGIC *Logic: Monitor Mandi price spikes and volume drops against a 30-day rolling baseline.*

# COMMAND ----------

df_mandi = spark.read.format("delta").load(f"{BRONZE_BASE}/mandi_prices")

# Window: partition by district and commodity for accurate historical commodity trends
window_30d = Window.partitionBy("district", "commodity").orderBy("date").rowsBetween(-29, 0)

df_mandi_base = (
    df_mandi
    .withColumn("price_30d_avg",   avg("modal_price").over(window_30d))
    .withColumn("arrival_30d_avg", avg("arrivals_tonnes").over(window_30d))
    # Flags: High Price (>30%) or Low Volume (<50%)
    .withColumn("price_spike_flag", when(col("modal_price")     > (col("price_30d_avg")   * 1.3), 1.0).otherwise(0.0))
    .withColumn("arrival_dip_flag", when(col("arrivals_tonnes") < (col("arrival_30d_avg") * 0.5), 1.0).otherwise(0.0))
    # Combine signals per market entry
    .withColumn("raw_price_score", (col("price_spike_flag") * 0.5) + (col("arrival_dip_flag") * 0.5))
)

# Aggregate to District+Date level (taking the highest stress across commodities)
df_signal_b = (
    df_mandi_base
    .groupBy("district", "date")
    .agg(spark_max("raw_price_score").alias("price_score"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Signal C: Social Distress Score (25%)
# MAGIC *Logic: Measure farmer inquiry volume relating to weather/pest/disease categories.*

# COMMAND ----------

df_kcc = spark.read.format("delta").load(f"{BRONZE_BASE}/kcc_2022")

stress_categories = ['Weather', 'Pest', 'Disease', 'Flood']

df_signal_c = (
    df_kcc
    .filter(col("category").isin(stress_categories))
    .groupBy("district", "date")
    .agg(count("*").alias("stress_query_count"))
    # Conditional scoring based on inquiry density
    .withColumn("kcc_stress_score", 
        when(col("stress_query_count") >= 10, 1.0)
        .when(col("stress_query_count") >= 5,  0.6)
        .when(col("stress_query_count") >= 2,  0.3)
        .otherwise(0.0)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final Triangulation & Confidence Scoring

# COMMAND ----------

# Join strategy: Outer join on district+date, using Rainfall as primary time-series spine
df_triangulated = (
    df_signal_a.alias("a")
    .join(df_signal_b.alias("b"), ["district", "date"], "left")
    .join(df_signal_c.alias("c"), ["district", "date"], "left")
    # Fill missing scores with 0.0 assuming no signal = no stress
    .fillna(0.0, subset=["price_score", "kcc_stress_score", "stress_query_count"])
    # Weighted Multi-Signal Computation
    .withColumn("confidence_score", 
        spark_round(
            (col("rain_score") * 0.50) + 
            (col("price_score") * 0.25) + 
            (col("kcc_stress_score") * 0.25), 
        4)
    )
    .withColumn("is_valid_trigger", when(col("confidence_score") >= 0.60, True).otherwise(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Confirmed Triggers & Reporting

# COMMAND ----------

# Filter only validated trigger events
df_confirmed = (
    df_triangulated
    .filter(col("is_valid_trigger") == True)
    .orderBy(col("confidence_score").desc())
)

# Write to Silver layer
(df_confirmed.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(SILVER_SINK))

# ── Summary Reports ──────────────────────────────────────────────────────────
total_confirmed = df_confirmed.count()
print(f"✅  SILVER LAYER COMPLETE")
print(f"📊  TOTAL CONFIRMED TRIGGERS : {total_confirmed:,}")

print("\n🚀 TOP 20 CONFIDENCE TRIGGERS:")
display(df_confirmed.limit(20))

print("\n📍 BREAKDOWN: TRIGGERS PER DISTRICT:")
df_breakdown = df_confirmed.groupBy("district").count().orderBy("count", ascending=False)
display(df_breakdown)
