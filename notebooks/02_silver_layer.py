# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 02 — Silver Layer Enrichment (Multi-Signal)
# MAGIC **Purpose:** Process Bronze tables, harmonize district names, and compute a multi-signal Confidence Score for parametric insurance triggers.
# MAGIC | Signal | Layer | Weight |
# MAGIC |--------|-------|--------|
# MAGIC | Rainfall Anomaly | Daily Rainfall | 0.50 |
# MAGIC | Price Anomaly | Mandi Prices | 0.25 |
# MAGIC | Inquiry Anomaly | KCC Queries | 0.25 |

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, avg, stddev, count, sum as spark_sum,
    to_date, concat_ws, lit, expr
)
from pyspark.sql import Window

# ── Constants ─────────────────────────────────────────────────────────────────
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"
SILVER_PATH = "dbfs:/FileStore/krishi_kavach/silver/events_confirmed"
CONFIDENCE_THRESHOLD = 0.6

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Tables

# COMMAND ----------

# COMMAND ----------

df_rain = spark.read.format("delta").load(f"{BRONZE_BASE}/district_daily_rainfall")
df_mandi = spark.read.format("delta").load(f"{BRONZE_BASE}/mandi_prices")
df_kcc = spark.read.format("delta").load(f"{BRONZE_BASE}/kcc_2022")
df_policy = spark.read.format("delta").load(f"{BRONZE_BASE}/pmfby_policy")
df_agmarknet = spark.read.format("delta").load(f"{BRONZE_BASE}/agmarknet_reference")

print(f"📥  Rainfall (bronze) : {df_rain.count():,} rows")
print(f"📥  Mandi    (bronze) : {df_mandi.count():,} rows")
print(f"📥  KCC      (bronze) : {df_kcc.count():,} rows")

# MAGIC %md
# MAGIC ## 2. Process Rainfall Anomalies (Dry Spells)

# COMMAND ----------

# Construct Date: month (Jan, Feb...) to number, then concat
# Assuming daily_rainfall has 'month' as name and 'day' as int
# For simplicity, we'll treat 2022 as the reference year
df_rain_events = (
    df_rain
    .withColumn("is_dry_day", when(col("rainfall_mm") < 2.0, 1).otherwise(0))
    # Window to find rolling dry days
    .withColumn("dry_spell_score", spark_sum("is_dry_day").over(
        Window.partitionBy("state", "district").orderBy("month", "day").rowsBetween(-7, 0)
    ) / 7.0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Process Mandi Price Anomalies

# COMMAND ----------

# Calculate district-commodity averages
window_dist_comm = Window.partitionBy("District", "Commodity")

df_price_events = (
    df_mandi
    .withColumn("avg_modal_price", avg("Modal_Price").over(window_dist_comm))
    .withColumn("price_spike_ratio", col("Modal_Price") / col("avg_modal_price"))
    .withColumn("price_score", when(col("price_spike_ratio") > 1.2, 1.0).otherwise(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Process KCC Query Stress

# COMMAND ----------

# High volume of weather/pest related queries
df_kcc_events = (
    df_kcc
    .filter(col("QueryType").rlike("(?i)weather|rain|pest|disease|loss"))
    .groupBy("StateName", "DistrictName", "Year", "Month", "Day")
    .agg(count("*").alias("query_volume"))
    # Window for anomalies
    .withColumn("avg_vol", avg("query_volume").over(Window.partitionBy("StateName", "DistrictName")))
    .withColumn("kcc_score", when(col("query_volume") > (col("avg_vol") * 1.5), 1.0).otherwise(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Harmonized Join & Scoring

# COMMAND ----------

# Note: In production, we'd use a fuzzy-matcher for DistrictName. 
# Here we assume UpperCase join for better matching.

df_final = (
    df_rain_events.alias("r")
    .join(df_price_events.alias("p"), 
          (expr("upper(r.district) = upper(p.District)")) & 
          (col("r.day") == dayofmonth(col("p.Arrival_Date"))), "left")
    .join(df_kcc_events.alias("k"),
          (expr("upper(r.district) = upper(k.DistrictName)")) &
          (col("r.day") == col("k.Day")), "left")
    .fillna(0, subset=["price_score", "kcc_score"])
    .withColumn(
        "confidence_score",
        (col("dry_spell_score") * 0.5) + (col("price_score") * 0.25) + (col("kcc_score") * 0.25)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Filter & Save Confirmed Events

# COMMAND ----------

df_confirmed = (
    df_final
    .filter(col("confidence_score") >= CONFIDENCE_THRESHOLD)
    .select(
        col("r.state"), col("r.district"), col("r.month"), col("r.day"),
        col("rainfall_mm"), col("dry_spell_score"),
        col("price_score"), col("kcc_score"), col("confidence_score")
    )
)

(df_confirmed.write.format("delta").mode("overwrite").save(SILVER_PATH))

print(f"✅ Silver Layer Complete: {df_confirmed.count():,} trigger events confirmed.")
