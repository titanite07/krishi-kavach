# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 02 — Silver Layer Enrichment
# MAGIC **Purpose:** Enrich Bronze tables, apply a ±2-hour time-window join on district,
# MAGIC compute a Confidence Score, and persist confirmed trigger events.
# MAGIC
# MAGIC | Layer | Path |
# MAGIC |-------|------|
# MAGIC | Source (Delta) | `dbfs:/FileStore/krishi_kavach/bronze/` |
# MAGIC | Sink (Delta)   | `dbfs:/FileStore/krishi_kavach/silver/events_confirmed` |

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, abs as spark_abs,
    unix_timestamp, lit, round as spark_round
)
from pyspark.sql.types import TimestampType, DoubleType, BooleanType

# ── Constants ─────────────────────────────────────────────────────────────────
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"
SILVER_PATH  = "dbfs:/FileStore/krishi_kavach/silver/events_confirmed"

TWO_HOURS_SECONDS = 2 * 60 * 60     # 7200 seconds — time-window tolerance

# Confidence-score weights (must sum to 1.0)
WEIGHT_STRESS = 0.4
WEIGHT_RAIN   = 0.6
CONFIDENCE_THRESHOLD = 0.7           # events below this are discarded

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze Delta Tables

# COMMAND ----------

df_farmer_bronze = spark.read.format("delta").load(f"{BRONZE_BASE}/farmer_voice")
df_imd_bronze    = spark.read.format("delta").load(f"{BRONZE_BASE}/imd_grids")

print(f"📥  farmer_voice (bronze) : {df_farmer_bronze.count():,} rows")
print(f"📥  imd_grids    (bronze) : {df_imd_bronze.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Enrich Farmer Voice

# COMMAND ----------

df_farmer = (
    df_farmer_bronze
    # Ensure timestamp column is proper TimestampType (already cast in Bronze, re-cast for safety)
    .withColumn("timestamp_dt", col("timestamp").cast(TimestampType()))
    # Flag high-stress audio: 1 if stress score ≥ 0.7
    .withColumn(
        "audio_quality_flag",
        when(col("raw_stress_score") >= 0.7, 1).otherwise(0)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enrich IMD Grids

# COMMAND ----------

df_imd = (
    df_imd_bronze
    .withColumn("timestamp_dt", col("timestamp").cast(TimestampType()))
    # Rain anomaly: 1.0 if actual rainfall met or exceeded the 90th-percentile seasonal threshold
    .withColumn(
        "rain_anomaly_flag",
        when(
            col("actual_rain_mm") >= col("seasonal_rain_threshold_90"), 1.0
        ).otherwise(0.0)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time-Window Join (District + ±2 Hours)
# MAGIC
# MAGIC Strategy: cross-join on `district` first (reduces cartesian product), then filter
# MAGIC on the absolute difference of epoch timestamps ≤ 7 200 s.

# COMMAND ----------

# Alias DataFrames to avoid column-name ambiguity after join
df_f = df_farmer.alias("f")
df_i = df_imd.alias("i")

df_joined = (
    df_f.join(df_i, on="district", how="inner")          # joining on shared district key
    .filter(
        # Time-window tolerance: |farmer_ts - imd_ts| ≤ 2 hours
        spark_abs(
            unix_timestamp(col("f.timestamp_dt")) -
            unix_timestamp(col("i.timestamp_dt"))
        ) <= TWO_HOURS_SECONDS
    )
    # Disambiguate duplicate column names by selecting explicit aliases
    .select(
        col("district"),
        col("f.timestamp_dt").alias("farmer_timestamp_dt"),
        col("i.timestamp_dt").alias("imd_timestamp_dt"),
        col("f.mandi"),
        col("f.language"),
        col("f.device_id"),
        col("f.raw_stress_score"),
        col("f.audio_quality_flag"),
        col("i.grid_lat"),
        col("i.grid_lon"),
        col("i.actual_rain_mm"),
        col("i.seasonal_rain_mm"),
        col("i.seasonal_rain_threshold_90"),
        col("i.rain_anomaly_flag"),
    )
)

print(f"🔗  Joined rows (pre-confidence filter) : {df_joined.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compute Confidence Score

# COMMAND ----------

df_scored = (
    df_joined
    .withColumn(
        "confidence_score",
        spark_round(
            (col("raw_stress_score").cast(DoubleType()) * WEIGHT_STRESS) +
            (col("rain_anomaly_flag") * WEIGHT_RAIN),
            4           # round to 4 decimal places
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Flag Valid Events

# COMMAND ----------

df_flagged = df_scored.withColumn(
    "is_valid_event",
    when(col("confidence_score") >= CONFIDENCE_THRESHOLD, True).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filter Only Confirmed Events

# COMMAND ----------

df_confirmed = df_flagged.filter(col("is_valid_event") == True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Silver Delta Table

# COMMAND ----------

(
    df_confirmed
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Display Results

# COMMAND ----------

df_result = spark.read.format("delta").load(SILVER_PATH)
valid_event_count = df_result.count()

print("=" * 60)
print("  Krishi-Kavach · Silver Layer Enrichment Complete")
print("=" * 60)
print(f"  ✅ Valid trigger events confirmed : {valid_event_count:,}")
print(f"  🗂️  Saved to                      : {SILVER_PATH}")
print("=" * 60)

display(df_result)
