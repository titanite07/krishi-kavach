# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 01 — Bronze Layer Ingestion
# MAGIC **Purpose:** Read raw CSV files from DBFS, apply schema validation, and persist as Delta tables.
# MAGIC
# MAGIC | Layer | Path |
# MAGIC |-------|------|
# MAGIC | Source (CSV) | `dbfs:/FileStore/krishi_kavach/input/` |
# MAGIC | Sink (Delta) | `dbfs:/FileStore/krishi_kavach/bronze/` |

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# ── Paths ────────────────────────────────────────────────────────────────────
INPUT_BASE  = "dbfs:/FileStore/krishi_kavach/input"
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Farmer Voice — raw stress scores from IVR/voice calls

# COMMAND ----------

# ── Schema definition ─────────────────────────────────────────────────────────
farmer_voice_schema = StructType([
    StructField("timestamp",        StringType(),  True),   # will cast below
    StructField("mandi",            StringType(),  True),
    StructField("district",         StringType(),  True),
    StructField("language",         StringType(),  True),
    StructField("device_id",        StringType(),  True),
    StructField("raw_stress_score", DoubleType(),  True),
])

# ── Read CSV ──────────────────────────────────────────────────────────────────
df_farmer_raw = (
    spark.read
         .option("header", "true")
         .schema(farmer_voice_schema)
         .csv(f"{INPUT_BASE}/farmer_voice.csv")
)

# ── Cast timestamp & drop nulls ───────────────────────────────────────────────
df_farmer = (
    df_farmer_raw
    .withColumn("timestamp", to_timestamp(col("timestamp")))   # ISO-8601 or epoch string
    .withColumn("raw_stress_score", col("raw_stress_score").cast(DoubleType()))
    .dropna(subset=["timestamp", "district", "raw_stress_score"])  # mandatory fields
)

# ── Write as Delta ────────────────────────────────────────────────────────────
(
    df_farmer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_BASE}/farmer_voice")
)

count_farmer = spark.read.format("delta").load(f"{BRONZE_BASE}/farmer_voice").count()
print(f"✅  Bronze | farmer_voice  → {count_farmer:,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. IMD Grids — satellite-derived rainfall data

# COMMAND ----------

imd_grids_schema = StructType([
    StructField("timestamp",                  StringType(), True),
    StructField("district",                   StringType(), True),
    StructField("grid_lat",                   DoubleType(), True),
    StructField("grid_lon",                   DoubleType(), True),
    StructField("seasonal_rain_mm",           DoubleType(), True),
    StructField("seasonal_rain_threshold_90", DoubleType(), True),
    StructField("actual_rain_mm",             DoubleType(), True),
])

df_imd_raw = (
    spark.read
         .option("header", "true")
         .schema(imd_grids_schema)
         .csv(f"{INPUT_BASE}/imd_grids.csv")
)

# Cast + validate all numerical columns
df_imd = (
    df_imd_raw
    .withColumn("timestamp",                  to_timestamp(col("timestamp")))
    .withColumn("grid_lat",                   col("grid_lat").cast(DoubleType()))
    .withColumn("grid_lon",                   col("grid_lon").cast(DoubleType()))
    .withColumn("seasonal_rain_mm",           col("seasonal_rain_mm").cast(DoubleType()))
    .withColumn("seasonal_rain_threshold_90", col("seasonal_rain_threshold_90").cast(DoubleType()))
    .withColumn("actual_rain_mm",             col("actual_rain_mm").cast(DoubleType()))
    .dropna(subset=["timestamp", "district", "actual_rain_mm"])
)

(
    df_imd
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_BASE}/imd_grids")
)

count_imd = spark.read.format("delta").load(f"{BRONZE_BASE}/imd_grids").count()
print(f"✅  Bronze | imd_grids     → {count_imd:,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PMFBY Policy — government crop insurance master

# COMMAND ----------

pmfby_schema = StructType([
    StructField("district",     StringType(), True),
    StructField("crop",         StringType(), True),
    StructField("sum_insured",  DoubleType(), True),
    StructField("payout_rate",  DoubleType(), True),
    StructField("season",       StringType(), True),
])

df_pmfby_raw = (
    spark.read
         .option("header", "true")
         .schema(pmfby_schema)
         .csv(f"{INPUT_BASE}/pmfby_policy.csv")
)

df_pmfby = (
    df_pmfby_raw
    .withColumn("sum_insured", col("sum_insured").cast(DoubleType()))
    .withColumn("payout_rate", col("payout_rate").cast(DoubleType()))
    .dropna(subset=["district", "crop", "sum_insured", "payout_rate"])
)

(
    df_pmfby
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_BASE}/pmfby_policy")
)

count_pmfby = spark.read.format("delta").load(f"{BRONZE_BASE}/pmfby_policy").count()
print(f"✅  Bronze | pmfby_policy  → {count_pmfby:,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 55)
print("  Krishi-Kavach · Bronze Layer Ingestion Complete")
print("=" * 55)
print(f"  farmer_voice  : {count_farmer:>8,} rows")
print(f"  imd_grids     : {count_imd:>8,} rows")
print(f"  pmfby_policy  : {count_pmfby:>8,} rows")
print("=" * 55)
