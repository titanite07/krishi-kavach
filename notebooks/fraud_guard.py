# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | Silver Layer · Fraud Guard & Integrity Check
# MAGIC **Purpose:** Validate confirmed triggers against pre-defined fraud rules to prevent social-media manipulation or system gaming.
| Factor | Rule | Logic |
|--------|------|-------|
| Bulk Activity | Rule 1 | > 5 inquiries from same device per district/day |
| Zero-Weather | Rule 2 | High social stress without supporting rainfall/price |
| High Confidence | Rule 3 | Conf > 0.7 without any weather signal support |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.functions import col, when, count, lit, concat_ws
from pyspark.sql.window import Window

# ── Paths ────────────────────────────────────────────────────────────────────
SILVER_PATH  = "dbfs:/FileStore/krishi_kavach/silver/confirmed_triggers"
FRAUD_PATH   = "dbfs:/FileStore/krishi_kavach/silver/fraud_flagged"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Confirmed Triggers

# COMMAND ----------

df_triggers = spark.read.format("delta").load(SILVER_PATH)

# MAGIC %md
# MAGIC ## 2. Implement Fraud Detection Logic

# COMMAND ----------

# ── Rule 1: Single-device Bulk Submission ───────────────────────────
# Note: Assuming device_id is available from the original IVR signal or KCC metadata
# If device_id is missing (demo), we skip or use a mock column.
if "device_id" not in df_triggers.columns:
    df_triggers = df_triggers.withColumn("device_id", lit("mock_device_001"))

window_device = Window.partitionBy("district", "date", "device_id")

df_fraud_base = (
    df_triggers
    .withColumn("device_query_count", count("*").over(window_device))
    # Combine Rules into a single flag logic
    .withColumn(
        "fraud_flag",
        when(col("device_query_count") > 5, True) # Rule 1
        .when((col("rain_score") == 0.0) & (col("kcc_stress_score") >= 0.6) & (col("price_score") == 0.0), True) # Rule 2
        .when((col("confidence_score") >= 0.7) & (col("rain_score") == 0.0), True) # Rule 3
        .otherwise(False)
    )
    .withColumn(
        "fraud_reason",
        when(col("device_query_count") > 5, "Rule1: Bulk Device Submission")
        .when((col("rain_score") == 0.0) & (col("kcc_stress_score") >= 0.6) & (col("price_score") == 0.0), "Rule2: Zero-rain high-KCC stress")
        .when((col("confidence_score") >= 0.7) & (col("rain_score") == 0.0), "Rule3: Impossible confidence without weather")
        .otherwise("Clean")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split & Persist

# COMMAND ----------

# Split datasets
df_clean   = df_fraud_base.filter(col("fraud_flag") == False).drop("fraud_reason", "device_query_count")
df_flagged = df_fraud_base.filter(col("fraud_flag") == True)

# Overwrite clean triggers (Integrity Filter)
(df_clean.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(SILVER_PATH))

# Save flagged records for manual audit
(df_flagged.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(FRAUD_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integrity Report

# COMMAND ----------

total_count = df_fraud_base.count()
clean_count = df_clean.count()
fraud_count = df_flagged.count()
fraud_rate  = (fraud_count / total_count * 100) if total_count > 0 else 0

print(f"✅ Clean triggers : {clean_count:,}")
print(f"🚨 Fraud-flagged  : {fraud_count:,}")
print(f"📊 Fraud rate     : {fraud_rate:.2f}%")

print("\n🔍 FLAGS FOR AUDIT:")
display(df_flagged.select("district", "date", "confidence_score", "fraud_reason"))
