# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | Silver Layer · Fraud Guard [UC Version]
# MAGIC **Purpose:** Validate confirmed triggers against pre-defined fraud rules using UC Managed Tables.
# MAGIC 
# MAGIC | Factor | Rule | Logic |
|--------|------|-------|
| Bulk Activity | Rule 1 | > 5 inquiries from same device per district/day |
| Zero-Weather | Rule 2 | High social stress without supporting rainfall/price |
| High Confidence | Rule 3 | Conf > 0.7 without any weather signal support |

# COMMAND ----------

# ── UC Widgets ──────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "krishi_kavach", "Schema Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FULL_SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.window import Window

# ── Paths ────────────────────────────────────────────────────────────────────
TABLE_SILVER = f"{FULL_SCHEMA_PATH}.silver_confirmed_triggers"
TABLE_FRAUD  = f"{FULL_SCHEMA_PATH}.silver_fraud_flagged"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Confirmed Triggers

# COMMAND ----------

df_triggers = spark.table(TABLE_SILVER)

# MAGIC %md
# MAGIC ## 2. Implement Fraud Detection Logic

# COMMAND ----------

if "device_id" not in df_triggers.columns:
    df_triggers = df_triggers.withColumn("device_id", lit("mock_device_001"))

window_device = Window.partitionBy("district", "event_date", "device_id")

df_fraud_base = (
    df_triggers
    .withColumn("device_query_count", count("*").over(window_device))
    .withColumn(
        "fraud_flag",
        when(col("device_query_count") > 5, True) 
        .when((col("rain_score") == 0.0) & (col("kcc_stress_score") >= 0.6) & (col("price_score") == 0.0), True) 
        .when((col("confidence_score") >= 0.7) & (col("rain_score") == 0.0), True) 
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
# MAGIC ## 3. Split & Persist to UC Tables

# COMMAND ----------

df_clean   = df_fraud_base.filter(col("fraud_flag") == False).drop("fraud_reason", "device_query_count")
df_flagged = df_fraud_base.filter(col("fraud_flag") == True)

(df_clean.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TABLE_SILVER))

(df_flagged.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TABLE_FRAUD))

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

display(df_flagged.select("district", "event_date", "confidence_score", "fraud_reason"))
