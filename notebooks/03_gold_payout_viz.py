# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 03 — Gold Layer · Payout Simulation [UC Version]
# MAGIC **Purpose:** Join confirmed triggers with PMFBY policies and simulate payouts using UC Managed Tables.
# MAGIC 
# MAGIC | Component | Logic | Target UC Table |
|-----------|-------|-----------------|
| Payout Logic | Sum_Insured * Payout_Rate * Damage_Index | `gold_payout_simulation` |

# COMMAND ----------

# ── UC & Parameter Widgets ──────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "krishi_kavach", "Schema Name")

dbutils.widgets.text("payout_multiplier", "0.1", "Payout Rate Multiplier (0.0–1.0)")
dbutils.widgets.text("target_crop", "Mustard", "Target Crop for Payout Simulation")
dbutils.widgets.text("area_ha", "1.0", "Area per Farmer Group (Hectares)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FULL_SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

PAYOUT_MULT = float(dbutils.widgets.get("payout_multiplier"))
TARGET_CROP = dbutils.widgets.get("target_crop")
AREA_HA     = float(dbutils.widgets.get("area_ha"))

# ── Imports ──────────────────────────────────────────────────────────────────
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col, lit

# ── UC Tables ────────────────────────────────────────────────────────────────
TABLE_SILVER = f"{FULL_SCHEMA_PATH}.silver_confirmed_triggers"
TABLE_POLICY = f"{FULL_SCHEMA_PATH}.bronze_pmfby_policy"
TABLE_GOLD   = f"{FULL_SCHEMA_PATH}.gold_payout_simulation"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data & Payout Simulation

# COMMAND ----------

df_triggers = spark.table(TABLE_SILVER)
df_policy   = spark.table(TABLE_POLICY)

df_payout_sim = (
    df_triggers
    .withColumn("crop", lit(TARGET_CROP)) 
    .join(df_policy, ["district", "crop"], "left")
    .withColumn("area_ha",      lit(AREA_HA)) 
    .withColumn("damage_index", col("confidence_score")) 
    .withColumn("payout_amount", 
        col("area_ha") * col("sum_insured") * col("payout_rate") * col("damage_index") * PAYOUT_MULT
    )
    .select(
        "district", "event_date", "crop", 
        "rain_score", "price_score", "kcc_stress_score", "confidence_score",
        "damage_index", "sum_insured", "payout_rate", "area_ha", "payout_amount"
    )
)

(df_payout_sim.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TABLE_GOLD))

print(f"✅ GOLD LAYER UC MIGRATION COMPLETE: {TABLE_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Risk Dashboard Visualization

# COMMAND ----------

pdf = spark.table(TABLE_GOLD).groupBy("district").agg(
    F.sum("payout_amount").alias("total_payout_INR"),
    F.avg("rain_score").alias("avg_rain_score"),
    F.avg("price_score").alias("avg_price_score"),
    F.avg("kcc_stress_score").alias("avg_kcc_score")
).orderBy(col("total_payout_INR").desc()).toPandas()

# Figure
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# Panel 1
ax1.barh(pdf["district"], pdf["total_payout_INR"], color="#2E8B57")
ax1.set_title("Total Simulated Payouts by District")
ax1.invert_yaxis()

# Panel 2
ax2.bar(pdf["district"], pdf["avg_rain_score"], label="Weather", color="#1E90FF")
ax2.bar(pdf["district"], pdf["avg_price_score"], label="Market", color="#FFA500", bottom=pdf["avg_rain_score"])
ax2.bar(pdf["district"], pdf["avg_kcc_score"], label="Social", color="#FF4500", bottom=pdf["avg_rain_score"] + pdf["avg_price_score"])
ax2.legend()
plt.xticks(rotation=45)

display(fig)
