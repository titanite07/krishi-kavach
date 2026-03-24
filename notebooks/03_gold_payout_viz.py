# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 03 — Gold Layer · Payout Simulation & Reporting
# MAGIC **Purpose:** Join confirmed triggers with PMFBY insurance policies, simulate payouts, and render a high-fidelity risk dashboard.
# MAGIC 
# MAGIC | Component | Logic | Goal |
|-----------|-------|------|
| Payout Logic | Sum_Insured * Payout_Rate * Damage_Index | Financial quantification |
| Risk Report | Spark SQL Aggregation | District-level auditing |
| Dashboard | Dual-panel Matplotlib | Stakeholder Visualization |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from pyspark.sql.functions import col, lit, round as spark_round, avg, sum as spark_sum

# ── Paths ────────────────────────────────────────────────────────────────────
SILVER_PATH = "dbfs:/FileStore/krishi_kavach/silver/confirmed_triggers"
BRONZE_POLICY = "dbfs:/FileStore/krishi_kavach/bronze/pmfby_policy"
GOLD_SINK   = "dbfs:/FileStore/krishi_kavach/gold/payout_simulation"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data & Payout Simulation

# COMMAND ----------

# ── Load Confirmed Triggers (Silver) ──────────────────────────────────────────
df_triggers = spark.read.format("delta").load(SILVER_PATH)

# ── Load PMFBY Policy Master (Bronze) ─────────────────────────────────────────
df_policy = spark.read.format("delta").load(BRONZE_POLICY)

# ── Simulation Logic ──────────────────────────────────────────────────────────
df_payout_sim = (
    df_triggers
    .withColumn("crop", lit("Mustard")) # Hardcoded crop for demo simulation
    .join(df_policy, ["district", "crop"], "left")
    .withColumn("area_ha",      lit(1.0)) # 1.0 hectare demo assumption
    .withColumn("damage_index", col("confidence_score")) # Confidence as proxy for damage severity
    .withColumn("payout_amount", 
        col("area_ha") * col("sum_insured") * col("payout_rate") * col("damage_index")
    )
    # Refine selection for Gold layer
    .select(
        "district", "date", "crop", 
        "rain_score", "price_score", "kcc_stress_score", "confidence_score",
        "damage_index", "sum_insured", "payout_rate", "area_ha", "payout_amount"
    )
)

# ── Persist & Register SQL View ───────────────────────────────────────────────
(df_payout_sim.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(GOLD_SINK))

df_payout_sim.createOrReplaceTempView("gold_payout")

print(f"✅  GOLD LAYER: Saved {df_payout_sim.count():,} simulated payout records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Risk Report (Spark SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- District-level fiscal performance and signal attribution
# MAGIC SELECT 
# MAGIC   district,
# MAGIC   COUNT(*) AS trigger_events,
# MAGIC   ROUND(AVG(confidence_score), 3) AS avg_confidence,
# MAGIC   ROUND(AVG(rain_score), 3) AS avg_rain_score,
# MAGIC   ROUND(AVG(price_score), 3) AS avg_price_score,
# MAGIC   ROUND(AVG(kcc_stress_score), 3) AS avg_kcc_score,
# MAGIC   ROUND(SUM(payout_amount), 2) AS total_payout_INR
# MAGIC FROM gold_payout
# MAGIC GROUP BY district
# MAGIC ORDER BY total_payout_INR DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Risk Dashboard Visualization

# COMMAND ----------

# ── Extract Aggregated Data ───────────────────────────────────────────────────
# We use the same SQL logic to get a Pandas DF for Matplotlib
df_report = spark.sql("""
    SELECT 
      district,
      SUM(payout_amount) AS total_payout_INR,
      AVG(rain_score) AS avg_rain_score,
      AVG(price_score) AS avg_price_score,
      AVG(kcc_stress_score) AS avg_kcc_score
    FROM gold_payout
    GROUP BY district
    ORDER BY total_payout_INR DESC
""")

pdf = df_report.toPandas()

# ── Figure Layout ─────────────────────────────────────────────────────────────
plt.rcParams['font.family'] = 'sans-serif'
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# ── Panel 1: Total Payouts by District ─────────────────────────────────────────
bars = ax1.barh(pdf["district"], pdf["total_payout_INR"], color="#2E8B57", alpha=0.9)
ax1.set_title("Krishi-Kavach: Total Simulated Payouts by District", fontsize=14, fontweight="bold")
ax1.set_xlabel("Total Payout (INR)", fontsize=11)
ax1.invert_yaxis() # Highest payout at top

# Add ₹ value labels
for bar in bars:
    width = bar.get_width()
    ax1.text(width, bar.get_y() + bar.get_height()/2, f' ₹{width:,.0f}', 
             va='center', fontsize=10, fontweight='bold')

# ── Panel 2: Stacked Signal Attribution ───────────────────────────────────────
# We stack average scores to show which signal is driving triggers in each district
ax2.bar(pdf["district"], pdf["avg_rain_score"],   label="Weather Signal", color="#1E90FF") # DodgerBlue
ax2.bar(pdf["district"], pdf["avg_price_score"],  label="Market Signal",  color="#FFA500", bottom=pdf["avg_rain_score"]) # Orange
ax2.bar(pdf["district"], pdf["avg_kcc_score"],    label="Social Signal",  color="#FF4500", bottom=pdf["avg_rain_score"] + pdf["avg_price_score"]) # OrangeRed

ax2.set_title("Average Signal Scores by District", fontsize=14, fontweight="bold")
ax2.set_ylabel("Signal Strength (0 to 1)", fontsize=11)
ax2.legend(loc="upper right", fontsize=10)
plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")

# ── Final Rendering ───────────────────────────────────────────────────────────
plt.tight_layout(pad=3.0)
display(fig)
