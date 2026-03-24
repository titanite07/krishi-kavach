# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 03 — Gold Layer · Payout Visualisation (Production)
# MAGIC **Purpose:** Connect confirmed triggers to PMFBY insurance policy data, calculate final simulated payouts, and render visualizations.
# MAGIC 
# MAGIC | Process | Source | Logic |
# MAGIC |---------|--------|-------|
# MAGIC | Triggers | `silver/events_confirmed` | CONFIDENCE_SCORE >= 0.6 |
# MAGIC | Policies | `bronze/pmfby_policy` | SUM_INSURED per State/Season |
# MAGIC | Payout | Calculation | SUM_INSURED * CONFIDENCE_SCORE * DISRUPTION_FACTOR |

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, count, avg, lit
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
SILVER_PATH = "dbfs:/FileStore/krishi_kavach/silver/events_confirmed"
POLICY_PATH = "dbfs:/FileStore/krishi_kavach/bronze/pmfby_policy"
GOLD_PATH   = "dbfs:/FileStore/krishi_kavach/gold/payout_simulation"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Confirmed Events & Policies

# COMMAND ----------

df_triggers = spark.read.format("delta").load(SILVER_PATH)
df_policy   = spark.read.format("delta").load(POLICY_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Map District-Level Triggers to State-Level Policies
# MAGIC *Note: The real PMFBY data we have is at the State level. Let's aggregate triggers per state to match.*

# COMMAND ----------

df_state_triggers = (
    df_triggers
    .groupBy("state")
    .agg(
        avg("confidence_score").alias("avg_confidence"),
        count("*").alias("trigger_count"),
        avg("rainfall_mm").alias("avg_rainfall")
    )
)

# Join with Policy
df_payout = (
    df_state_triggers.alias("t")
    .join(df_policy.alias("p"), col("t.state") == col("p.state_name"), "inner")
    .withColumn("payout_amount", col("p.sum_insured") * col("t.avg_confidence") * 0.1) # 10% base payout factor for trigger
    .select(
        col("p.state_name"), 
        col("t.trigger_count"), 
        col("p.sum_insured"), 
        col("payout_amount"),
        col("t.avg_confidence")
    )
)

(df_payout.write.format("delta").mode("overwrite").save(GOLD_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Render Insurance Risk Visualization

# COMMAND ----------

pdf = df_payout.toPandas()

fig, ax1 = plt.subplots(figsize=(12, 6))

# Primary Bar Chart: Payout Amount
color_payout = "#2E8B57"
ax1.set_xlabel("State", fontsize=12)
ax1.set_ylabel("Simulated Payout (₹)", fontsize=12, color=color_payout)
bars = ax1.bar(pdf["state_name"], pdf["payout_amount"], color=color_payout, alpha=0.8, label="Simulated Payout")
ax1.tick_params(axis='y', labelcolor=color_payout)

# Secondary Chart: Confidence
ax2 = ax1.twinx()
ax2.set_ylabel("Avg Confidence", fontsize=12, color="#B22222")
ax2.plot(pdf["state_name"], pdf["avg_confidence"], color="#B22222", marker='o', linewidth=2, label="Signal Strength")
ax2.tick_params(axis='y', labelcolor="#B22222")
ax2.set_ylim(0, 1.0)

# Format Currency
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x/1e7:,.1f} Cr"))

plt.title("Krishi-Kavach: Simulated PMFBY Payouts by State (2022 Data)", fontsize=14, fontweight="bold", pad=20)
fig.tight_layout()
display(fig)
