# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 03 — Gold Layer · Payout Visualisation
# MAGIC **Purpose:** Load the Gold payout simulation table and render a bar chart
# MAGIC of total simulated insurance payouts per district.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# ── Load Gold Delta table ─────────────────────────────────────────────────────
GOLD_PATH = "dbfs:/FileStore/krishi_kavach/gold/payout_simulation"

df_gold = spark.read.format("delta").load(GOLD_PATH)

# ── Aggregate: total payout per district ─────────────────────────────────────
df_agg = (
    df_gold
    .groupBy("district")
    .sum("payout_amount")                          # sums the payout_amount column
    .withColumnRenamed("sum(payout_amount)", "total_payout")
    .orderBy("total_payout", ascending=False)      # largest payout first → natural reading order
)

# Convert to Pandas for matplotlib
pdf = df_agg.toPandas()

# ── Chart ─────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(max(10, len(pdf) * 1.1), 6))

bars = ax.bar(
    pdf["district"],
    pdf["total_payout"],
    color="#2E8B57",          # Sea-green — agricultural theme
    edgecolor="white",
    linewidth=0.6,
    width=0.6,
)

# Value labels on top of each bar (formatted as ₹ with commas)
for bar, value in zip(bars, pdf["total_payout"]):
    ax.text(
        bar.get_x() + bar.get_width() / 2.0,   # x-centre of bar
        bar.get_height() * 1.01,                # just above bar top
        f"₹{value:,.0f}",
        ha="center", va="bottom",
        fontsize=9, fontweight="bold", color="#1a1a1a"
    )

# Axis labels & title
ax.set_title(
    "Krishi-Kavach: Simulated Insurance Payouts by District",
    fontsize=14, fontweight="bold", pad=16
)
ax.set_xlabel("District", fontsize=11, labelpad=8)
ax.set_ylabel("Payout Amount (₹)", fontsize=11, labelpad=8)

# Format y-axis ticks with ₹ and comma separators
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}")
)

# Rotate x-axis labels if there are many districts
plt.xticks(rotation=30 if len(pdf) > 6 else 0, ha="right", fontsize=10)
plt.yticks(fontsize=10)

# Light grid for readability
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)

plt.tight_layout()

# Render inline in Databricks
display(fig)
