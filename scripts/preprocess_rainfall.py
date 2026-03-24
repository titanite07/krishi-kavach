"""
preprocess_rainfall.py
───────────────────────
Converts the wide-format 'Indian Rainfall Dataset' into a long-format CSV.
Input: state, district, month, 1st, 2nd... 31st
Output: state, district, month, day, rainfall_mm

Usage:
    python scripts/preprocess_rainfall.py
"""

import pandas as pd
import pathlib

# ── Paths ─────────────────────────────────────────────────────────────────────
IN_FILE  = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\Rainfall\Indian Rainfall Dataset District-wise Daily Measurements.csv")
OUT_FILE = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\Rainfall\daily_rainfall_melted.csv")

# ── Load ──────────────────────────────────────────────────────────────────────
print(f"Loading: {IN_FILE}")
# Using semicolon delimiter based on analysis
df = pd.read_csv(IN_FILE, sep=';')

# ── Melt ──────────────────────────────────────────────────────────────────────
print("Restructuring data from wide to long format …")

# Columns that remain as categories
id_vars = ['state', 'district', 'month']

# Columns to be melted (1st to 31st)
day_cols = [col for col in df.columns if col.endswith('st') or col.endswith('nd') or col.endswith('rd') or col.endswith('th')]
# Filter out any non-day columns if they exist
day_cols = [col for col in day_cols if any(c.isdigit() for c in col)]

df_long = pd.melt(
    df,
    id_vars=id_vars,
    value_vars=day_cols,
    var_name='day_raw',
    value_name='rainfall_mm'
)

# ── Clean Day ──────────────────────────────────────────────────────────────────
# Extract numeric day: "1st" -> 1
df_long['day'] = df_long['day_raw'].str.extract('(\d+)').astype(int)

# Reorder columns
df_long = df_long[['state', 'district', 'month', 'day', 'rainfall_mm']]

# Remove invalid days (e.g., 31st for June or Feb 30th)
# We can do this simply by dropping 0.0 values or being strict about the calendar.
# For now, we keep all and let the Spark Silver layer handle date conversion.

# ── Save ──────────────────────────────────────────────────────────────────────
df_long.to_csv(OUT_FILE, index=False)
print(f"✅ Success! Melted data saved to:\n   {OUT_FILE}")
print(df_long.head(5))
