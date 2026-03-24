"""
check_district_alignment.py
───────────────────────────
Checks for overlaps and mismatches in district names across preprocessed files.
"""

import pandas as pd
import pathlib

# ── Paths ─────────────────────────────────────────────────────────────────────
IMD_FILE      = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\IMD\imd_rainfall_2022.csv")
RAINFALL_FILE = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\Rainfall\daily_rainfall_melted.csv")
MANDI_FILE    = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\mandi datset\mandi_prices_cleaned.csv")

# ── Load ──────────────────────────────────────────────────────────────────────
df_imd      = pd.read_csv(IMD_FILE)
df_rainfall = pd.read_csv(RAINFALL_FILE)
df_mandi    = pd.read_csv(MANDI_FILE)

dist_imd      = set(df_imd['district'].unique())
dist_rainfall = set(df_rainfall['district'].unique())
dist_mandi    = set(df_mandi['District'].unique())

print(f"IMD Districts     : {len(dist_imd)}")
print(f"Rainfall Districts: {len(dist_rainfall)}")
print(f"Mandi Districts   : {len(dist_mandi)}")

overlap = dist_imd.intersection(dist_rainfall)
print(f"Overlap (IMD & Rainfall): {len(overlap)}")

# Sample mismatches
mismatches = list(dist_rainfall - dist_imd)[:10]
print(f"Sample Rainfall districts NOT in IMD: {mismatches}")

mismatches_2 = list(dist_imd - dist_rainfall)[:10]
print(f"Sample IMD districts NOT in Rainfall: {mismatches_2}")
