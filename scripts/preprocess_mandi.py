"""
preprocess_mandi.py
────────────────────
Cleans the Mandi price dataset headers and normalizes formats.
Input: Min_x0020_Price, etc.
Output: State, District, Market, Commodity, Variety, Grade, Arrival_Date, Min_Price, Max_Price, Modal_Price

Usage:
    python scripts/preprocess_mandi.py
"""

import pandas as pd
import pathlib

# ── Paths ─────────────────────────────────────────────────────────────────────
IN_FILE  = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\mandi datset\mandi dataset.csv")
OUT_FILE = pathlib.Path(r"d:\Projects\Krishi-Kavach\data\mandi datset\mandi_prices_cleaned.csv")

# ── Load ──────────────────────────────────────────────────────────────────────
print(f"Loading: {IN_FILE}")
df = pd.read_csv(IN_FILE)

# ── Rename Headers ────────────────────────────────────────────────────────────
# Mapping x0020 to space or underscores
df.columns = [c.replace('_x0020_', '_') for c in df.columns]

print(f"Cleaned Columns: {list(df.columns)}")

# ── Normalize Dates ───────────────────────────────────────────────────────────
# Ensure Arrival_Date is datetime then string ISO
df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], dayfirst=True)

# ── Save ──────────────────────────────────────────────────────────────────────
df.to_csv(OUT_FILE, index=False)
print(f"✅ Success! Cleaned Mandi data saved to:\n   {OUT_FILE}")
print(df.head(3))
