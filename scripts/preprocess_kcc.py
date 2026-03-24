"""
preprocess_kcc_fast.py
───────────────────────
Seeks to the end of the 7.8GB KCC file to extract 2022-2024 records quickly.
"""

import pandas as pd
import pathlib
import io

# ── Paths ─────────────────────────────────────────────────────────────────────
IN_FILE  = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\Kcc\kcc_dataset.csv")
OUT_FILE = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\Kcc\kcc_2022_filtered.csv")

# ── Config ────────────────────────────────────────────────────────────────────
TARGET_YEARS = [2022]
FILE_SIZE = IN_FILE.stat().st_size
# Seek back from end (approx last 2GB should cover 2022-2025)
SEEK_POS  = max(0, FILE_SIZE - 2000000000) 

# ── Process ───────────────────────────────────────────────────────────────────
print(f"Seeking to {SEEK_POS / 1e9 :.2f} GB in {IN_FILE} …")

try:
    with open(IN_FILE, 'rb') as f:
        f.seek(SEEK_POS)
        # Skip the first partial line
        f.readline()
        
        # Read the rest in chunks
        # Read everything from here to the end. 2GB is manageable in memory if we filter immediately.
        # But let's use chunks to be safe.
        chunk_iter = pd.read_csv(f, names=['BlockName', 'Category', 'Year', 'Month', 'Day', 'Crop', 'DistrictName', 'QueryType', 'Season', 'Sector', 'StateName', 'QueryText', 'KccAns'], 
                                 chunksize=500000, low_memory=False)
        
        first_chunk = True
        total_rows_saved = 0
        
        for i, chunk in enumerate(chunk_iter):
            # Ensure Year is numeric
            chunk['Year'] = pd.to_numeric(chunk['Year'], errors='coerce')
            
            # Filter
            filtered = chunk[chunk['Year'].isin(TARGET_YEARS)]
            
            if not filtered.empty:
                cols_to_keep = ['Year', 'Month', 'Day', 'Crop', 'DistrictName', 'QueryType', 'Season', 'Sector', 'StateName']
                filtered = filtered[cols_to_keep]
                
                # Save
                filtered.to_csv(OUT_FILE, index=False, mode='a', header=first_chunk)
                first_chunk = False
                total_rows_saved += len(filtered)
            
            if (i + 1) % 10 == 0:
                print(f"   Processed { (i+1) * 500000 / 1e6 :.1f}M rows from seek point …")

    print(f"\n✅ Success! Saved {total_rows_saved} rows to:\n   {OUT_FILE}")

except Exception as e:
    print(f"\n❌ Error: {e}")
