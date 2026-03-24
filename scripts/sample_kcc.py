"""
sample_kcc.py
─────────────
Samples the huge KCC dataset to understand categorical values for filtering.
"""

import pandas as pd
import pathlib

IN_FILE = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\Kcc\kcc_dataset.csv")

# Use a small chunk to check unique values
print(f"Sampling {IN_FILE} (first 100,000 rows)...")

try:
    # We use engine='python' or low_memory=False for large files
    chunk = pd.read_csv(IN_FILE, nrows=100000)
    
    print("\nColumns and Dtypes:")
    print(chunk.dtypes)
    
    print("\nTop Query Types:")
    print(chunk['QueryType'].value_counts().head(10))
    
    print("\nTop Sectors:")
    print(chunk['Sector'].value_counts().head(10) if 'Sector' in chunk.columns else "Sector not found")
    
    print("\nYears available in this sample:")
    print(chunk['Year'].unique())

except Exception as e:
    print(f"Error sampling: {e}")
