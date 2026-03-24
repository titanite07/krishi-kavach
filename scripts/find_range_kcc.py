"""
find_max_year_kcc.py
──────────────────────
Quickly scans the beginning and end of the huge KCC file to find year range.
"""

import pandas as pd
import pathlib

IN_FILE = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\Kcc\kcc_dataset.csv")

def get_year(row_count=1000, from_end=False):
    try:
        if from_end:
            # Get last N bytes and read
            file_size = IN_FILE.stat().st_size
            with open(IN_FILE, 'rb') as f:
                f.seek(max(0, file_size - 1000000)) # Last 1MB
                last_lines = f.read().decode('utf-8', errors='ignore').splitlines()
                # Skip partial first line
                df = pd.DataFrame([l.split(',') for l in last_lines[1:] if ',' in l])
                # Year is column index 2 based on previous sample
                return df[2].unique()
        else:
            df = pd.read_csv(IN_FILE, nrows=row_count)
            return df['Year'].unique()
    except Exception as e:
        return [f"Error: {e}"]

print(f"Scanning {IN_FILE}...")
print(f"Start Years: {list(get_year(5000))}")
print(f"End Years: {list(get_year(from_end=True))}")
