"""
convert_nc_to_csv.py
────────────────────
Converts the IMD RF25 NetCDF rainfall grid file into a flat CSV that
matches the Krishi-Kavach imd_grids schema:

    timestamp, district, grid_lat, grid_lon,
    seasonal_rain_mm, seasonal_rain_threshold_90, actual_rain_mm

Usage:
    python scripts/convert_nc_to_csv.py
"""

import pathlib
import numpy as np
import pandas as pd
import xarray as xr

# ── Paths ─────────────────────────────────────────────────────────────────────
NC_FILE   = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\IMD\RF25_ind2022_rfp25.nc")
OUT_DIR   = pathlib.Path(r"D:\Projects\Krishi-Kavach\data\IMD")
OUT_CSV   = OUT_DIR / "imd_rainfall_2022.csv"

# ── Load NetCDF ───────────────────────────────────────────────────────────────
print(f"Loading: {NC_FILE}")
# Since the file is already in NetCDF format (.nc), we use xarray directly.
# imdlib is primarily for IMD binary (.grd) files.
ds = xr.open_dataset(str(NC_FILE))
print("Variables  :", list(ds.data_vars))
print("Dimensions :", dict(ds.dims))
print("Coords     :", list(ds.coords))

# ── Identify rainfall variable (IMD uses 'rf' or 'RAIN' or first var) ─────────
rain_var = list(ds.data_vars)[0]
print(f"Using variable: {rain_var}")

da = ds[rain_var]   # DataArray: (time, lat, lon) or (lat, lon)

# ── Flatten to a DataFrame ────────────────────────────────────────────────────
print("Flattening to DataFrame …")
df_raw = (
    da
    .to_dataframe()
    .reset_index()
    .dropna(subset=[rain_var])
)

# Rename to something readable
rename_map = {}
for col in df_raw.columns:
    lc = col.lower()
    if lc in ("lat", "latitude"):
        rename_map[col] = "grid_lat"
    elif lc in ("lon", "longitude", "long"):
        rename_map[col] = "grid_lon"
    elif lc in ("time", "date"):
        rename_map[col] = "timestamp"
df_raw = df_raw.rename(columns=rename_map)
df_raw = df_raw.rename(columns={rain_var: "actual_rain_mm"})

print(f"Raw shape  : {df_raw.shape}")
print(df_raw.head(3))

# ── Aggregate: compute seasonal totals and 90th-percentile threshold ───────────
# seasonal_rain_mm  = sum over all dates per grid cell
# actual_rain_mm    = mean daily rain per grid cell across the year
# seasonal_rain_threshold_90 = 90th percentile of seasonal totals across all cells

if "timestamp" in df_raw.columns:
    # Summarise per grid cell
    agg = (
        df_raw
        .groupby(["grid_lat", "grid_lon"])["actual_rain_mm"]
        .agg(
            seasonal_rain_mm="sum",
            actual_rain_mm="mean",
        )
        .reset_index()
    )
    threshold_90 = float(np.percentile(agg["seasonal_rain_mm"].values, 90))
    agg["seasonal_rain_threshold_90"] = round(threshold_90, 2)
    agg["timestamp"] = "2022-06-01 00:00:00"   # representative Kharif start
else:
    # Already one row per grid cell
    agg = df_raw.copy()
    agg["seasonal_rain_mm"] = agg["actual_rain_mm"]
    threshold_90 = float(np.percentile(agg["seasonal_rain_mm"].values, 90))
    agg["seasonal_rain_threshold_90"] = round(threshold_90, 2)
    agg["timestamp"] = "2022-06-01 00:00:00"

# Add a placeholder district column (spatial join could be done separately)
agg["district"] = (
    "LAT" + agg["grid_lat"].round(2).astype(str)
    + "_LON" + agg["grid_lon"].round(2).astype(str)
)

# Round numerical columns
for col in ["grid_lat", "grid_lon", "seasonal_rain_mm", "actual_rain_mm"]:
    agg[col] = agg[col].round(4)

# Reorder to match imd_grids schema
out_cols = [
    "timestamp", "district",
    "grid_lat", "grid_lon",
    "seasonal_rain_mm", "seasonal_rain_threshold_90", "actual_rain_mm",
]
agg = agg[out_cols]

# ── Write CSV ─────────────────────────────────────────────────────────────────
OUT_DIR.mkdir(parents=True, exist_ok=True)
agg.to_csv(OUT_CSV, index=False)

print(f"\n✅ Done! {len(agg):,} grid cells written to:\n   {OUT_CSV}")
print(f"   90th-percentile seasonal threshold : {threshold_90:,.2f} mm")
print(agg.head(5).to_string(index=False))
