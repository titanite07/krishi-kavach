# Krishi-Kavach | Real-World Dataset Integration Walkthrough

We have successfully transitioned the **Krishi-Kavach** pipeline from synthetic samples to a high-fidelity, multi-signal parametric insurance system using over 8GB of real-world Indian agricultural data.

## 1. Data Processing Pipeline
Processed 6 major datasets to ensure compatibility with Databricks PySpark:

- **IMD NetCDF (2022)**: Converted gridded rainfall data into a flat CSV ([imd_rainfall_2022.csv](file:///d:/Projects/Krishi-Kavach/data/IMD/imd_rainfall_2022.csv)).
- **Mandi Market Prices**: Cleaned 418 market datasets, removing XML artifacts and normalizing dates.
- **District-wise Daily Rainfall**: Melted wide-format measurements (D1-D31) into a long-format series for time-series analysis.
- **Kisan Call Center (KCC)**: Extracted a 213MB 2022-subset from a massive **7.8GB** query log using a fast-seek algorithm.

## 2. Updated Architecture
The pipeline now uses a **Triangulation Logic** to confirm crop distress:

- **Signal A (Weather)**: Daily rainfall anomalies and dry-spell detection (Window-based).
- **Signal B (Market)**: Price spikes and arrival volume dips in local Mandis.
- **Signal C (Social)**: High volumes of 'Weather' and 'Pest' queries in the KCC logs.

## 3. Layer Implementation

### [Bronze Layer](file:///d:/Projects/Krishi-Kavach/notebooks/01_bronze_layer.py)
Ingests all 6 sources into Delta format with strict schema validation.

### [Silver Layer](file:///d:/Projects/Krishi-Kavach/notebooks/02_silver_layer.py)
Computes the **Trigger Confidence Score**. An event is only confirmed if the weighted average of Rainfall (50%), Price (25%), and KCC Stress (25%) exceeds the `0.6` threshold.

### [Gold Layer](file:///d:/Projects/Krishi-Kavach/notebooks/03_gold_payout_viz.py)
Maps confirmed triggers to PMFBY State policies to simulate fiscal exposure and generate risk reports.

---
> [!IMPORTANT]
> To run the pipeline, upload the preprocessed files in `data/` to your DBFS `FileStore/krishi_kavach/input/` directory and execute the notebooks in order: 01 → 02 → 03.
