# 🌾 Krishi-Kavach

> **Parametric Crop Insurance Trigger System for India**  
> Powered by Databricks · PySpark · Delta Lake

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform: Databricks](https://img.shields.io/badge/Platform-Databricks-red.svg)](https://databricks.com)
[![Language: PySpark](https://img.shields.io/badge/Language-PySpark-blue.svg)](https://spark.apache.org/docs/latest/api/python/)

---

## 📌 Overview

**Krishi-Kavach** ("Crop Shield" in Hindi) is a hackathon-grade end-to-end parametric insurance trigger pipeline built on **Databricks + Delta Lake**.  
It ingests multi-source data (farmer voice stress scores, IMD satellite rainfall, PMFBY government policy), detects valid trigger events using a confidence-score model, and simulates insurance payouts — all visualised in a single chart.

---

## 🏗️ Architecture

```
[Raw CSVs on DBFS]
      │
      ▼
┌──────────────────────┐
│  01 · Bronze Layer   │  Schema validation, type casting, Delta write
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  02 · Silver Layer   │  Enrichment, ±2h time-window join, confidence scoring
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  03 · Gold Layer Viz │  Payout simulation bar chart (matplotlib + display())
└──────────────────────┘
```

---

## 📂 Project Structure

```
krishi-kavach/
├── notebooks/
│   ├── 01_bronze_layer.py      # CSV ingestion → Delta (farmer_voice, imd_grids, pmfby_policy)
│   ├── 02_silver_layer.py      # Enrichment + ±2 h join + confidence scoring → events_confirmed
│   └── 03_gold_payout_viz.py   # Payout simulation visualisation (matplotlib bar chart)
├── data/
│   └── sample/
│       ├── farmer_voice.csv    # Sample IVR stress-score data
│       ├── imd_grids.csv       # Sample IMD satellite rainfall data
│       └── pmfby_policy.csv    # Sample PMFBY crop insurance policy master
├── requirements.txt
├── LICENSE
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Databricks workspace (Runtime ≥ 12.x with Delta Lake)
- DBFS write access to `dbfs:/FileStore/krishi_kavach/`
- `gh` CLI (optional, for local development)

### 1 — Upload Sample Data

```bash
# Using Databricks CLI
databricks fs cp data/sample/farmer_voice.csv  dbfs:/FileStore/krishi_kavach/input/farmer_voice.csv
databricks fs cp data/sample/imd_grids.csv     dbfs:/FileStore/krishi_kavach/input/imd_grids.csv
databricks fs cp data/sample/pmfby_policy.csv  dbfs:/FileStore/krishi_kavach/input/pmfby_policy.csv
```

### 2 — Import Notebooks into Databricks

In your Databricks workspace:  
**Workspace → Import → Browse** → select each `.py` file from the `notebooks/` folder.

Databricks auto-detects `# COMMAND ----------` cell markers.

### 3 — Run in Order

| # | Notebook | Output Delta Path |
|---|----------|-------------------|
| 1 | `01_bronze_layer.py` | `bronze/farmer_voice`, `bronze/imd_grids`, `bronze/pmfby_policy` |
| 2 | `02_silver_layer.py` | `silver/events_confirmed` |
| 3 | `03_gold_payout_viz.py` | *(reads* `gold/payout_simulation` *— generate this from Silver)* |

---

## 📊 Confidence Score Model

| Signal | Column | Weight |
|--------|--------|--------|
| Farmer voice stress | `raw_stress_score` | **0.4** |
| Rainfall anomaly | `rain_anomaly_flag` | **0.6** |

```
confidence_score = (raw_stress_score × 0.4) + (rain_anomaly_flag × 0.6)
is_valid_event   = confidence_score ≥ 0.70
```

---

## 🧪 Sample Data Schema

### `farmer_voice.csv`
| Column | Type | Description |
|--------|------|-------------|
| timestamp | Timestamp | IVR call timestamp |
| mandi | String | Nearest market (mandi) name |
| district | String | Administrative district |
| language | String | Call language (Hindi / Marathi …) |
| device_id | String | Farmer handset ID |
| raw_stress_score | Double | ML-derived stress score [0–1] |

### `imd_grids.csv`
| Column | Type | Description |
|--------|------|-------------|
| timestamp | Timestamp | Satellite observation time |
| district | String | Administrative district |
| grid_lat / grid_lon | Double | Grid centroid coordinates |
| seasonal_rain_mm | Double | Cumulative seasonal rainfall |
| seasonal_rain_threshold_90 | Double | 90th-percentile seasonal threshold |
| actual_rain_mm | Double | Measured rainfall for the period |

### `pmfby_policy.csv`
| Column | Type | Description |
|--------|------|-------------|
| district | String | Administrative district |
| crop | String | Crop type |
| sum_insured | Double | Insurance face value (₹) |
| payout_rate | Double | Fraction of sum insured paid out |
| season | String | Kharif / Rabi |

---

## 📄 License

MIT © 2026 · Krishi-Kavach Team
