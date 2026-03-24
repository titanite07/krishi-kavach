# 🌾 Krishi-Kavach
> **Parametric Crop Insurance Triangulation System**  
> Powered by Databricks · Unity Catalog · PySpark · Z-Score Anomaly Detection

[![Platform: Databricks](https://img.shields.io/badge/Platform-Databricks-red.svg)](https://databricks.com)
[![Storage: Unity Catalog](https://img.shields.io/badge/Storage-Unity%20Catalog-orange.svg)](https://www.databricks.com/product/unity-catalog)

---

## 📌 Overview
**Krishi-Kavach** ("Crop Shield") is a production-grade parametric insurance engine that cross-validates climate distress by triangulating 3 independent signals: **Weather Anomalies**, **Market Price Volatility**, and **Social Distress Queries** (Kisan Call Center).

By using **Z-Score based anomaly detection**, it identifies extreme climatic events with statistical precision, reducing insurance 'basis risk' for millions of Indian farmers.

---

## 🏗️ Architecture (Unity Catalog)
The pipeline is managed via **Unity Catalog (UC)** with a multi-layered governed approach:

1.  **Ingestion Layer**: Raw CSVs uploaded to **UC Volumes** (`/Volumes/main/krishi_kavach/input/`).
2.  **Bronze Layer**: Managed Tables for all 6 signals with autonomous **District Normalization** (CRT).
3.  **Silver Layer**: **Z-Score Anomaly Detection** and 3-Signal Triangulation.
4.  **Gold Layer**: Payout Simulation & Stakeholder Risk Dashboard.

---

## 🚀 Getting Started

### 1. Data Setup
Upload the 6 production CSVs from `data/production_input/` to your Databricks **Unity Catalog Volume**:
`/Volumes/<catalog>/<schema>/input/`

### 2. Execution Order
Run the notebooks in the following order:
1.  `00_district_crt`: Standardizes district name variants.
2.  `01_bronze_layer`: Ingests volumes into managed tables.
3.  `02_silver_layer`: Computes statistical triggers (Z-Score model).
4.  `fraud_guard`: Identifies social manipulation & Sybil attacks.
5.  `03_gold_payout_viz`: Generates the fiscal risk dashboard.

---

## 📊 The Triangulation Model

An event is confirmed if the **Weighted Confidence Score >= 0.60**:

| Signal | Source | Logic | Weight |
|--------|--------|-------|--------|
| **Weather** | IMD Rainfall | Rainfall Z-Score > 1.5$\sigma$ | **50%** |
| **Market** | Mandi Prices | Price Z-Score > 1.5$\sigma$ | **25%** |
| **Social** | KCC Queries | Query Volume Spikes | **25%** |

---

## 🛠️ Key Features
- **Dynamic Backtesting**: Use Databricks Widgets to adjust Z-score thresholds and payout rates live.
- **Fraud Guard**: Integrity layer to flag zero-weather high-confidence anomalies.
- **District CRT**: Autonomous resolution of historical/local district name variants.

---

## 📄 License
MIT © 2026 · Krishi-Kavach Team
