# Krishi-Kavach | Final Project Status Report

**Krishi-Kavach** is a high-fidelity parametric crop insurance trigger system designed to automate and validate agricultural distress claims across India using multi-signal geographic and market data.

---

## 🛠️ Implementation Overview
The project has successfully transitioned from synthetic samples to a **Production-Ready (v2.0)** pipeline. 

### 1. Data Processing & Integration
We have successfully integrated **6 independent real-world datasets**:
- **Weather**: IMD NetCDF (Gridded) + District-wise Daily Measurements (2022).
- **Market**: Mandi Market Prices (Cleaned) + AGMARKNET Reference Data.
- **Social**: 7.8GB Kisan Call Center (KCC) Query Logs (Filtered 2022 Subset).
- **Insurance**: PMFBY State-Level Policy Master.

### 2. Multi-Signal Triangulation Engine (Silver Layer)
Triggers are no longer based on single-point rainfall data. The system now cross-validates:
- **Signal A (50%)**: Dry spells and rainfall spikes using a 7-day rolling window.
- **Signal B (25%)** Market price spikes and arrival volume dips.
- **Signal C (25%)**: Farmer inquiry density (Social Stress) for weather/pest sub-categories.

### 3. Fraud and Integrity Layer
A **Fraud Guard** sweep identifies:
- **Bulk Submissions**: Many inquiries from a single device ID.
- **Signal Anomalies**: High social stress without supporting weather/price data.

### 4. Risk Reporting (Gold Layer)
- Automated payout simulation based on confidence scores and PMFBY policy values.
- **Dual-Panel Dashboard**: Visualizes fiscal exposure by district and attributes signals.

---

## 🚀 Deployment: Importing into Databricks

**The project is ready for immediate import!** Follow these steps:

### 1. Link Repository
In your Databricks Workspace:
1. Go to **Workspace** -> **Repos**.
2. Click **Add Repo**.
3. Repository URL: `https://github.com/titanite07/krishi-kavach.git`
4. Databricks will automatically pull the updated [.py](file:///d:/Projects/Krishi-Kavach/01_bronze_layer.py) notebooks.

### 2. Prepare Data (Crucial Step)
Before running, you must upload the preprocessed files from your local `data/` folder to DBFS:
- **Source**: `D:\Projects\Krishi-Kavach\data\`
- **Destination**: `dbfs:/FileStore/krishi_kavach/input/`
- *Note: Ensure the local filenames match those expected in [01_bronze_layer.py](file:///d:/Projects/Krishi-Kavach/01_bronze_layer.py).*

### 3. Execution Sequence
Run the notebooks in this order:
1. [01_bronze_layer.py](file:///d:/Projects/Krishi-Kavach/01_bronze_layer.py)
2. [02_silver_layer.py](file:///d:/Projects/Krishi-Kavach/02_silver_layer.py)
3. [fraud_guard.py](file:///d:/Projects/Krishi-Kavach/notebooks/fraud_guard.py)
4. [03_gold_payout_viz.py](file:///d:/Projects/Krishi-Kavach/03_gold_payout_viz.py)
