# Krishi-Kavach Fault Analysis Report

After reviewing the code in the Bronze, Silver, and Gold layers of the Databricks pipeline, I have identified several **critical analytical faults** and vulnerabilities that completely invalidate the pipeline's logic in its current state. 

Here is a breakdown of the identified faults:

## 🚨 Critical Analytical Faults (Silver Layer)

### 1. Incomplete Date Joins (Cross-Joining Unrelated Months)
In [02_silver_layer.py](file:///c:/Users/Lochan%20Gowda/Data%20Bricks%20hackathon/notebooks/02_silver_layer.py), the final join between Rainfall (r), Prices (p), and KCC queries (k) attempts to join on the day of the month without accounting for the actual month or year.

```python
# FAULTY LOGIC
.join(df_price_events.alias("p"), 
      (expr("upper(r.district) = upper(p.District)")) & 
      (col("r.day") == dayofmonth(col("p.Arrival_Date"))), "left")
```
**Impact:** A rainfall event on **July 15** will mistakenly match with a price spike on **January 15**, **March 15**, etc. This corrupts the entire `confidence_score` calculation with randomized overlapping data.
**Fix required:** Construct a proper Date column in `df_rain` and `df_kcc` (incorporating year, month, and day) and join strictly on Date.

### 2. Broken Time-Series Window Function (Alphabetical Month Sorting)
In [02_silver_layer.py](file:///c:/Users/Lochan%20Gowda/Data%20Bricks%20hackathon/notebooks/02_silver_layer.py), the 7-day rolling window for finding dry spells orders rows by `month` and `day`. However, `month` is a `StringType` in the Bronze layer schema (e.g., "Jan", "Feb").

```python
# FAULTY LOGIC
Window.partitionBy("state", "district").orderBy("month", "day").rowsBetween(-7, 0)
```
**Impact:** Strings are ordered alphabetically. "Apr" comes before "Aug", which comes before "Dec", which comes before "Jan". The 7-day rolling window will compute across discontinuous, chronologically incorrect boundaries.
**Fix required:** Convert the `month` string to an integer representation or real Date before applying window functions.

---

## ⚠️ Medium/High Vulnerabilities

### 1. Schema Case-Sensitivity and Implicit Joins
- The Bronze schema definitions use exact casing (e.g. `DistrictName` for KCC, `district` for IMD). 
- In the Gold layer ([03_gold_payout_viz.py](file:///c:/Users/Lochan%20Gowda/Data%20Bricks%20hackathon/notebooks/03_gold_payout_viz.py)), the state join uses `col("t.state") == col("p.state_name")`. If capitalization or spelling differs slightly (e.g., "Madhya pradesh" vs "MADHYA PRADESH"), the join will silently drop triggers, directly impacting rural insurance payouts.

### 2. Missing Error Handling & Hardcoded Paths (Bronze Layer)
In [01_bronze_layer.py](file:///c:/Users/Lochan%20Gowda/Data%20Bricks%20hackathon/notebooks/01_bronze_layer.py), the `spark.read` commands assume every CSV exists perfectly at the specified paths. If a file is missing or corrupted, the notebook completely crashes instead of logging the error or skipping cleanly.

### 3. Static Configuration
Variables like `CONFIDENCE_THRESHOLD = 0.6` in Silver and the `0.1` payout multiplier in Gold are hardcoded directly into script logic rather than passed as dynamic widget inputs, limiting backtesting and parameter tuning capabilities.

---

## 💡 Recommendation
I highly recommend fixing the **Date join** and **Window ordering** bugs immediately. Would you like me to write the fixes for the [02_silver_layer.py](file:///c:/Users/Lochan%20Gowda/Data%20Bricks%20hackathon/notebooks/02_silver_layer.py) script?
