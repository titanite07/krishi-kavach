# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 00 — District Cross-Reference Table (CRT)
# MAGIC **Purpose:** Standardize inconsistent district names across IMD, Mandi, PMFBY, and KCC datasets to prevent join failures.
# MAGIC 
# MAGIC # MAGIC | Feature | Logic |
# MAGIC |---------|-------|
# MAGIC | Canonical Mapping | Lookup table for historical & local variants |
# MAGIC | normalization UDF | Spark SQL function for on-the-fly cleaning |
# MAGIC | Delta Storage | Persistent reference table in DBFS |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Define Mappings ──────────────────────────────────────────────────────────
# Comprehensive map of historical, local, and bureaucratic variants to canonical names
DISTRICT_CRT = {
    # Requested & Historical
    "allahabad":        "Prayagraj",
    "prayagraj":        "Prayagraj",
    "bangalore":        "Bengaluru",
    "bangalore urban":  "Bengaluru",
    "bengaluru":        "Bengaluru",
    "bombay":           "Mumbai",
    "mumbai":           "Mumbai",
    "madras":           "Chennai",
    "chennai":          "Chennai",
    "calcutta":         "Kolkata",
    "kolkata":          "Kolkata",
    "orissa":           "Odisha",
    "uttaranchal":      "Uttarakhand",
    
    # Modern Renames (North)
    "gurgaon":          "Gurugram",
    "mewat":            "Nuh",
    "fyzabad":          "Ayodhya",
    "ayodhya":          "Ayodhya",
    "shimla":           "Shimla",
    "simla":            "Shimla",
    "benaras":          "Varanasi",
    "varanasi":         "Varanasi",
    "cawnpore":         "Kanpur",
    
    # Modern Renames (South)
    "tanjore":          "Thanjavur",
    "trichy":           "Tiruchirappalli",
    "tiruchirappalli":  "Tiruchirappalli",
    "trivandrum":       "Thiruvananthapuram",
    "cochin":           "Kochi",
    "mysore":           "Mysuru",
    "hubli":            "Hubballi",
    "mangalore":        "Mangaluru",
    "gulbarga":         "Kalaburagi",
    "belgaum":          "Belagavi",
    "bellary":          "Ballari",
    "pondy":            "Puducherry",
    "pondicherry":      "Puducherry",
    
    # Modern Renames (West/East)
    "baroda":           "Vadodara",
    "gauhati":          "Guwahati",
    "poona":            "Pune",
    "waltair":          "Visakhapatnam",
    "vizag":            "Visakhapatnam"
}

# ── Persist to Delta ─────────────────────────────────────────────────────────
crt_data = [(k, v) for k, v in DISTRICT_CRT.items()]
df_crt = spark.createDataFrame(crt_data, ["raw_name", "canonical_name"])

(df_crt.write
   .format("delta")
   .mode("overwrite")
   .save("dbfs:/FileStore/krishi_kavach/reference/district_crt"))

print(f"✅ CRT Table Persisted: {df_crt.count()} mappings saved.")

# ── Register Normalization UDF ──────────────────────────────────────────────
def normalize_district_logic(raw_district):
    if not raw_district: return None
    
    # 1. Clean & Lowercase
    clean = str(raw_district).strip().lower()
    
    # 2. Lookup in CRT
    if clean in DISTRICT_CRT:
        return DISTRICT_CRT[clean]
    
    # 3. Fallback to Title Case (e.g. "PUNE" -> "Pune")
    return clean.title()

# Register for use in Spark SQL and DataFrames
normalize_udf = F.udf(normalize_district_logic, StringType())
spark.udf.register("normalize_district", normalize_district_logic, StringType())

# COMMAND ----------

# ── Verification ─────────────────────────────────────────────────────────────
# Test cases
test_df = spark.createDataFrame([
    ("ALLAHABAD",), ("Bangalore Urban",), ("mumbai",), ("Unknown-District",)
], ["district"])

display(test_df.withColumn("standardized", normalize_udf(F.col("district"))))
