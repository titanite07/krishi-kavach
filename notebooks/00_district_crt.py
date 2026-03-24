# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 00 — District Cross-Reference Table (CRT) [UC Version]
# MAGIC **Purpose:** Standardize inconsistent district names across UC-managed datasets.
# MAGIC 
# MAGIC | Feature | Logic |
|---------|-------|
| Canonical Mapping | Lookup table for historical & local variants |
| normalization UDF | Spark SQL function for on-the-fly cleaning |
| UC Storage | Managed table: `<catalog>.<schema>.district_crt` |

# COMMAND ----------

# ── UC Widgets ──────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "krishi_kavach", "Schema Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FULL_SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Define Mappings ──────────────────────────────────────────────────────────
DISTRICT_CRT = {
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
    "gurgaon":          "Gurugram",
    "mewat":            "Nuh",
    "fyzabad":          "Ayodhya",
    "ayodhya":          "Ayodhya",
    "shimla":           "Shimla",
    "simla":            "Shimla",
    "benaras":          "Varanasi",
    "varanasi":         "Varanasi",
    "cawnpore":         "Kanpur",
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
    "baroda":           "Vadodara",
    "gauhati":          "Guwahati",
    "poona":            "Pune",
    "waltair":          "Visakhapatnam",
    "vizag":            "Visakhapatnam"
}

# ── Persist to Unity Catalog ─────────────────────────────────────────────────
crt_data = [(k, v) for k, v in DISTRICT_CRT.items()]
df_crt = spark.createDataFrame(crt_data, ["raw_name", "canonical_name"])

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_PATH}")

table_name = f"{FULL_SCHEMA_PATH}.district_crt"
(df_crt.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(table_name))

print(f"✅ UC Table Persisted: {table_name}")

# ── Register Normalization UDF ──────────────────────────────────────────────
def normalize_district_logic(raw_district):
    if not raw_district: return None
    clean = str(raw_district).strip().lower()
    if clean in DISTRICT_CRT:
        return DISTRICT_CRT[clean]
    return clean.title()

normalize_udf = F.udf(normalize_district_logic, StringType())
spark.udf.register("normalize_district", normalize_district_logic, StringType())

# COMMAND ----------

# ── Verification ─────────────────────────────────────────────────────────────
display(spark.table(table_name).limit(10))
