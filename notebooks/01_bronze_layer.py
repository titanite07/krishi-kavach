# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 01 — Bronze Layer · Data Ingestion [UC Version]
# MAGIC **Purpose:** Ingest raw CSV datasets from UC Volumes into managed Delta tables with district normalization.
# MAGIC 
# MAGIC # MAGIC | Dataset | UC Volume Path | Target UC Table |
# MAGIC # MAGIC |---------|----------------|-----------------|
# MAGIC # MAGIC | IMD Rainfall | `/Volumes/<c>/<s>/input/imd_rainfall_2022.csv` | `bronze_imd_rainfall` |
# MAGIC # MAGIC | Mandi Prices | `/Volumes/<c>/<s>/input/mandi_prices.csv` | `bronze_mandi_prices` |
# MAGIC # MAGIC | Daily Rain | `/Volumes/<c>/<s>/input/district_daily_rainfall.csv` | `bronze_district_daily_rainfall` |
# MAGIC # MAGIC | KCC Queries | `/Volumes/<c>/<s>/input/kcc_2022.csv` | `bronze_kcc_2022` |
# MAGIC # MAGIC | PMFBY | `/Volumes/<c>/<s>/input/pmfby_policy.csv` | `bronze_pmfby_policy` |
# MAGIC # MAGIC | AGMARKNET | `/Volumes/<c>/<s>/input/agmarknet_reference.csv` | `bronze_agmarknet_reference` |

# COMMAND ----------

# ── UC Widgets ──────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "krishi_kavach", "Schema Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FULL_SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, when, to_date, lit
from pyspark.sql.types import StringType, DoubleType, DateType

# ── Paths & Table Roots ─────────────────────────────────────────────────────
INPUT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/input"

# ── District Normalization Utility ──────────────────────────────────────────
DISTRICT_CRT = { 
    "allahabad": "Prayagraj", "prayagraj": "Prayagraj", "bangalore": "Bengaluru", 
    "bangalore urban": "Bengaluru", "bengaluru": "Bengaluru", "bombay": "Mumbai", 
    "mumbai": "Mumbai", "madras": "Chennai", "calcutta": "Kolkata", "kolkata": "Kolkata", 
    "orissa": "Odisha", "uttaranchal": "Uttarakhand", "gurgaon": "Gurugram", 
    "mewat": "Nuh", "tanjore": "Thanjavur", "trichy": "Tiruchirappalli", "vizag": "Visakhapatnam"
}

def normalize_district_logic(raw):
    if not raw: return None
    clean = str(raw).strip().lower()
    return DISTRICT_CRT.get(clean, clean.title())

normalize_udf = F.udf(normalize_district_logic, StringType())

# ── Utility: Delta Persistence ───────────────────────────────────────────────
def save_to_bronze_uc(df, table_base_name):
    # Apply normalization to 'district' column right before saving
    if "district" in df.columns:
        df = df.withColumn("district", normalize_udf(col("district")))
    
    full_table_name = f"{FULL_SCHEMA_PATH}.bronze_{table_base_name}"
    
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table_name))
    
    # Post-normalization check
    print(f"\n✅ UC TABLE: {full_table_name}")
    df_delta = spark.table(full_table_name)
    
    if "district" in df_delta.columns:
        df_unknown = df_delta.filter(~lower(col("district")).isin(list(DISTRICT_CRT.keys()))).select("district").distinct()
        if df_unknown.count() > 0:
            print(f"⚠️  UNMAPPED DISTRICTS:")
            display(df_unknown.limit(5))
    
    print(f"📊 ROW COUNT: {df_delta.count():,}")
    display(df_delta.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest IMD Rainfall (NetCDF-derived CSV)

# COMMAND ----------

df_imd = (spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/imd_rainfall_2022.csv")
          .withColumn("date", to_date(col("date")))
          .withColumn("rainfall_mm", col("rainfall_mm").cast(DoubleType()))
          .dropna(subset=["district", "rainfall_mm"]))

save_to_bronze_uc(df_imd, "imd_rainfall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Mandi Market Prices

# COMMAND ----------

df_mandi = (spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/mandi_prices.csv")
            .filter(col("modal_price").cast("double").isNotNull())
            .withColumn("date", to_date(col("date")))
            .withColumn("modal_price", col("modal_price").cast(DoubleType()))
            .withColumn("arrivals_tonnes", col("arrivals_tonnes").cast(DoubleType())))

save_to_bronze_uc(df_mandi, "mandi_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest District-wise Daily Rainfall (Melted)

# COMMAND ----------

df_daily_rain = (spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/district_daily_rainfall.csv")
                .withColumn("rainfall_mm", col("rainfall_mm").cast(DoubleType()))
                .withColumn("day", col("day").cast("int")))

save_to_bronze_uc(df_daily_rain, "district_daily_rainfall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Kisan Call Center (KCC) 2022 subset

# COMMAND ----------

df_kcc = (spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/kcc_2022.csv")
          .withColumn("date", to_date(col("date"))))

save_to_bronze_uc(df_kcc, "kcc_2022")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest PMFBY Policy & Agmarknet Reference

# COMMAND ----------

df_policy = spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/pmfby_policy.csv")
save_to_bronze_uc(df_policy, "pmfby_policy")

df_agmarknet = spark.read.option("header", "true").csv(f"{INPUT_VOLUME}/agmarknet_reference.csv")
save_to_bronze_uc(df_agmarknet, "agmarknet_reference")

# COMMAND ----------

print("🚀 BRONZE LAYER UC MIGRATION COMPLETE")
