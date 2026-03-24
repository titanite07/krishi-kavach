# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 01 — Bronze Layer Ingestion (Production)
# MAGIC **Purpose:** Ingest 6 real-world agricultural and weather datasets from DBFS into Delta format.
# MAGIC 
# MAGIC | Dataset | Source Path | Delta Sink | Status |
# MAGIC |---------|-------------|------------|--------|
# MAGIC | IMD Rainfall | `.../input/imd_rainfall_2022.csv` | `.../bronze/imd_rainfall` | Production |
# MAGIC | Mandi Prices | `.../input/mandi_prices.csv` | `.../bronze/mandi_prices` | Production |
# MAGIC | Daily Rainfall | `.../input/district_daily_rainfall.csv` | `.../bronze/district_daily_rainfall` | Production |
# MAGIC | KCC Queries | `.../input/kcc_2022.csv` | `.../bronze/kcc_2022` | Production |
# MAGIC | PMFBY Policies | `.../input/pmfby_policy.csv` | `.../bronze/pmfby_policy` | Production |
# MAGIC | AGMARKNET | `.../input/agmarknet_reference.csv` | `.../bronze/agmarknet_reference` | Production |

# COMMAND ----------

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.functions import (
    col, to_date, lit, expr, concat_ws, lpad, 
    filter as spark_filter, upper, initcap, regexp_extract
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)

# ── Paths ────────────────────────────────────────────────────────────────────
INPUT_BASE  = "dbfs:/FileStore/krishi_kavach/input"
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"

# ── District Normalization Utility ──────────────────────────────────────────
# Mapping from 00_district_crt.py (redefined here for independence)
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
def save_to_bronze(df, table_name):
    # Apply normalization to 'district' column right before saving
    df = df.withColumn("district", normalize_udf(col("district")))
    
    path = f"{BRONZE_BASE}/{table_name}"
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
    
    # Post-normalization check: Display any district NOT in CRT for manual review
    print(f"\n✅ TABLE: {table_name}")
    df_delta = spark.read.format("delta").load(path)
    df_unknown = df_delta.filter(~lower(col("district")).isin(list(DISTRICT_CRT.keys()))).select("district").distinct()
    if df_unknown.count() > 0:
        print(f"⚠️  UNMAPPED DISTRICTS (Check CRT):")
        display(df_unknown.limit(5))
    
    print(f"📊 ROW COUNT: {df_delta.count():,}")
    display(df_delta.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. IMD Rainfall Ingestion
# MAGIC *Logic: Basic schema casting and null removal.*

# COMMAND ----------

imd_schema = StructType([
    StructField("district",    StringType(), True),
    StructField("date",        StringType(), True),
    StructField("rainfall_mm", DoubleType(), True),
])

df_imd = (
    spark.read.option("header", "true")
    .schema(imd_schema)
    .csv(f"{INPUT_BASE}/imd_rainfall_2022.csv")
    .withColumn("date", to_date(col("date")))
    .dropna(subset=["district", "rainfall_mm"])
)

save_to_bronze(df_imd, "imd_rainfall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mandi Market Prices Ingestion
# MAGIC *Logic: XML artifact cleanup (non-numeric filter) and ISO date normalization.*

# COMMAND ----------

mandi_schema = StructType([
    StructField("district",        StringType(), True),
    StructField("date",            StringType(), True),
    StructField("commodity",       StringType(), True),
    StructField("modal_price",     StringType(), True), # to filter XML strings
    StructField("arrivals_tonnes", DoubleType(), True),
])

df_mandi = (
    spark.read.option("header", "true")
    .schema(mandi_schema)
    .csv(f"{INPUT_BASE}/mandi_prices.csv")
    # Filter rows where modal_price is NOT numeric (remove XML artifacts like 'x0020')
    .filter(col("modal_price").cast("double").isNotNull())
    .withColumn("modal_price", col("modal_price").cast(DoubleType()))
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .dropna(subset=["district", "date", "modal_price"])
)

save_to_bronze(df_mandi, "mandi_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. District-wise Daily Rainfall (Wide-to-Long)
# MAGIC *Logic: Melting 31 daily columns into a single date-driven series.*

# COMMAND ----------

# We read raw then melt
df_wide_rain = spark.read.option("header", "true").csv(f"{INPUT_BASE}/district_daily_rainfall.csv")

# Identify day columns (D1, D2 ... D31)
day_cols = [f"D{i}" for i in range(1, 32)]

# Melt logic using stack expression
stack_expr = f"stack({len(day_cols)}, " + ", ".join([f"'{c}', {c}" for c in day_cols]) + ") as (day_raw, rainfall_mm)"

df_long_rain = (
    df_wide_rain
    .select("district", "year", "month", expr(stack_expr))
    # Extract numeric day: "D1" -> 1
    .withColumn("day_num", regexp_extract(col("day_raw"), r"(\d+)", 1))
    # Construct date: YYYY-MM-DD
    .withColumn("date_str", concat_ws("-", col("year"), lpad(col("month"), 2, "0"), lpad(col("day_num"), 2, "0")))
    .withColumn("date",      to_date(col("date_str")))
    .withColumn("rainfall_mm", col("rainfall_mm").cast(DoubleType()))
    .dropna(subset=["rainfall_mm", "date"])
    .select("district", "date", "rainfall_mm")
)

save_to_bronze(df_long_rain, "district_daily_rainfall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Kisan Call Center (KCC) Stress Inquiries
# MAGIC *Logic: Filtering for stress-related categories only.*

# COMMAND ----------

kcc_schema = StructType([
    StructField("district",   StringType(), True),
    StructField("date",       StringType(), True),
    StructField("category",   StringType(), True),
    StructField("query_text", StringType(), True),
])

stress_categories = ['Weather', 'Pest', 'Disease', 'Flood', 'Drought']

df_kcc = (
    spark.read.option("header", "true")
    .schema(kcc_schema)
    .csv(f"{INPUT_BASE}/kcc_2022.csv")
    .filter(col("category").isin(stress_categories))
    .withColumn("date", to_date(col("date")))
    .dropna(subset=["district"])
)

save_to_bronze(df_kcc, "kcc_2022")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. PMFBY Insurance Policy Master
# MAGIC *Logic: Policy validation and zero-sum filtering.*

# COMMAND ----------

pmfby_schema = StructType([
    StructField("district",    StringType(), True),
    StructField("crop",        StringType(), True),
    StructField("sum_insured", DoubleType(), True),
    StructField("payout_rate", DoubleType(), True),
    StructField("season",      StringType(), True),
])

df_pmfby = (
    spark.read.option("header", "true")
    .schema(pmfby_schema)
    .csv(f"{INPUT_BASE}/pmfby_policy.csv")
    # Validation: Sum Insured must be > 0
    .filter((col("sum_insured").isNotNull()) & (col("sum_insured") > 0))
    .dropna(subset=["district", "crop"])
)

save_to_bronze(df_pmfby, "pmfby_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AGMARKNET Market Reference Data
# MAGIC *Logic: Title Case standardization for district names.*

# COMMAND ----------

agmarknet_schema = StructType([
    StructField("district",    StringType(), True),
    StructField("mandi_name",  StringType(), True),
    StructField("crop",        StringType(), True),
    StructField("year",        IntegerType(), True),
    StructField("min_price",   DoubleType(), True),
    StructField("max_price",   DoubleType(), True),
    StructField("modal_price", DoubleType(), True),
])

df_agmarknet = (
    spark.read.option("header", "true")
    .schema(agmarknet_schema)
    .csv(f"{INPUT_BASE}/agmarknet_reference.csv")
    # Standardize District: "AHMEDNAGAR" -> "Ahmednagar"
    .withColumn("district", initcap(col("district")))
    .dropna(subset=["district", "modal_price"])
)

save_to_bronze(df_agmarknet, "agmarknet_reference")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏁 Summary Table
# MAGIC ***

# COMMAND ----------

print("=" * 60)
print("  Krishi-Kavach · Bronze Layer Ingestion Complete")
print("=" * 60)
print(f"  Total Tables Optimized: 6")
print("  Location: dbfs:/FileStore/krishi_kavach/bronze/")
print("=" * 60)
