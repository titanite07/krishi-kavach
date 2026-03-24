# Databricks notebook source
# MAGIC %md
# MAGIC # Krishi-Kavach | 01 — Bronze Layer Ingestion (Production)
# MAGIC **Purpose:** Ingest real-world agricultural and weather datasets into the Bronze Delta layer.
# MAGIC
# MAGIC # MAGIC | Dataset | Source File | Delta Table |
# MAGIC |---------|-------------|-------------|
# MAGIC | IMD Grids | `imd_rainfall_2022.csv` | `imd_grids` |
# MAGIC | PMFBY Policies | `state-level-pmfby.csv` | `pmfby_policy` |
# MAGIC | Daily Rainfall | `daily_rainfall_melted.csv` | `daily_rainfall` |
# MAGIC | Mandi Prices | `mandi_prices_cleaned.csv` | `mandi_prices` |
# MAGIC | KCC Queries | `kcc_2022_filtered.csv` | `kcc_queries` |
# MAGIC | Agriculture Prices | `Agriculture_price_dataset.csv` | `agri_prices` |

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, concat_ws
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType
)

# ── Paths ────────────────────────────────────────────────────────────────────
INPUT_BASE  = "dbfs:/FileStore/krishi_kavach/input"
BRONZE_BASE = "dbfs:/FileStore/krishi_kavach/bronze"

def write_to_bronze(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{BRONZE_BASE}/{table_name}"))
    print(f"✅ Bronze | {table_name:<15} -> {df.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. IMD Gridded Rainfall (Processed NetCDF)

# COMMAND ----------

imd_schema = StructType([
    StructField("timestamp",                  StringType(), True),
    StructField("district",                   StringType(), True),
    StructField("grid_lat",                   DoubleType(), True),
    StructField("grid_lon",                   DoubleType(), True),
    StructField("seasonal_rain_mm",           DoubleType(), True),
    StructField("seasonal_rain_threshold_90", DoubleType(), True),
    StructField("actual_rain_mm",             DoubleType(), True),
])

df_imd = (spark.read.option("header", "true").schema(imd_schema).csv(f"{INPUT_BASE}/imd_rainfall_2022.csv")
          .withColumn("timestamp", to_timestamp(col("timestamp"))))

write_to_bronze(df_imd, "imd_grids")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PMFBY State-Level Policies

# COMMAND ----------

pmfby_schema = StructType([
    StructField("id",              IntegerType(), True),
    StructField("year",            IntegerType(), True),
    StructField("season",          StringType(),  True),
    StructField("scheme",          StringType(),  True),
    StructField("state_id",        IntegerType(), True),
    StructField("state_name",      StringType(),  True),
    StructField("farmer_count",    IntegerType(), True),
    StructField("land_area",       DoubleType(),  True),
    StructField("sum_insured",     DoubleType(),  True),
    StructField("gross_premium",   DoubleType(),  True),
    StructField("farmer_premium",  DoubleType(),  True),
    StructField("central_subsidy", DoubleType(),  True),
    StructField("state_subsidy",   DoubleType(),  True),
])

df_pmfby = spark.read.option("header", "true").schema(pmfby_schema).csv(f"{INPUT_BASE}/state-level-pmfby.csv")
write_to_bronze(df_pmfby, "pmfby_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Daily District-wise Rainfall (Melted)

# COMMAND ----------

daily_rain_schema = StructType([
    StructField("state",       StringType(), True),
    StructField("district",    StringType(), True),
    StructField("month",       StringType(), True),
    StructField("day",         IntegerType(), True),
    StructField("rainfall_mm", DoubleType(),  True),
])

# Note: We will construct a real date in the Silver layer
df_daily_rain = spark.read.option("header", "true").schema(daily_rain_schema).csv(f"{INPUT_BASE}/daily_rainfall_melted.csv")
write_to_bronze(df_daily_rain, "daily_rainfall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mandi Market Prices (Cleaned)

# COMMAND ----------

mandi_schema = StructType([
    StructField("State",        StringType(), True),
    StructField("District",     StringType(), True),
    StructField("Market",       StringType(), True),
    StructField("Commodity",    StringType(), True),
    StructField("Variety",      StringType(), True),
    StructField("Grade",        StringType(), True),
    StructField("Arrival_Date", StringType(), True),
    StructField("Min_Price",    DoubleType(), True),
    StructField("Max_Price",    DoubleType(), True),
    StructField("Modal_Price",  DoubleType(), True),
])

df_mandi = (spark.read.option("header", "true").schema(mandi_schema).csv(f"{INPUT_BASE}/mandi_prices_cleaned.csv")
            .withColumn("Arrival_Date", to_date(col("Arrival_Date"))))

write_to_bronze(df_mandi, "mandi_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. KCC Query Logs (Filtered 2022)

# COMMAND ----------

kcc_schema = StructType([
    StructField("Year",         IntegerType(), True),
    StructField("Month",        IntegerType(), True),
    StructField("Day",          IntegerType(), True),
    StructField("Crop",         StringType(),  True),
    StructField("DistrictName", StringType(),  True),
    StructField("QueryType",    StringType(),  True),
    StructField("Season",       StringType(),  True),
    StructField("Sector",       StringType(),  True),
    StructField("StateName",    StringType(),  True),
])

df_kcc = spark.read.option("header", "true").schema(kcc_schema).csv(f"{INPUT_BASE}/kcc_2022_filtered.csv")
write_to_bronze(df_kcc, "kcc_queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Historical Agri Prices

# COMMAND ----------

agri_price_schema = StructType([
    StructField("STATE",         StringType(), True),
    StructField("District Name", StringType(), True),
    StructField("Market Name",   StringType(), True),
    StructField("Commodity",     StringType(), True),
    StructField("Variety",       StringType(), True),
    StructField("Grade",         StringType(), True),
    StructField("Min_Price",     DoubleType(), True),
    StructField("Max_Price",     DoubleType(), True),
    StructField("Modal_Price",   DoubleType(), True),
    StructField("Price Date",    StringType(), True),
])

df_agri_prices = (spark.read.option("header", "true").schema(agri_price_schema).csv(f"{INPUT_BASE}/Agriculture_price_dataset.csv")
                  .withColumn("Price Date", to_date(col("Price Date"), "d/M/yyyy")))

write_to_bronze(df_agri_prices, "agri_prices")
