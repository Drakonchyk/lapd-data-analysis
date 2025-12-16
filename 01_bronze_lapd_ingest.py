# Databricks notebook source
from pyspark.sql import functions as F
import requests
import pandas as pd
import time

dbutils.widgets.text("year_from", "2020")
YEAR_FROM = int(dbutils.widgets.get("year_from"))

BASE_URL = "https://data.lacity.org/resource/2nrs-mtv8.json"

CATALOG = "lapd"
SCHEMA  = "bronze"
TABLE   = "crime"
TABLE_FQN = f"{CATALOG}.{SCHEMA}.{TABLE}"

LIMIT = 50000  



# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- METASTORE-LEVEL PRIVILEGES
# MAGIC -- =====================================================
# MAGIC GRANT CREATE CATALOG ON METASTORE TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE CATALOG ON METASTORE TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE CATALOG ON METASTORE TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CATALOG-LEVEL PRIVILEGES
# MAGIC -- =====================================================
# MAGIC GRANT USE CATALOG ON CATALOG lapd TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE SCHEMA ON CATALOG lapd TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT BROWSE ON CATALOG lapd TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG lapd TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE SCHEMA ON CATALOG lapd TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT BROWSE ON CATALOG lapd TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG lapd TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE SCHEMA ON CATALOG lapd TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT BROWSE ON CATALOG lapd TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- SCHEMA-LEVEL PRIVILEGES (BRONZE)
# MAGIC -- =====================================================
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.bronze TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.bronze TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.bronze TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.bronze TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.bronze TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.bronze TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- SCHEMA-LEVEL PRIVILEGES (SILVER)
# MAGIC -- =====================================================
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.silver TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.silver TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.silver TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.silver TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA lapd.silver TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT CREATE TABLE ON SCHEMA lapd.silver TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- TABLE-LEVEL PRIVILEGES (BRONZE)
# MAGIC -- =====================================================
# MAGIC GRANT SELECT ON TABLE lapd.bronze.crime TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.bronze.crime TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE lapd.bronze.crime TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.bronze.crime TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE lapd.bronze.crime TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.bronze.crime TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- TABLE-LEVEL PRIVILEGES (SILVER)
# MAGIC -- =====================================================
# MAGIC GRANT SELECT ON TABLE lapd.silver.crime TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.silver.crime TO `pavliuk.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE lapd.silver.crime TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.silver.crime TO `samoilenko.pn@ucu.edu.ua`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE lapd.silver.crime TO `pelekh.pn@ucu.edu.ua`;
# MAGIC GRANT MODIFY ON TABLE lapd.silver.crime TO `pelekh.pn@ucu.edu.ua`;
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Writing Bronze data to table: {TABLE_FQN}")


# COMMAND ----------

offset = 0
all_rows = []

while True:
    params = {
        "$limit": LIMIT,
        "$offset": offset,
        "$where": f"date_occ >= '{YEAR_FROM}-01-01T00:00:00'"
    }

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()

    batch = r.json()
    if not batch:
        break

    all_rows.extend(batch)
    offset += LIMIT
    print(f"Loaded {len(all_rows)} records so far...")

    time.sleep(0.2) 

print(f"✅ Total records loaded: {len(all_rows)}")


# COMMAND ----------

bronze_df = spark.createDataFrame(pd.DataFrame(all_rows))


bronze_df = (
    bronze_df
    .withColumn("ingest_date", F.current_date())
    .withColumn("processing_datetime", F.current_timestamp())
    .withColumn("data_source", F.lit("LAPD Socrata API"))
)


(
    bronze_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_FQN)
)

rows_written = spark.table(TABLE_FQN).count()
print(f"✅ Rows written to {TABLE_FQN}: {rows_written}")

spark.table(TABLE_FQN).limit(5).display()
