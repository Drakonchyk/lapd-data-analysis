# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import types as T


GOLD_CATALOG = "lapd"     
GOLD_SCHEMA  = "gold" 
SILVER_FQN   = "lapd.silver.crime"  

FACT_GOLD_TBL = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.fact_crime_incidents_gold"
MART_DAILY_TBL = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.mart_crime_daily_area"
MART_HOURLY_TBL = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.mart_crime_hourly_pattern"
MART_TYPE_TBL   = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.mart_crime_type_trends"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA}")

df = spark.table(SILVER_FQN)

def norm_str(c):
    return F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")

date_only = F.to_date("date_occ")
occ_ts = F.to_timestamp(
    F.concat_ws(
        " ",
        F.date_format(date_only, "yyyy-MM-dd"),
        F.format_string("%02d:%02d:00", F.col("hour_occ"), F.col("minute_occ"))
    )
)

gold_fact = (
    df
    # --- basic cleaning ---
    .withColumn("dr_no", norm_str("dr_no"))
    .withColumn("crime_category", norm_str("crime_category"))
    .withColumn("area_id", norm_str("area_id"))
    .withColumn("area_name", norm_str("area_name"))
    .withColumn("reporting_district", norm_str("reporting_district"))
    .withColumn("crime_code", norm_str("crime_code"))
    .withColumn("crime_description", norm_str("crime_description"))
    .withColumn("victim_sex", norm_str("victim_sex"))
    .withColumn("victim_descent", norm_str("victim_descent"))
    .withColumn("premise_description", norm_str("premise_description"))
    .withColumn("weapon_description", norm_str("weapon_description"))
    .withColumn("status_description", norm_str("status_description"))

    # --- typed fixes / sanity ---
    .withColumn("victim_age", F.when((F.col("victim_age") < 0) | (F.col("victim_age") > 110), None).otherwise(F.col("victim_age")))
    .withColumn("lat", F.when((F.col("lat") < -90) | (F.col("lat") > 90), None).otherwise(F.col("lat")))
    .withColumn("lon", F.when((F.col("lon") < -180) | (F.col("lon") > 180), None).otherwise(F.col("lon")))
    .withColumn("reporting_delay_days", F.when(F.col("reporting_delay_days") < 0, None).otherwise(F.col("reporting_delay_days")))

    # --- derived time columns ---
    .withColumn("occurrence_ts", occ_ts)
    .withColumn("occurrence_date", F.to_date("occurrence_ts"))
    .withColumn("year", F.year("occurrence_ts"))
    .withColumn("month", F.month("occurrence_ts"))
    .withColumn("weekday", F.date_format("occurrence_ts", "E"))  # Mon/Tue...

    # --- analytics-ready derived features ---
    .withColumn("is_violent", F.col("crime_category") == F.lit("Violent"))
    .withColumn("has_weapon", ~F.lower(F.col("weapon_description")).isin("no weapon / unknown", "unknown weapon/other weapon", "no weapon", "unknown"))
    .withColumn(
        "hour_bucket",
        F.when(F.col("hour_occ").between(0, 5), "Night")
         .when(F.col("hour_occ").between(6, 11), "Morning")
         .when(F.col("hour_occ").between(12, 17), "Afternoon")
         .otherwise("Evening")
    )
    .withColumn(
        "victim_age_group",
        F.when(F.col("victim_age").isNull(), "Unknown")
         .when(F.col("victim_age") <= 17, "Child")
         .when(F.col("victim_age") <= 25, "YoungAdult")
         .when(F.col("victim_age") <= 64, "Adult")
         .otherwise("Senior")
    )
    .withColumn("is_reported_late", F.col("reporting_delay_days") > F.lit(7))

    # --- select final schema ---
    .select(
        "dr_no",
        "date_occ", "hour_occ", "minute_occ",
        "occurrence_ts", "occurrence_date",
        "year", "month", "weekday",
        "area_id", "area_name", "reporting_district",
        "crime_code", "crime_description", "crime_category", "is_violent",
        "victim_age", "victim_age_group", "victim_sex", "victim_descent",
        "premise_description",
        "weapon_description", "has_weapon",
        "status_description",
        "lat", "lon",
        "reporting_delay_days", "is_reported_late",
        "ingest_date", "processing_datetime"
    )
)

w = (
    F.window(F.col("processing_datetime"), "100 years")  
)

from pyspark.sql.window import Window

dedup_w = Window.partitionBy("dr_no").orderBy(F.col("processing_datetime").desc())
gold_fact = (
    gold_fact
    .withColumn("rn", F.row_number().over(dedup_w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

(
    gold_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FACT_GOLD_TBL)
)

print("Wrote:", FACT_GOLD_TBL)
display(gold_fact.limit(20))


# COMMAND ----------



# COMMAND ----------


# 2) Build GOLD MARTS (aggregations for fast EDA / dashboards)


gold = spark.table(FACT_GOLD_TBL)

# ---- Mart 1: daily per area ----
mart_daily_area = (
    gold.groupBy("occurrence_date", "area_id", "area_name")
        .agg(
            F.count("*").alias("crime_count"),
            F.sum(F.col("is_violent").cast("int")).alias("violent_count"),
            F.avg("reporting_delay_days").alias("avg_reporting_delay_days"),
            F.expr("percentile(reporting_delay_days, 0.5)").alias("median_reporting_delay_days")
        )
        .withColumn("violent_share", F.col("violent_count") / F.col("crime_count"))
        .orderBy("occurrence_date", "area_id")
)

(
    mart_daily_area.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MART_DAILY_TBL)
)

print("Wrote:", MART_DAILY_TBL)
display(mart_daily_area.limit(20))

# ---- Mart 2: hourly pattern by weekday + category ----
mart_hourly = (
    gold.groupBy("weekday", "hour_occ", "crime_category")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("weekday", "hour_occ", "crime_category")
)

(
    mart_hourly.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MART_HOURLY_TBL)
)

print("Wrote:", MART_HOURLY_TBL)
display(mart_hourly.limit(20))
# ---- Mart 3: month trends by category ---
mart_type_trends = (
    gold.groupBy("year", "month", "crime_category")
        .agg(
            F.count("*").alias("crime_count"),
            F.sum(F.col("is_violent").cast("int")).alias("violent_count"),
        )
        .orderBy("year", "month", "crime_category")
)

(
    mart_type_trends.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MART_TYPE_TBL)
)

print("Wrote:", MART_TYPE_TBL)
display(mart_type_trends.limit(20))


# COMMAND ----------


# 3) Quick validation checks (Gold quality)

gold = spark.table(FACT_GOLD_TBL)

print("Gold rows:", gold.count())
print("Distinct dr_no:", gold.select("dr_no").distinct().count())

# Null checks for key columns
from pyspark.sql import functions as F

key_cols = ["dr_no", "occurrence_ts", "crime_category", "area_id", "area_name"]
nulls = gold.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in key_cols])
display(nulls)

# Sanity
geo_cov = gold.select(
    (F.sum((F.col("lat").isNotNull() & F.col("lon").isNotNull()).cast("int"))).alias("rows_with_geo"),
    F.count("*").alias("total_rows")
)
display(geo_cov)
