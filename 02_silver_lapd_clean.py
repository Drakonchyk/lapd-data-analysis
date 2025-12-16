# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

CATALOG = "lapd"
BRONZE_TABLE = f"{CATALOG}.bronze.crime"
SILVER_TABLE = f"{CATALOG}.silver.crime"

df0 = spark.table(BRONZE_TABLE)

print("Bronze row count:", df0.count())
print("Bronze columns:", len(df0.columns))
df0.printSchema()
df0.limit(5).display()

# COMMAND ----------

df1 = df0.select(
    F.col("dr_no").alias("dr_no"),

    F.col("date_rptd").alias("date_reported_raw"),
    F.col("date_occ").alias("date_occ_raw"),
    F.col("time_occ").alias("time_occ_raw"),

    F.col("area").alias("area_id"),
    F.col("area_name").alias("area_name"),
    F.col("rpt_dist_no").alias("reporting_district"),

    F.col("part_1_2").alias("part_1_2"),
    F.col("crm_cd").alias("crime_code"),
    F.col("crm_cd_desc").alias("crime_description"),

    F.col("vict_age").alias("victim_age_raw"),
    F.col("vict_sex").alias("victim_sex_raw"),
    F.col("vict_descent").alias("victim_descent_raw"),

    F.col("premis_desc").alias("premise_description_raw"),
    F.col("weapon_desc").alias("weapon_description_raw"),
    F.col("status_desc").alias("status_description_raw"),

    F.col("lat").alias("lat_raw"),
    F.col("lon").alias("lon_raw"),

    F.col("ingest_date"),
    F.col("processing_datetime")
)

df1.printSchema()
df1.limit(5).display()


# COMMAND ----------

df2 = (
    df1
    .withColumn("date_reported", F.to_timestamp("date_reported_raw"))
    .withColumn("date_occ", F.to_timestamp("date_occ_raw"))
)

df2.select(
    "date_reported_raw", "date_reported",
    "date_occ_raw", "date_occ"
).limit(10).display()

print("NULL date_reported:", df2.filter(F.col("date_reported").isNull()).count())
print("NULL date_occ:", df2.filter(F.col("date_occ").isNull()).count())



# COMMAND ----------

from pyspark.sql.types import IntegerType

df3 = (
    df2
    .withColumn("time_occ_int", F.col("time_occ_raw").cast(IntegerType()))
    .withColumn("hour_occ", F.floor(F.col("time_occ_int") / 100))
    .withColumn("minute_occ", F.col("time_occ_int") % 100)
)

df3.select(
    "time_occ_raw", "time_occ_int", "hour_occ", "minute_occ"
).limit(10).display()

invalid_time = df3.filter(
    ~F.col("hour_occ").between(0, 23) |
    ~F.col("minute_occ").between(0, 59)
).count()

print("Rows with invalid time:", invalid_time)


# COMMAND ----------

df4 = (
    df3
    # victim age
    .withColumn(
        "victim_age",
        F.when(
            (F.col("victim_age_raw").cast("int") > 0) &
            (F.col("victim_age_raw").cast("int") < 120),
            F.col("victim_age_raw").cast("int")
        ).otherwise(None)
    )
    # sex
    .withColumn(
        "victim_sex",
        F.when(F.col("victim_sex_raw").isin("M", "F"), F.col("victim_sex_raw"))
         .otherwise("Unknown")
    )
    # descent
    .withColumn(
        "victim_descent",
        F.when(F.col("victim_descent_raw").isNull(), "Unknown")
         .otherwise(F.col("victim_descent_raw"))
    )
    # premise / weapon / status
    .withColumn(
        "premise_description",
        F.when(F.col("premise_description_raw").isNull(), "Unknown")
         .otherwise(F.col("premise_description_raw"))
    )
    .withColumn(
        "weapon_description",
        F.when(F.col("weapon_description_raw").isNull(), "No weapon / Unknown")
         .otherwise(F.col("weapon_description_raw"))
    )
    .withColumn(
        "status_description",
        F.when(F.col("status_description_raw").isNull(), "Unknown")
         .otherwise(F.col("status_description_raw"))
    )
)

df4.select(
    "victim_age_raw", "victim_age",
    "victim_sex_raw", "victim_sex",
    "weapon_description"
).limit(10).display()

df4.groupBy("victim_sex").count().orderBy(F.desc("count")).display()
print("Victim age NULLs:", df4.filter(F.col("victim_age").isNull()).count())


# COMMAND ----------

from pyspark.sql.types import DoubleType

df5 = (
    df4
    .withColumn("lat", F.col("lat_raw").cast(DoubleType()))
    .withColumn("lon", F.col("lon_raw").cast(DoubleType()))
)

before_geo = df5.count()

df5_clean = df5.filter(
    (F.col("lat").isNotNull()) &
    (F.col("lon").isNotNull()) &
    ~((F.col("lat") == 0) & (F.col("lon") == 0))
)

after_geo = df5_clean.count()

print("Rows before geo filter:", before_geo)
print("Rows after geo filter :", after_geo)
print("Dropped rows          :", before_geo - after_geo)

df5_clean.select("lat", "lon", "area_name").limit(10).display()


# COMMAND ----------

df6 = (
    df5_clean
    .withColumn("year", F.year("date_occ"))
    .withColumn("month", F.month("date_occ"))
    .withColumn("weekday", F.date_format("date_occ", "E"))
    .withColumn(
        "crime_category",
        F.when(F.col("part_1_2") == "1", "Violent")
         .when(F.col("part_1_2") == "2", "Non-Violent")
         .otherwise("Other")
    )
    .withColumn("reporting_delay_days", F.datediff("date_reported", "date_occ"))
)

df6.select(
    "date_occ", "year", "month", "weekday",
    "crime_category", "reporting_delay_days"
).limit(10).display()

df6.groupBy("crime_category").count().display()


# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

silver_df = df6.select(
    "dr_no",
    "date_occ",
    "hour_occ",
    "minute_occ",
    "year",
    "month",
    "weekday",
    "area_id",
    "area_name",
    "reporting_district",
    "crime_code",
    "crime_description",
    "crime_category",
    "victim_age",
    "victim_sex",
    "victim_descent",
    "premise_description",
    "weapon_description",
    "status_description",
    "lat",
    "lon",
    "reporting_delay_days",
    "ingest_date",
    "processing_datetime"
)

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("✅ Silver rows:", spark.table(SILVER_TABLE).count())
spark.table(SILVER_TABLE).limit(5).display()
