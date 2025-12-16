# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lapd.gold.mart_crime_monthly_area AS
# MAGIC SELECT
# MAGIC     area_id,
# MAGIC     area_name,
# MAGIC     YEAR(occurrence_date) AS year,
# MAGIC     MONTH(occurrence_date) AS month,
# MAGIC     COUNT(*) AS crime_count,
# MAGIC     SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_count,
# MAGIC     AVG(reporting_delay_days) AS avg_reporting_delay_days,
# MAGIC     PERCENTILE(reporting_delay_days, 0.5) AS median_reporting_delay_days,
# MAGIC     SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS violent_share
# MAGIC FROM lapd.gold.fact_crime_incidents_gold
# MAGIC GROUP BY area_id, area_name, YEAR(occurrence_date), MONTH(occurrence_date);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lapd.gold.mart_crime_monthly_area

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lapd.gold.mart_crime_hourly_area AS
# MAGIC SELECT
# MAGIC     area_id,
# MAGIC     area_name,
# MAGIC     weekday,
# MAGIC     hour_occ,
# MAGIC     COUNT(*) AS crime_count
# MAGIC FROM lapd.gold.fact_crime_incidents_gold
# MAGIC GROUP BY area_id, area_name, weekday, hour_occ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lapd.gold.dim_area_geo (
# MAGIC     area_id STRING,
# MAGIC     area_name STRING,
# MAGIC     geojson STRING
# MAGIC );
# MAGIC