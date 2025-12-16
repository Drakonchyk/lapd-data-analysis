# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# COMMAND ----------

CATALOG = "lapd"
GOLD_SCHEMA = "gold"

FACT_TBL = f"{CATALOG}.{GOLD_SCHEMA}.fact_crime_incidents_gold"
MART_DAILY_TBL = f"{CATALOG}.{GOLD_SCHEMA}.mart_crime_daily_area"
MART_HOURLY_TBL = f"{CATALOG}.{GOLD_SCHEMA}.mart_crime_hourly_pattern"
MART_TYPE_TBL = f"{CATALOG}.{GOLD_SCHEMA}.mart_crime_type_trends"

# Load data
fact = spark.table(FACT_TBL)
mart_daily = spark.table(MART_DAILY_TBL)
mart_hourly = spark.table(MART_HOURLY_TBL)
mart_type = spark.table(MART_TYPE_TBL)


# COMMAND ----------

fact.select(
    F.count("*").alias("total_incidents"),
    F.min("occurrence_date").alias("start_date"),
    F.max("occurrence_date").alias("end_date")
).display()
fact.limit(10).display()


# COMMAND ----------

daily_counts = (
    fact.groupBy("occurrence_date")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("occurrence_date")
        .toPandas()
)

plt.figure(figsize=(12, 4))
plt.plot(daily_counts["occurrence_date"], daily_counts["crime_count"])
plt.title("Кількість злочинів у часі (щоденно)")
plt.xlabel("Дата")
plt.ylabel("К-ть злочинів")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

Z_THRESHOLD = 3
ROLLING_DAYS = 7


daily_pd = (
    fact.groupBy("occurrence_date")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("occurrence_date")
        .toPandas()
)

daily_pd["occurrence_date"] = pd.to_datetime(daily_pd["occurrence_date"])
daily_pd = daily_pd.sort_values("occurrence_date").reset_index(drop=True)

rolling_mean = daily_pd["crime_count"].shift(1).rolling(ROLLING_DAYS).mean()
rolling_std  = daily_pd["crime_count"].shift(1).rolling(ROLLING_DAYS).std(ddof=1)

daily_pd["rolling_mean"] = rolling_mean
daily_pd["rolling_std"]  = rolling_std
daily_pd["z_score"]      = (daily_pd["crime_count"] - daily_pd["rolling_mean"]) / daily_pd["rolling_std"]
daily_pd["is_anomaly"]   = daily_pd["z_score"].abs() >= Z_THRESHOLD

daily_pd["weekday"] = daily_pd["occurrence_date"].dt.strftime("%a")    
daily_pd["day_of_month"] = daily_pd["occurrence_date"].dt.day

daily_pd_valid = daily_pd.dropna(subset=["rolling_std"])

anoms = daily_pd_valid[daily_pd_valid["is_anomaly"]].copy()

weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
weekday_counts = (
    anoms.groupby("weekday")
        .size()
        .reindex(weekday_order, fill_value=0)
)


dom_counts = (
    anoms.groupby("day_of_month")
        .size()
        .reindex(range(1, 32), fill_value=0)
)

weekday_summary_pd = weekday_counts.reset_index()
weekday_summary_pd.columns = ["weekday", "anomaly_days"]

dom_summary_pd = dom_counts.reset_index()
dom_summary_pd.columns = ["day_of_month", "anomaly_days"]

display(weekday_summary_pd)
display(dom_summary_pd)

plt.figure(figsize=(8, 4))
plt.bar(weekday_counts.index, weekday_counts.values)
plt.title(f"Аномалії за днем тижня (|z| ≥ {Z_THRESHOLD})")
plt.xlabel("День тижня")
plt.ylabel("К-ть аномальних днів")
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 4))
plt.bar(dom_counts.index.astype(int), dom_counts.values)
plt.title(f"Аномалії за днем місяця (|z| ≥ {Z_THRESHOLD})")
plt.xlabel("День місяця")
plt.ylabel("К-ть аномальних днів")
plt.xticks(range(1, 32))
plt.tight_layout()
plt.show()

top_anoms = (
    anoms.sort_values("z_score", key=lambda s: s.abs(), ascending=False)
        .loc[:, ["occurrence_date", "crime_count", "rolling_mean", "rolling_std", "z_score", "weekday", "day_of_month"]]
        .head(30)
)

display(top_anoms)


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

hourly_counts = (
    fact.groupBy("hour_occ")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("hour_occ")
        .toPandas()
)

plt.figure(figsize=(10, 4))
plt.plot(hourly_counts["hour_occ"], hourly_counts["crime_count"])
plt.xticks(range(0, 24))
plt.xlabel("Година доби")
plt.ylabel("К-ть злочинів")
plt.title("Розподіл злочинів за годинами доби")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()


# COMMAND ----------


hourly_by_category = (
    fact.groupBy("hour_occ", "crime_category")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("hour_occ", "crime_category")
        .toPandas()
)

pivot_hourly = hourly_by_category.pivot(
    index="hour_occ",
    columns="crime_category",
    values="crime_count"
).fillna(0)

plt.figure(figsize=(10, 4))
for col in pivot_hourly.columns:
    plt.plot(pivot_hourly.index, pivot_hourly[col], label=col)

plt.xticks(range(0, 24))
plt.xlabel("Година доби")
plt.ylabel("К-ть злочинів")
plt.title("Розподіл злочинів за годинами (Violent vs Non-Violent)")
plt.legend()
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()


# COMMAND ----------

fact.select("crime_category").distinct().display()


# COMMAND ----------

from pyspark.sql import functions as F

crime_codes_full = (
    fact.groupBy("crime_code", "crime_description")
        .agg(F.count("*").alias("crime_count"))
        .orderBy(F.desc("crime_count"))
)

crime_codes_full.display()


# COMMAND ----------

from pyspark.sql import functions as F


# Domestic & child-related 
domestic_child = [
    626, 236,  # intimate partner assaults
    901, 902, 900, 903,  # restraining/court related часто йдуть разом з domestic кейсами
    812, 627, 235, 237, 813, 870, 922,  # child-related
    954  # contributing (часто juvenile-related), можна лишити тут або в legal
]

# Sexual crimes 
sexual = [
    121, 122, 815, 820, 821, 810, 760, 830, 840,
    850, 860, 762, 814,
    805, 806, 822, 921 
]

# Violent crimes (assaults, threats, robbery, kidnapping, homicide, severe violence)
violent = [
    624, 625, 623, 622, 
    230, 231, 236,     
    210, 220,          
    930, 928,          
    910, 920, 434,      
    110, 113,         
    647,               
    435, 436,        
    753, 251, 250,    
    755               
]

# Property crimes 
property_crimes = [

    510, 520, 522, 433,
    330, 410, 310, 320,

    440, 441, 341, 350, 351, 450,
    352, 452,
    470, 471, 473, 474, 475,
    446, 349,
    442, 343, 443,
    420, 331, 421,
    480, 485, 487,

    740, 745, 648,

    354, 662, 664, 666, 668, 670,
    649, 660,
    653, 654, 651, 652,
    951, 950,
    347,
    661,  
    940  
]

# Public order / quality-of-life
public_order = [
    888, 886, 890, 884, 882, 880,
    933, 932,        
    437,        
    438,           
    439,             
    949,          
    353, 453,        
    432,           
    926        
]

# Legal / compliance 
legal_compliance = [
    845,             
    904, 906,        
    942,           
    944,             
    948,           
    924              
]

# Weapon-related 
weapon_related = [
    761, 756, 931
]

fact_macro = (
    fact.withColumn("crime_code_i", F.col("crime_code").cast("int"))
        .withColumn(
            "macro_category",
            F.when(F.col("crime_code_i").isin(domestic_child), "Domestic & child-related")
             .when(F.col("crime_code_i").isin(sexual), "Sexual crimes")
             .when(F.col("crime_code_i").isin(violent), "Violent crimes")
             .when(F.col("crime_code_i").isin(property_crimes), "Property crimes")
             .when(F.col("crime_code_i").isin(public_order), "Public order crimes")
             .when(F.col("crime_code_i").isin(legal_compliance), "Legal / compliance")
             .when(F.col("crime_code_i").isin(weapon_related), "Weapon-related")
             .otherwise("Other / rare")
        )
        .drop("crime_code_i")
)

other_stats = (
    fact_macro.groupBy("macro_category")
              .agg(F.count("*").alias("crime_count"))
              .withColumn("share", F.col("crime_count") / F.sum("crime_count").over(Window.partitionBy()))
              .orderBy(F.desc("crime_count"))
)

other_stats.display()


# COMMAND ----------

macro_dist = (
    fact_macro.groupBy("macro_category")
              .agg(F.count("*").alias("crime_count"))
              .orderBy(F.desc("crime_count"))
              .toPandas()
)

plt.figure(figsize=(8, 4))
plt.barh(macro_dist["macro_category"], macro_dist["crime_count"])
plt.xlabel("К-ть злочинів")
plt.title("Розподіл злочинів за макрокатегоріями")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

other_count = (
    fact_macro
    .filter(F.col("macro_category") == "Other / rare")
    .count()
)

total_count = fact_macro.count()

print(f"Other / rare crimes: {other_count}")
print(f"Total crimes: {total_count}")
print(f"Share of Other / rare: {other_count / total_count:.4%}")


# COMMAND ----------

other_breakdown = (
    fact_macro
    .filter(F.col("macro_category") == "Other / rare")
    .groupBy("crime_code", "crime_description")
    .agg(F.count("*").alias("crime_count"))
    .orderBy(F.desc("crime_count"))
)

other_breakdown.display()


# COMMAND ----------

from pyspark.sql import functions as F

area_summary = (
    fact_macro
    .groupBy("area_id", "area_name")
    .agg(F.count("*").alias("crime_count"))
    .orderBy(F.desc("crime_count"))
)

area_summary.display()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


area_totals = (
    fact_macro
    .groupBy("area_name")
    .agg(F.count("*").alias("total_crimes"))
)


area_violent = (
    fact_macro
    .filter(F.col("macro_category") == "Violent crimes")
    .groupBy("area_name")
    .agg(F.count("*").alias("violent_crimes"))
)


area_violent_share = (
    area_totals
    .join(area_violent, on="area_name", how="left")
    .fillna(0, subset=["violent_crimes"])
    .withColumn(
        "violent_share",
        F.col("violent_crimes") / F.col("total_crimes")
    )
    .orderBy(F.desc("violent_share"))
)

area_violent_share.display()


# COMMAND ----------

import matplotlib.pyplot as plt

TOP_N = 10

top_share_df = (
    area_violent_share
    .select("area_name", "violent_share", "violent_crimes", "total_crimes")
    .limit(TOP_N)
)

top_total_areas = (
    area_totals
    .orderBy(F.desc("total_crimes"))
    .limit(TOP_N)
    .select("area_name")
    .toPandas()["area_name"]
    .tolist()
)

pdf = top_share_df.toPandas()
pdf["violent_share_pct"] = pdf["violent_share"] * 100

pdf["color"] = pdf["area_name"].apply(
    lambda x: "tab:red" if x in top_total_areas else "tab:blue"
)

plt.figure(figsize=(10, 5))
plt.barh(
    pdf["area_name"][::-1],
    pdf["violent_share_pct"][::-1],
    color=pdf["color"][::-1]
)

plt.xlabel("Violent crimes share (%)")
plt.ylabel("Area")
plt.title("Top-10 areas by violent crime share\n(Red = also top-10 by total crimes)")
plt.grid(axis="x", linestyle="--", alpha=0.4)
plt.show()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt


top10_pd = (
    area_violent_share
    .limit(10)
    .toPandas()
)

plt.figure(figsize=(10, 5))

plt.bar(
    top10_pd["area_name"],
    top10_pd["violent_share"],
    color="#d73027" 
)

plt.ylabel("Частка насильницьких злочинів")
plt.xlabel("Район")
plt.title("ТОП-10 районів за часткою насильницьких злочинів")
plt.xticks(rotation=45, ha="right")


plt.grid(axis="y", linestyle="--", alpha=0.3)

plt


# COMMAND ----------

# top_areas = (
#     fact_macro
#     .groupBy("area_name")
#     .count()
#     .orderBy(F.desc("count"))
#     .limit(5)
#     .select("area_name")
# )

# area_macro_top = (
#     area_macro_share
#     .join(top_areas, on="area_name", how="inner")
# )


# COMMAND ----------

from pyspark.sql import functions as F


area_total = (
    fact_macro
    .groupBy("area_name")
    .agg(F.count("*").alias("total_crimes"))
)


area_night = (
    fact_macro
    .filter(F.col("hour_occ").between(0, 5))
    .groupBy("area_name")
    .agg(F.count("*").alias("night_crimes"))
)


area_night_stats = (
    area_total
    .join(area_night, on="area_name", how="left")
    .fillna(0, subset=["night_crimes"])
    .withColumn(
        "night_crime_share",
        F.col("night_crimes") / F.col("total_crimes")
    )
    .orderBy(F.desc("night_crime_share"))
)

area_night_stats.display()


# COMMAND ----------

import matplotlib.pyplot as plt

night_pd = area_night_stats.toPandas()

plt.figure(figsize=(10, 5))
plt.bar(
    night_pd["area_name"],
    night_pd["night_crime_share"],
    color="#4575b4"
)

plt.ylabel("Частка нічних злочинів (00–05)")
plt.xlabel("Район")
plt.title("Частка злочинів у нічний час по районах")
plt.xticks(rotation=45, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.3)
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

crime_type_counts = (
    fact_macro
    .groupBy("crime_code", "crime_description")
    .agg(F.count("*").alias("crime_count"))
)

crime_types_50 = (
    crime_type_counts
    .filter(F.col("crime_count") > 50)
    .select("crime_code", "crime_description")
)

crime_reporting_delay = (
    fact_macro
    .join(crime_types_50, on=["crime_code", "crime_description"], how="inner")
    .groupBy("crime_code", "crime_description")
    .agg(
        F.count("*").alias("crime_count"),
        F.expr("percentile_approx(reporting_delay_days, 0.5)").alias(
            "median_reporting_delay_days"
        )
    )
    .orderBy(F.desc("median_reporting_delay_days"))
)

crime_reporting_delay.display()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt


MIN_COUNT = 50
NIGHT_START = 0
NIGHT_END = 5


crime_type_stats = (
    fact_macro
    .groupBy("crime_code", "crime_description")
    .agg(
        F.count("*").alias("crime_count"),
        F.avg(F.when(F.col("hour_occ").between(NIGHT_START, NIGHT_END), F.lit(1)).otherwise(F.lit(0))).alias("night_share"),
        F.expr("percentile_approx(hour_occ, 0.5)").alias("median_hour_occ"),
        F.expr("percentile_approx(reporting_delay_days, 0.5)").alias("median_reporting_delay_days")
    )
    .filter(F.col("crime_count") > MIN_COUNT)
)

crime_type_stats.display()

night_leaning = (
    crime_type_stats
    .filter(F.col("night_share") >= 0.30)
    .orderBy(F.desc("crime_count"))
)

night_leaning.display()


night_fast_report = (
    night_leaning
    .orderBy(F.asc("median_reporting_delay_days"))
    .select("crime_code", "crime_description", "crime_count", "night_share", "median_hour_occ", "median_reporting_delay_days")
    .limit(10)
)

night_slow_report = (
    night_leaning
    .orderBy(F.desc("median_reporting_delay_days"))
    .select("crime_code", "crime_description", "crime_count", "night_share", "median_hour_occ", "median_reporting_delay_days")
    .limit(10)
)

print("=== Night-leaning types with FAST reporting (low median delay) ===")
night_fast_report.display()

print("=== Night-leaning types with SLOW reporting (high median delay) ===")
night_slow_report.display()


plot_pd = (
    crime_type_stats
    .select("crime_description", "crime_count", "night_share", "median_reporting_delay_days")
    .toPandas()
)

plt.figure(figsize=(9, 5))
plt.scatter(plot_pd["night_share"], plot_pd["median_reporting_delay_days"], alpha=0.6)

plt.xlabel("Night share of incidents (00–05)")
plt.ylabel("Median reporting delay (days)")
plt.title("Night activity vs reporting delay by crime type (>50 incidents)")
plt.grid(axis="y", linestyle="--", alpha=0.3)
plt.tight_layout()
plt.show()


top10_types = plot_pd.sort_values("crime_count", ascending=False).head(10)

plt.figure(figsize=(9, 5))
plt.scatter(plot_pd["night_share"], plot_pd["median_reporting_delay_days"], alpha=0.35)
plt.scatter(top10_types["night_share"], top10_types["median_reporting_delay_days"], alpha=0.9)

for _, r in top10_types.iterrows():
    plt.text(r["night_share"], r["median_reporting_delay_days"], r["crime_description"][:18], fontsize=8)

plt.xlabel("Night share of incidents (00–05)")
plt.ylabel("Median reporting delay (days)")
plt.title("Top-10 frequent crime types highlighted")
plt.grid(axis="y", linestyle="--", alpha=0.3)
plt.tight_layout()
plt.show()
