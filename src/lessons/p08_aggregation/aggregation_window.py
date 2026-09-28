#!/usr/bin/env python3
"""Topic P8: Aggregation and Window Functions

Demonstrates groupBy().agg() and window() functions for time-based analysis.

Run: python src/lessons/p08_aggregation/aggregation_window.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F
from pyspark.sql.window import Window

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# groupBy aggregation: total trips and revenue per pickup location
agg_df = (
    df.groupBy("PULocationID")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("trip_distance").alias("avg_distance")
    )
    .orderBy(F.desc("total_revenue"))
)
print("Top 5 pickup locations by revenue:")
agg_df.show(5, truncate=False)

# Window function: rank locations by revenue within groups
window_spec = Window.partitionBy("PULocationID").orderBy(F.desc("total_amount"))
df_with_rank = df.withColumn("revenue_rank", F.row_number().over(window_spec))
print("\nSample with window rank:")
df_with_rank.select("PULocationID", "total_amount", "revenue_rank").show(5, truncate=False)

# Time-based window aggregation
df2 = df.withColumn("hour", F.hour("tpep_pickup_datetime"))
hourly = df2.groupBy("hour").agg(F.count("*").alias("trips")).orderBy("hour")
print("\nTrips by hour of day:")
hourly.show(24, truncate=False)
spark.stop()
