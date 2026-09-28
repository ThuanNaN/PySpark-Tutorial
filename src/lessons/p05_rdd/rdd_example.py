#!/usr/bin/env python3
"""Topic P5: RDD

Demonstrates RDD creation from DataFrame, transformations, and actions.

Run: python src/lessons/p05_rdd/rdd_example.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")
# Convert DataFrame to RDD
rdd = df.rdd
print(f"RDD created with {rdd.getNumPartitions()} partitions")

# RDD transformations
pickup_rdd = rdd.map(lambda row: row["PULocationID"])
unique_locations = pickup_rdd.distinct().count()
print(f"Unique pickup locations: {unique_locations}")

# RDD actions
total = rdd.map(lambda row: float(row["total_amount"])).reduce(lambda a, b: a + b)
print(f"Total amount from RDD reduce: ${total:.2f}")

total_amounts = rdd.map(lambda row: float(row["total_amount"]))
avg = total_amounts.sum() / total_amounts.count()
print(f"Average total amount from RDD: ${avg:.2f}")
spark.stop()
