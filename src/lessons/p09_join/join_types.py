#!/usr/bin/env python3
"""Topic P9: Join Types

Demonstrates inner, left, and broadcast joins between DataFrames.

Run: python src/lessons/p09_join/join_types.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Create a small lookup DataFrame for locations
locations = spark.createDataFrame([
    (1, "Penn Station"), (2, "Ellis Island"), (3, "Times Square"),
    (100, "JFK Airport"), (101, "LaGuardia"), (102, "Brooklyn")
], ["PULocationID", "LocationName"])

# Inner join: only matching records
inner_join = df.join(locations, "PULocationID", "inner")
print(f"Inner join: {inner_join.count()} rows (only matching locations)")
inner_join.select("VendorID", "LocationName", "total_amount").show(5, truncate=False)

# Left join: all taxi records, with location names where available
left_join = df.join(locations, "PULocationID", "left")
print(f"\nLeft join: {left_join.count()} rows (all taxi records)")
left_join.select("PULocationID", "LocationName", "total_amount").show(5, truncate=False)

# Broadcast join: explicitly broadcast the small DataFrame for efficiency
broadcast_join = df.join(F.broadcast(locations), "PULocationID", "left")
print(f"\nBroadcast join: {broadcast_join.count()} rows")
print("Broadcast join sends the small locations table to all nodes, avoiding shuffle.")
spark.stop()
