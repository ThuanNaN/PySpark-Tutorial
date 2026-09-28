#!/usr/bin/env python3
"""Topic P14: Broadcast Join and Accumulators

Demonstrates explicit broadcast join for small tables and accumulators for counting.

Run: python src/lessons/p14_broadcast/broadcast_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Create a small lookup table for locations
locations = spark.createDataFrame([
    (1, "Penn Station"), (2, "Ellis Island"), (3, "Times Square"),
    (100, "JFK Airport"), (101, "LaGuardia"), (102, "Brooklyn")
], ["PULocationID", "LocationName"])

# Explicit broadcast join: the small table is sent to all worker nodes
broadcast_join = df.join(F.broadcast(locations), "PULocationID", "left")
print(f"Broadcast join: {broadcast_join.count()} rows")
print("Using F.broadcast() hints Spark to send the small 'locations' table to every executor,")
print("avoiding a costly shuffle of the large taxi DataFrame.")

# Accumulator: a variable that is only "added" to across tasks (used for counting)
trip_counter = spark.sparkContext.accumulator(0)

def count_trips(row):
    global trip_counter
    trip_counter += 1
    return row

# Use accumulator in a map operation
_ = df.rdd.map(count_trips).count()
print(f"\nAccumulator count of trips processed: {trip_counter.value}")
print("Accumulators are write-only variables used for counters and sums across the cluster.")
spark.stop()
