#!/usr/bin/env python3
"""Topic P2: Spark Architecture

Demonstrates why Spark exists vs Pandas by comparing
single-machine memory limitations with distributed processing.

Run: python src/lessons/p02_spark_architecture/spark_concept.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf

conf = load_spark_conf()
spark = create_spark(conf)

# Load a sample of the taxi data to show Spark's capabilities
df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")
print(f"DataFrame loaded: {df.count()} rows")
print(f"Schema: {df.schema.simpleString()}")
print(f"Number of executors: {spark.sparkContext.defaultParallelism}")
print("Spark distributes data across partitions and processes them in parallel,")
print("unlike Pandas which is limited to a single machine's memory.")
spark.stop()
