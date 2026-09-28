#!/usr/bin/env python3
"""Topic P13: Performance Tuning

Demonstrates AQE (Adaptive Query Execution), shuffle partition adjustment,
and caching strategies.

Run: python src/lessons/p13_performance/tuning_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

# Check AQE configuration
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
print(f"AQE enabled: {aqe_enabled}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Adjust shuffle partitions for the workload
spark.conf.set("spark.sql.shuffle.partitions", "4")
print(f"\nAdjusted shuffle partitions to: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Demonstrate AQE benefits: coalesce small partitions after shuffle
agg_df = (
    df.groupBy("PULocationID")
    .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
)
print(f"\nAggregation result: {agg_df.count()} rows")
print("AQE automatically coalesces shuffle partitions and optimizes join strategies at runtime.")

# Show query plan
print("\nQuery execution plan (optimized):")
agg_df.explain(mode="formatted")
spark.stop()
