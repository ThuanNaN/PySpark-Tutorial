#!/usr/bin/env python3
"""Topic P11: Spark SQL

Demonstrates SQL queries, temp views, and catalog operations.

Run: python src/lessons/p11_spark_sql/spark_sql_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Register DataFrame as a temporary view
df.createOrReplaceTempView("trips")
print("Temp view 'trips' created")

# Run SQL queries
result = spark.sql("SELECT PULocationID, COUNT(*) as trip_count, SUM(total_amount) as revenue FROM trips GROUP BY PULocationID ORDER BY revenue DESC LIMIT 5")
print("\nTop 5 pickup locations by revenue (SQL):")
result.show(truncate=False)

# SQL with computed columns
avg_result = spark.sql("SELECT CAST(VendorID AS STRING) as vendor, AVG(trip_distance) as avg_dist FROM trips WHERE trip_distance > 0 GROUP BY vendor ORDER BY avg_dist DESC LIMIT 5")
print("\nAverage trip distance by vendor (SQL):")
avg_result.show(truncate=False)

# Catalog operations
catalog = spark.catalog
print(f"\nTables in catalog: {catalog.listTables()}")
print(f"Current database: {spark.sql('SELECT CURRENT_DATABASE()').collect()[0][0]}")
print(f"Spark version: {spark.sql('SELECT SPARK_VERSION()').collect()[0][0]}")
spark.stop()
