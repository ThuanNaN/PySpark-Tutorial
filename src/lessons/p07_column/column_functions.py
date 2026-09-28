#!/usr/bin/env python3
"""Topic P7: Column Operations

Demonstrates Column operations and built-in functions using withColumn, select,
and pyspark.sql.functions.

Run: python src/lessons/p07_column/column_functions.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Select specific columns
selected = df.select("VendorID", "trip_distance", "total_amount")
print(f"Selected columns: {[f.name for f in selected.schema.fields]}")

# withColumn to add derived columns
df2 = df.withColumn("fare_per_mile", F.when(df.trip_distance > 0, df.total_amount / df.trip_distance).otherwise(0))
print(f"\nAdded fare_per_mile column")
df2.select("trip_distance", "total_amount", "fare_per_mile").show(5, truncate=False)

# Built-in functions
upper_vendor = df.select(F.upper(F.col("VendorID").cast("string")).alias("vendor_upper"))
print("\nUpper vendor IDs:")
upper_vendor.show(5, truncate=False)

# Round and cast
df3 = df.withColumn("rounded_amount", F.round(df.total_amount, 2))
print(f"\nRounded amounts sample:")
df3.select("total_amount", "rounded_amount").show(5, truncate=False)
spark.stop()
