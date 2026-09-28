#!/usr/bin/env python3
"""Topic P10: Data Sources

Demonstrates reading and writing Parquet, CSV, and JSON formats.

Run: python src/lessons/p10_data_sources/read_write_formats.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
import os

conf = load_spark_conf()
spark = create_spark(conf)

# Read Parquet (columnar format - most efficient)
parquet_df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")
print(f"Parquet: {parquet_df.count()} rows read")
print(f"Parquet schema fields: {len(parquet_df.schema.fields)}")

# Read CSV format (write a small sample first, then read back)
sample_path = "data/sample_trips.csv"
parquet_df.limit(100).coalesce(1).write.mode("overwrite").csv(sample_path, header=True)
csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
print(f"\nCSV: {csv_df.count()} rows read back")
print(f"CSV columns: {[f.name for f in csv_df.schema.fields][:5]}")

# Write to JSON
json_path = "data/sample_trips.json"
parquet_df.limit(50).coalesce(1).write.mode("overwrite").json(json_path)
json_df = spark.read.json(json_path)
print(f"\nJSON: {json_df.count()} rows read back")

# Compare: Parquet is more efficient - check file sizes
print("\nFormat comparison:")
print("Parquet: columnar storage, supports predicate pushdown and column pruning")
print("CSV: row-based, human-readable, larger file sizes")
print("JSON: row-based, nested structures, flexible but slower")

# Cleanup temp files
for p in [sample_path, json_path]:
    if os.path.exists(p):
        import shutil
        shutil.rmtree(p)
spark.stop()
