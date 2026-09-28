#!/usr/bin/env python3
"""Topic P15: Caching and Persistence

Demonstrates df.cache(), .persist(), and .checkpoint() for reusing intermediate DataFrames.

Run: python src/lessons/p15_caching/caching_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Cache the DataFrame in memory for reuse
cached_df = df.cache()
print(f"Cached DataFrame: {cached_df.count()} rows (triggers first action)")
print(f"Storage level: {cached_df.storageLevel}")

# Second action is faster because data is cached
print(f"\nSecond access (from cache): {cached_df.count()} rows")

# Persist with a specific storage level (memory and disk)
persisted_df = df.persist(StorageLevel.MEMORY_AND_DISK)
print(f"\nPersisted DataFrame storage level: {persisted_df.storageLevel}")
persisted_df.count()

# Checkpoint for lineage truncation (breaks the DAG)
# Requires a checkpoint directory
import tempfile, os
chk_dir = tempfile.mkdtemp()
spark.sparkContext.setCheckpointDir(chk_dir)

checkpointed_df = df.checkpoint()
print(f"\nCheckpointed DataFrame (lineage truncated): {checkpointed_df.count()} rows")
print(f"Checkpoint directory: {chk_dir}")

# Clean up
spark.stop()
import shutil
shutil.rmtree(chk_dir)

print("\nCaching summary:")
print("- .cache(): stores DataFrame in memory only (MEMORY_ONLY)")
print("- .persist(): allows specifying storage level (memory, disk, serialized)")
print("- .checkpoint(): truncates lineage to avoid long DAG chains")
