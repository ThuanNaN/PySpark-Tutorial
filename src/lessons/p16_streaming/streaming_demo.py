#!/usr/bin/env python3
"""Topic P16: Structured Streaming

Demonstrates readStream, window aggregation with watermark, and writeStream.
Uses data/yellow/ as a simulated streaming source directory.

Run: python src/lessons/p16_streaming/streaming_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F

conf = load_spark_conf()
spark = create_spark(conf)

# Read from the Parquet directory as a simulated streaming source
schema = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet").schema

# Create a streaming DataFrame from the directory
stream_df = (
    spark.readStream
    .format("parquet")
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .load("data/yellow/")
)

# Apply watermark to handle late data within 10 minutes
events = stream_df.withWatermark("tpep_pickup_datetime", "10 minutes")

# Window aggregation over 15-minute intervals
windowed = (
    events.groupBy(
        F.window("tpep_pickup_datetime", "15 minutes"),
        "PULocationID"
    )
    .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
)

# Write to console in update mode, trigger once so it terminates automatically
query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(once=True)
    .start()
)

print("Streaming query started. Processing one trigger of windowed aggregations.")
query.awaitTermination()
spark.stop()
