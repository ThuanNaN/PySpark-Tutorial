#!/usr/bin/env python3
"""Topic P6: DataFrame

Demonstrates DataFrame creation with explicit schema definitions.

Run: python src/lessons/p06_dataframe/dataframe_schema.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

conf = load_spark_conf()
spark = create_spark(conf)

# Define a custom schema for taxi trip data
schema = StructType([
    StructField("VendorID", IntegerType(), nullable=False),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=True),
    StructField("DOLocationID", IntegerType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
])

# Create DataFrame with explicit schema
df = spark.read.schema(schema).parquet("data/yellow/yellow_tripdata_2023-01.parquet")
print(f"DataFrame created with custom schema: {df.count()} rows")
print(f"Columns: {[f.name for f in df.schema.fields]}")
df.printSchema()

# Show inferred vs explicit schema
inferred_df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")
print(f"\nInferred schema has {len(inferred_df.schema.fields)} fields")
print(f"Explicit schema has {len(df.schema.fields)} fields")
spark.stop()
