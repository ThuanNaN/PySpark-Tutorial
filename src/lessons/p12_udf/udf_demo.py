#!/usr/bin/env python3
"""Topic P12: UDF and Pandas UDF

Demonstrates custom UDFs and Pandas UDFs (vectorized functions) for transformations.

Run: python src/lessons/p12_udf/udf_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Regular UDF: row-at-a-time Python function
def classify_distance(dist):
    """Classify trip distance into categories."""
    if dist < 1:
        return "short"
    elif dist < 5:
        return "medium"
    else:
        return "long"

distance_udf = F.udf(classify_distance, StringType())
df2 = df.withColumn("distance_category", distance_udf(df.trip_distance))
print("Regular UDF - distance categories:")
df2.select("trip_distance", "distance_category").show(10, truncate=False)

# Pandas UDF (vectorized): operates on pandas Series for better performance
@pandas_udf(DoubleType())
def calculate_fee(pandas_series: pd.Series) -> pd.Series:
    """Calculate a flat booking fee of $2.50 on each trip."""
    return pandas_series + 2.50

df3 = df.withColumn("with_booking_fee", calculate_fee(df.total_amount))
print("\nPandas UDF - total_amount with booking fee:")
df3.select("total_amount", "with_booking_fee").show(5, truncate=False)

# Pandas UDF for grouping and aggregation
@pandas_udf("double")
def avg_per_group(x: pd.Series) -> float:
    """Calculate mean (for demo purposes)."""
    return x.mean()

print("\nUDFs demonstrate the tradeoff:")
print("- Regular UDF: row-at-a-time, serialized, slower")
print("- Pandas UDF: vectorized, batch processing, much faster")
spark.stop()
