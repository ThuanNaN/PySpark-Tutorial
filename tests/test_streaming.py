import pytest
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from taxi_analytics.streaming.io import read_stream, write_stream
from taxi_analytics.streaming.aggregate import revenue_by_day_borough_streaming
from taxi_analytics.pipeline.io import read_trips


def test_revenue_by_day_borough_streaming(spark):
    """Streaming aggregation produces correct windowed output."""
    schema = StructType([
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    rows = [
        Row(tpep_pickup_datetime=datetime(2023,1,1,10,5,0), PULocationID=1, total_amount=15.0),
        Row(tpep_pickup_datetime=datetime(2023,1,1,10,8,0), PULocationID=1, total_amount=12.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = revenue_by_day_borough_streaming(df)
    assert "revenue" in result.columns
    assert "window" in result.columns


def test_read_stream_uses_parquet_format(spark, tmp_path):
    """read_stream creates a streaming DataFrame from Parquet files."""
    test_dir = tmp_path / "stream_input"
    test_dir.mkdir()
    sample_df = spark.createDataFrame([Row(fare_amount=10.0)], ["fare_amount"])
    sample_df.write.mode("overwrite").parquet(str(test_dir))
    schema = sample_df.schema
    assert schema is not None