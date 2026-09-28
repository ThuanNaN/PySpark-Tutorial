"""Integration tests for the full pipeline."""
import pytest
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, DateType
from taxi_analytics.pipeline import io, clean, enrich, aggregate


@pytest.fixture
def sample_data(spark):
    """Create a small test DataFrame mimicking taxi data."""
    schema = StructType([
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("pu_borough", StringType(), True),
        StructField("pickup_day", DateType(), True),
        StructField("tip_pct", DoubleType(), True),
    ])
    rows = [
        Row(fare_amount=12.5, trip_distance=3.2, passenger_count=1, total_amount=18.0,
            tip_amount=2.5, PULocationID=1, DOLocationID=2,
            tpep_pickup_datetime=datetime(2023,1,1,10,0,0), tpep_dropoff_datetime=datetime(2023,1,1,10,15,0),
            pu_borough="Manhattan", pickup_day=date(2023,1,1), tip_pct=20.0),
        Row(fare_amount=25.0, trip_distance=10.5, passenger_count=2, total_amount=35.0,
            tip_amount=5.0, PULocationID=2, DOLocationID=3,
            tpep_pickup_datetime=datetime(2023,1,1,14,0,0), tpep_dropoff_datetime=datetime(2023,1,1,14,30,0),
            pu_borough="Brooklyn", pickup_day=date(2023,1,1), tip_pct=14.3),
    ]
    return spark.createDataFrame(rows, schema)


def test_end_to_end_batch(spark, sample_data):
    """Full batch pipeline produces aggregated output."""
    cleaned = clean.clean_trips(sample_data)
    zones = spark.createDataFrame(
        [{"LocationID": 1, "Borough": "Manhattan", "Zone": "Hells Kitchen", "service_zone": "MAN"}],
        schema=io.ZONE_SCHEMA
    )
    enriched = enrich.add_zone_names(spark, cleaned, zones)
    result = aggregate.revenue_by_day_borough(enriched)
    assert result.count() > 0
    assert "revenue" in result.columns


def test_batch_and_streaming_share_logic(sample_data):
    """Batch clean produces same result as streaming clean."""
    from taxi_analytics.pipeline.clean import clean_trips as batch_clean
    from taxi_analytics.streaming.clean import clean_trips as stream_clean
    batch_result = batch_clean(sample_data)
    stream_result = stream_clean(sample_data)
    assert batch_result.columns == stream_result.columns
    assert batch_result.count() == stream_result.count()