import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from taxi_analytics.pipeline.io import read_trips, read_zones, write_parquet, ZONE_SCHEMA
from taxi_analytics.pipeline.clean import clean_trips
from taxi_analytics.pipeline.enrich import add_zone_names
from taxi_analytics.pipeline.aggregate import revenue_by_day_borough


@pytest.fixture
def sample_trips(spark):
    """Create a mini fixture mimicking NYC taxi data."""
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
        StructField("tip_pct", DoubleType(), True),
    ])
    rows = [
        Row(fare_amount=12.5, trip_distance=3.2, passenger_count=1, total_amount=18.0, tip_amount=2.5, PULocationID=1, DOLocationID=2, tpep_pickup_datetime=datetime(2023,1,1,10,0,0), tpep_dropoff_datetime=datetime(2023,1,1,10,15,0), tip_pct=20.0),
        Row(fare_amount=-5.0, trip_distance=0.0, passenger_count=1, total_amount=10.0, tip_amount=1.0, PULocationID=1, DOLocationID=2, tpep_pickup_datetime=datetime(2023,1,1,11,0,0), tpep_dropoff_datetime=datetime(2023,1,1,11,15,0), tip_pct=10.0),
        Row(fare_amount=15.0, trip_distance=2.0, passenger_count=None, total_amount=20.0, tip_amount=3.0, PULocationID=1, DOLocationID=2, tpep_pickup_datetime=datetime(2023,1,1,12,0,0), tpep_dropoff_datetime=datetime(2023,1,1,12,15,0), tip_pct=15.0),
    ]
    return spark.createDataFrame(rows, schema)


def test_clean_filters_invalid(sample_trips):
    """Invalid records (negative fare, 0 distance, null passenger) are removed."""
    cleaned = clean_trips(sample_trips)
    assert cleaned.count() == 1


def test_clean_adds_columns(sample_trips):
    """Cleaned data has pickup_day and tip_pct columns."""
    cleaned = clean_trips(sample_trips)
    assert "pickup_day" in cleaned.columns
    assert "tip_pct" in cleaned.columns


def test_read_zones_schema(spark):
    """Zone lookup schema matches expected."""
    assert ZONE_SCHEMA == StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ])


def test_revenue_aggregation(spark):
    """Aggregation produces expected columns."""
    schema = StructType([
        StructField("pickup_day", DateType(), True),
        StructField("pu_borough", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("tip_pct", DoubleType(), True),
    ])
    df = spark.createDataFrame(
        [Row(pickup_day=date(2023,1,1), pu_borough="Manhattan", total_amount=18.0, trip_distance=3.2, tip_pct=20.0)],
        schema
    )
    result = revenue_by_day_borough(df)
    assert "revenue" in result.columns
    assert "avg_distance" in result.columns
    assert "avg_tip_pct" in result.columns