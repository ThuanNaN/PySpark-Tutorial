import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from taxi_analytics.pipeline.io import read_trips, read_zones, write_parquet, ZONE_SCHEMA
from taxi_analytics.pipeline.clean import clean_trips
from taxi_analytics.pipeline.enrich import add_zone_names
from taxi_analytics.pipeline.aggregate import revenue_by_day_borough


@pytest.fixture
def sample_trips(spark):
    """Create a mini fixture mimicking NYC taxi data 'vibes'."""
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
        Row(fare_amount=12.5, trip_distance=3.2, passenger_count=1, total_amount=18.0, tip_amount=2.5, PULocationID=1, DOLocationID=2, tpep_pickup_datetime="2023-01-01 10:00:00", tpep_dropoff_datetime="2023-01-01 10:15:00", tip_pct=20.0),
        Row(fare_amount=-5.0, trip_distance=0.0, passenger_count=1, total_amount=10.0, tip_amount=1.0, PULocationID=1, DOLocationID=2, tpep_pickup_datetime="2023-01-01 11:00:00", tpep_dropoff_datetime="2023-01-01 11:15:00", tip_pct=10.0),
        Row(fare_amount=15.0, trip_distance=2.0, passenger_count=None, total_amount=20.0, tip_amount=3.0, PULocationID=1, DOLocationID=2, tpep_pickup_datetime="2023-01-01 12:00:00", tpep_dropoff_datetime="2023-01-01 12:15:00", tip_pct=15.0),
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
    """Zone lookup CSV reads with correct schema."""
    zones = read_zones(spark, "data/lookup/taxi_zone_lookup.csv")
    assert zones.schema == ZONE_SCHEMA


def test_revenue_aggregation(spark):
    """Aggregation produces expected columns."""
    schema = StructType([
        StructField("pickup_day", DateType(), True),
        StructField("pu_borough", StringType(), True),
        StructField("trips", IntegerType(), True),
    ])
    df = spark.createDataFrame(
        [Row(pickup_day="2023-01-01", pu_borough="Manhattan", trips=100)],
        schema
    )
    result = revenue_by_day_borough(df)
    assert "revenue" in result.columns
    assert "avg_distance" in result.columns
    assert "avg_tip_pct" in result.columns
