import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from taxi_analytics.ml.feature_utils import prepare_features
from taxi_analytics.ml.train import train_regression, train_classification


def test_prepare_features(spark):
    """Feature engineering produces a 'features' vector column."""
    schema = StructType([
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("pu_borough", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    rows = [Row(fare_amount=12.5, trip_distance=3.2, passenger_count=1, pu_borough="Manhattan", total_amount=18.0)]
    df = spark.createDataFrame(rows, schema)
    result = prepare_features(df)
    assert "features" in result.columns
    assert "borough_index" in result.columns


def test_train_regression(spark):
    """LinearRegression model trains and produces predictions."""
    from pyspark.ml.linalg import Vectors, VectorUDT
    schema = StructType([
        StructField("features", VectorUDT(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    rows = [Row(features=Vectors.dense([1.0, 2.0, 3.0]), total_amount=15.0)]
    df = spark.createDataFrame(rows, schema)
    model = train_regression(df)
    predictions = model.transform(df)
    assert "prediction" in predictions.columns
