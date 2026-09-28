from pyspark.sql import SparkSession
from taxi_analytics.utils.spark import load_spark_conf, create_spark


def test_load_spark_conf():
    conf = load_spark_conf("conf/spark.conf")
    assert conf["appName"] == "taxi-analytics"
    assert conf["master"] == "local[4]"
    assert conf["spark.sql.adaptive.enabled"] == "true"


def test_create_spark(spark):
    """Reuse the pytest fixture's SparkSession for integration."""
    conf = load_spark_conf("conf/spark.conf")
    session = create_spark(conf)
    assert session.sparkContext.appName == "taxi-analytics"
