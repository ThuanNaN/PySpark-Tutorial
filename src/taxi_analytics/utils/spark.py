from pyspark.sql import SparkSession
import configparser


def load_spark_conf(path="conf/spark.conf"):
    """Load Spark configuration from INI file."""
    conf = configparser.ConfigParser()
    conf.read(path)
    return conf["spark"]


def create_spark(conf=None):
    """Create a SparkSession from a config parser object."""
    builder = SparkSession.builder.appName(
        conf.get("appName", "taxi-analytics")
    )
    if conf.get("master"):
        builder.master(conf["master"])
    for k, v in conf.items():
        if k.startswith("spark."):
            builder.config(k, v)
    return builder.getOrCreate()
