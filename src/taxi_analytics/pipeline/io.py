from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, to_date

ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), False),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

def read_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def read_zones(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=ZONE_SCHEMA)

def write_parquet(df: DataFrame, path: str) -> None:
    df.coalesce(4).write.mode("overwrite").partitionBy("pickup_day").parquet(path)
