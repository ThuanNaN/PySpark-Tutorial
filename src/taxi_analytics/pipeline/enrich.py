from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def add_zone_names(spark: SparkSession, df: DataFrame, zones: DataFrame) -> DataFrame:
    pu_zones = zones.selectExpr("LocationID as pu_id", "Zone as pu_zone", "Borough as pu_borough")
    return (
        df
        .join(F.broadcast(pu_zones), df.PULocationID == F.col("pu_id"), "left")
        .drop("pu_id")
    )
