from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, round as f_round

def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("fare_amount") > 0)
        .filter(col("trip_distance") > 0)
        .filter(col("passenger_count").isNotNull())
        .withColumn("pickup_day", when(col("tpep_pickup_datetime").isNotNull(), col("tpep_pickup_datetime").cast("date")))
        .withColumn("tip_pct", f_round(col("tip_amount") / col("fare_amount") * 100, 2))
    )
