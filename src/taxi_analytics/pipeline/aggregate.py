from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def revenue_by_day_borough(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_day", "pu_borough")
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_amount").alias("revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("tip_pct").alias("avg_tip_pct"),
        )
    )
