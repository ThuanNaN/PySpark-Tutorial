from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def revenue_by_day_borough_streaming(df: DataFrame) -> DataFrame:
    """Streaming aggregation with watermark for late data (Topic P16).

    Uses withWatermark to limit state growth, then window() to aggregate
    by 15-minute tumbling windows. Output mode: 'update'.
    """
    events = df.withWatermark("tpep_pickup_datetime", "10 minutes")
    return (
        events.groupBy(
            F.window("tpep_pickup_datetime", "15 minutes"),
            "PULocationID"
        )
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_amount").alias("revenue"),
        )
    )
