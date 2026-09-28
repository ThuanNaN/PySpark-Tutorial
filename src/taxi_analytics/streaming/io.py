from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from taxi_analytics.pipeline.io import ZONE_SCHEMA


def read_stream(spark: SparkSession, schema: StructType = None) -> DataFrame:
    """Create a streaming DataFrame reading from stream_input/ directory.

    Uses Parquet format — files 'dropped' into stream_input/ are processed
    as micro-batches (Topic P16, Section 0).
    """
    if schema is None:
        schema = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet").schema
    return (
        spark.readStream
        .format("parquet")
        .schema(schema)
        .load("data/stream_input/")
    )


def write_stream(df: DataFrame, query_name: str) -> None:
    """Write streaming DataFrame to Parquet with checkpoint.

    Usage:
        query = write_stream(result, "stream_revenue")
        query.awaitTermination()
    """
    return (
        df.writeStream
        .format("parquet")
        .option("path", f"out/{query_name}/")
        .option("checkpointLocation", f"chk/{query_name}/")
        .outputMode("append")
        .start()
    )
