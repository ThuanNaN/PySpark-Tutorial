from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer


def prepare_features(df: DataFrame) -> DataFrame:
    """Prepare features for ML models from cleaned taxi data.

    Creates:
    - numeric_features: fare_amount, trip_distance, passenger_count, tip_pct
    - categorical_indexed: pu_borough (StringIndexer)
    - assembled: feature vector for MLlib
    """
    # Index categorical column
    borough_indexer = StringIndexer(
        inputCol="pu_borough", outputCol="borough_index"
    )
    indexed = borough_indexer.fit(df).transform(df)

    # Assemble feature vector
    assembler = VectorAssembler(
        inputCols=["fare_amount", "trip_distance", "passenger_count", "borough_index"],
        outputCol="features"
    )
    return assembler.transform(indexed)
