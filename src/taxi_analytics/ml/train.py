from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import RandomForestClassifier


def train_regression(df: DataFrame, label_col: str = "total_amount"):
    """Train a LinearRegression model to predict trip cost."""
    lr = LinearRegression(
        featuresCol="features",
        labelCol=label_col,
        maxIter=10,
        regParam=0.3,
        elasticNetParam=0.8,
    )
    return lr.fit(df)


def train_classification(df: DataFrame, label_col: str = "tip_pct"):
    """Train a RandomForest classifier to predict tip category."""
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=label_col,
        numTrees=20,
    )
    return rf.fit(df)
