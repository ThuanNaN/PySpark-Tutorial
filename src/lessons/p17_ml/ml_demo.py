#!/usr/bin/env python3
"""Topic P17: Machine Learning

Demonstrates training a LinearRegression and RandomForest model on taxi data.

Run: python src/lessons/p17_ml/ml_demo.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

conf = load_spark_conf()
spark = create_spark(conf)

df = spark.read.parquet("data/yellow/yellow_tripdata_2023-01.parquet")

# Prepare features: select relevant columns and assemble into feature vector
df_clean = df.select(
    "trip_distance", "PULocationID", "DOLocationID", "total_amount"
).filter(df.trip_distance > 0).filter(df.total_amount > 0).limit(5000)

# Assemble features into a single vector column
assembler = VectorAssembler(
    inputCols=["trip_distance", "PULocationID", "DOLocationID"],
    outputCol="features"
)
feature_df = assembler.transform(df_clean)

# Split into training and test sets
train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)
print(f"Training set: {train_df.count()} rows")
print(f"Test set: {test_df.count()} rows")

# Train LinearRegression model
lr = LinearRegression(featuresCol="features", labelCol="total_amount", maxIter=10)
lr_model = lr.fit(train_df)
lr_predictions = lr_model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="total_amount", predictionCol="prediction", metricName="rmse")
lr_rmse = evaluator.evaluate(lr_predictions)
print(f"\nLinearRegression RMSE: {lr_rmse:.2f}")
print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")

# Train RandomForestRegressor model
rf = RandomForestRegressor(featuresCol="features", labelCol="total_amount", numTrees=10)
rf_model = rf.fit(train_df)
rf_predictions = rf_model.transform(test_df)
rf_rmse = evaluator.evaluate(rf_predictions)
print(f"\nRandomForest RMSE: {rf_rmse:.2f}")
print(f"Feature importances: {rf_model.featureImportances}")

print("\nML summary:")
print("- LinearRegression: simple, interpretable, fast to train")
print("- RandomForest: ensemble method, more accurate, handles non-linear relationships")
spark.stop()
