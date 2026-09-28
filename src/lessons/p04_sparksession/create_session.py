#!/usr/bin/env python3
"""Topic P4: SparkSession

Demonstrates creating a SparkSession from conf/spark.conf
using load_spark_conf and create_spark.

Run: python src/lessons/p04_sparksession/create_session.py
"""
from taxi_analytics.utils.spark import create_spark, load_spark_conf

conf = load_spark_conf()
spark = create_spark(conf)
print(f"SparkSession created: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
spark.stop()
