#!/usr/bin/env python3
"""Batch analytics pipeline entry point (Topic P19).

Usage:
    python -m taxi_analytics.jobs.run_analytics \
        data/yellow/yellow_tripdata_2023-01.parquet \
        data/lookup/taxi_zone_lookup.csv \
        out/analytics
"""
import sys
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from taxi_analytics.pipeline import io, clean, enrich, aggregate


def main(trips_path: str, zones_path: str, output_path: str):
    """Run the full batch analytics pipeline."""
    conf = load_spark_conf()
    spark = create_spark(conf)

    trips = io.read_trips(spark, trips_path)       # Read Parquet (schema auto)
    zones = io.read_zones(spark, zones_path)       # Read CSV lookup (schema explicit)
    cleaned = clean.clean_trips(trips)              # Filter invalid records
    enriched = enrich.add_zone_names(spark, cleaned, zones)  # Broadcast join zones
    result = aggregate.revenue_by_day_borough(enriched)     # Aggregate revenue
    io.write_parquet(result, output_path)            # Write partitioned Parquet

    spark.stop()


if __name__ == "__main__":
    trips_path = sys.argv[1] if len(sys.argv) > 1 else "data/yellow/yellow_tripdata_2023-01.parquet"
    zones_path = sys.argv[2] if len(sys.argv) > 2 else "data/lookup/taxi_zone_lookup.csv"
    output_path = sys.argv[3] if len(sys.argv) > 3 else "out/analytics"
    main(trips_path, zones_path, output_path)
