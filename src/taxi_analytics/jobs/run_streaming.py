#!/usr/bin/env python3
"""Streaming analytics pipeline entry point (Topic P16).

Usage:
    python -m taxi_analytics.jobs.run_streaming \
        data/stream_input/ \
        out/stream_revenue/

Then in another terminal, 'thrill' files into the input directory:
    cp data/stream_backup/part-00001.parquet data/stream_input/
"""
import sys
from taxi_analytics.utils.spark import create_spark, load_spark_conf
from taxi_analytics.streaming import io as stream_io
from taxi_analytics.streaming import clean as stream_clean
from taxi_analytics.streaming import enrich as stream_enrich
from taxi_analytics.streaming import aggregate as stream_agg


def main(stream_input: str, output_path: str):
    """Run the full streaming analytics pipeline."""
    conf = load_spark_conf()
    spark = create_spark(conf)

    stream_df = stream_io.read_stream(spark)  # Read from stream_input/
    cleaned = stream_clean.clean_trips(stream_df)
    zones = stream_io.read_zones(spark, "data/lookup/taxi_zone_lookup.csv")
    enriched = stream_enrich.add_zone_names(spark, cleaned, zones)
    result = stream_agg.revenue_by_day_borough_streaming(enriched)

    query = stream_io.write_stream(result, output_path)
    print(f"Streaming query started. Throwing files into {stream_input}...")
    query.awaitTermination()


if __name__ == "__main__":
    stream_input = sys.argv[1] if len(sys.argv) > 1 else "data/stream_input/"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "out/stream_revenue/"
    main(stream_input, output_path)
