# PySpark Tutorial — NYC Taxi Analytics

A complete PySpark project accompanying the VLAI PySpark Tutorial (19 topics).

## Quick Start

1. Download data: `python scripts/crawl_nyc_taxi.py`
2. Run batch pipeline: `python -m taxi_analytics.jobs.run_analytics`
3. Run streaming pipeline: `python -m taxi_analytics.jobs.run_streaming`
4. Run tests: `pytest -q`

## Project Structure

- `scripts/crawl_nyc_taxi.py` — Download NYC Taxi data
- `src/taxi_analytics/` — Core package
  - `utils/spark.py` — SparkSession creation
  - `pipeline/` — Batch pipeline (io, clean, enrich, aggregate)
  - `streaming/` — Streaming pipeline (same logic, different I/O)
  - `jobs/run_analytics.py` — Batch entry point
  - `jobs/run_streaming.py` — Streaming entry point
  - `ml/` — MLlib models
- `src/lessons/` — Per-topic example scripts
- `tests/` — pytest suite

## Data

Dataset: NYC TLC Yellow Taxi, Jan–Mar 2023 (~140 MB Parquet)
