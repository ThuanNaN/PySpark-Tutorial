# Topic-to-Code Mapping

Every topic in the PySpark Tutorial has a corresponding code artifact in this project.

## Core Pipeline

| Module | Topics | Description |
|--------|--------|-------------|
| `src/taxi_analytics/utils/spark.py` | P4 | SparkSession creation |
| `src/taxi_analytics/pipeline/io.py` | P9, P10 | Read/Write data |
| `src/taxi_analytics/pipeline/clean.py` | P1 | Data cleaning |
| `src/taxi_analytics/pipeline/enrich.py` | P9 | Broadcast join |
| `src/taxi_analytics/pipeline/aggregate.py` | P8 | Aggregation + window |
| `src/taxi_analytics/streaming/io.py` | P16 | Streaming read/write |
| `src/taxi_analytics/streaming/aggregate.py` | P16 | Window + watermark |
| `src/taxi_analytics/ml/` | P17 | MLlib training |
| `src/taxi_analytics/jobs/` | P19 | Entry points |

## Per-Topic Lesson Scripts

All scripts in `src/lessons/` are runnable standalone examples demonstrating each topic's concept.

## Data Flow

```
scripts/crawl_nyc_taxi.py -> data/yellow/ + data/lookup/ + data/stream_backup/
                                    v
                    src/taxi_analytics/pipeline/ (batch)
                    src/taxi_analytics/streaming/ (streaming)
                    src/taxi_analytics/ml/ (machine learning)
                                    v
                    src/taxi_analytics/jobs/run_analytics.py
                    src/taxi_analytics/jobs/run_streaming.py
```
