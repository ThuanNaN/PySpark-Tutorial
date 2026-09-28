#!/usr/bin/env python3
"""Crawl NYC TLC Yellow Taxi data for the PySpark Tutorial.

Downloads Parquet files for Jan–Mar 2023 and taxi_zone_lookup.csv.
Output goes to data/yellow/ and data/lookup/.
"""
import os
import sys
from pathlib import Path

import requests
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
YELLOW_DIR = DATA_DIR / "yellow"
LOOKUP_DIR = DATA_DIR / "lookup"

# NYC TLC Parquet download base URL (public S3)
PARQUET_BASE = (
    "https://d37ci6vzurdchw.cloudfront.net/mobile_taxi/"
    "yellow_tripdata_2023-{month:02d}-parquet"
)
MONTHS = [1, 2, 3]
ZONE_LOOKUP_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "taxi_zone_lookup.csv"
)


def download_file(url: str, dest: Path, desc: str) -> None:
    """Download a file with progress bar."""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, desc=desc, unit="B", unit_scale=True
    ) as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))


def download_parquet_files() -> None:
    """Download Yellow Taxi Parquet files for all months."""
    YELLOW_DIR.mkdir(parents=True, exist_ok=True)
    for month in MONTHS:
        url = PARQUET_BASE.format(month=month)
        dest = YELLOW_DIR / f"yellow_tripdata_2023-{month:02d}.parquet"
        if not dest.exists():
            download_file(url, dest, f"Downloading 2023-{month:02d}")
        else:
            print(f"Skipping existing: {dest.name}")


def download_zone_lookup() -> None:
    """Download taxi zone lookup CSV."""
    LOOKUP_DIR.mkdir(parents=True, exist_ok=True)
    dest = LOOKUP_DIR / "taxi_zone_lookup.csv"
    if not dest.exists():
        download_file(ZONE_LOOKUP_URL, dest, "Downloading zone lookup")
    else:
        print(f"Skipping existing: {dest.name}")


def prepare_stream_backup() -> None:
    """Split Jan Parquet into 50 small files for streaming simulation."""
    import shutil

    src = YELLOW_DIR / "yellow_tripdata_2023-01.parquet"
    backup_dir = DATA_DIR / "stream_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(backup_dir, ignore_errors=True)

    if src.exists():
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("prepare_stream")
            .master("local[4]")
            .getOrCreate()
        )
        df = spark.read.parquet(str(src))
        df.repartition(50).write.mode("overwrite").parquet(str(backup_dir))
        spark.stop()
        print(f"Prepared {backup_dir} with 50 files for streaming simulation")
    else:
        print("Source Parquet not found — run crawl first")


def main():
    download_parquet_files()
    download_zone_lookup()
    prepare_stream_backup()
    print("\nAll data downloaded!")
    print(f"  Parquet: {YELLOW_DIR}/")
    print(f"  Lookup:  {LOOKUP_DIR}/")
    print(f"  Stream:  {DATA_DIR / 'stream_backup'}/")


if __name__ == "__main__":
    main()
