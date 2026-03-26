"""
Canary Islands Tourism - Data Engineering Pipeline
Entrypoint for local execution of pipeline steps.

Usage:
    uv run main.py ingest
"""

import sys


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "help"

    if command == "ingest":
        from ingestion.download import run
        run()
    elif command == "upload":
        from ingestion.upload_gcs import run
        run()
    elif command == "transform":
        from processing.spark_transform import run
        run()
    elif command == "load":
        from ingestion.load_bigquery import run
        run()
    else:
        print("Available commands:")
        print("  ingest     - Download raw CSV files from ISTAC API")
        print("  upload     - Upload raw CSVs to GCS raw bucket")
        print("  transform  - Run PySpark transformations → GCS processed bucket")
        print("  load       - Load Parquet from GCS processed → BigQuery raw tables")


if __name__ == "__main__":
    main()

