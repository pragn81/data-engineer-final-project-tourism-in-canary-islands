"""
Upload raw CSV files and processed Parquet files to GCS.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

CSV_FILES = [
    "tourist_accommodations.csv",
    "tourist_age_sex.csv",
    "tourist_revenue.csv",
]

PROCESSED_DATASETS = [
    "tourist_accommodations",
    "tourist_age_sex",
    "tourist_revenue",
]


def get_client() -> storage.Client:
    credentials_file = os.environ["GCP_CREDENTIALS_FILE"]
    return storage.Client.from_service_account_json(credentials_file)


def upload_file(client: storage.Client, bucket_name: str, local_path: Path, gcs_path: str) -> None:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    print(f"  -> gs://{bucket_name}/{gcs_path}")


def upload_raw_csvs(client: storage.Client, raw_bucket: str) -> None:
    print(f"Uploading CSVs to gs://{raw_bucket}/raw/")
    for filename in CSV_FILES:
        local_path = RAW_DIR / filename
        if not local_path.exists():
            raise FileNotFoundError(f"File not found: {local_path}. Run 'ingest' first.")
        upload_file(client, raw_bucket, local_path, f"raw/{filename}")


def upload_processed_parquet(client: storage.Client, processed_bucket: str) -> None:
    print(f"Uploading Parquet to gs://{processed_bucket}/processed/")
    for dataset in PROCESSED_DATASETS:
        dataset_dir = PROCESSED_DIR / dataset
        if not dataset_dir.exists():
            raise FileNotFoundError(f"Directory not found: {dataset_dir}. Run 'transform' first.")
        for parquet_file in dataset_dir.rglob("*.parquet"):
            gcs_path = f"processed/{dataset}/{parquet_file.relative_to(dataset_dir)}"
            upload_file(client, processed_bucket, parquet_file, gcs_path)


def run() -> None:
    raw_bucket = os.environ["GCS_RAW_BUCKET"]
    processed_bucket = os.environ["GCS_PROCESSED_BUCKET"]
    client = get_client()

    upload_raw_csvs(client, raw_bucket)
    upload_processed_parquet(client, processed_bucket)
    print("Upload complete.")


if __name__ == "__main__":
    run()
