"""
Load Parquet files from GCS processed bucket into BigQuery raw tables.
Tables are partitioned by period_date (DATE) and clustered by relevant dimensions.
"""

import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

# Maps table name -> (gcs_prefix, partition_field, cluster_fields)
TABLE_CONFIG = {
    "tourist_accommodations": (
        "processed/tourist_accommodations/",
        "period_date",
        ["territorio_code", "tipo_alojamiento_code", "pais_residencia_code"],
    ),
    "tourist_age_sex": (
        "processed/tourist_age_sex/",
        "period_date",
        ["territorio_code", "pais_residencia_code", "sexo_code", "edad_code"],
    ),
    "tourist_revenue": (
        "processed/tourist_revenue/",
        "period_date",
        ["territorio_code", "pais_residencia_code", "nivel_ingresos_code"],
    ),
}


def get_bq_client(credentials_file: str) -> bigquery.Client:
    return bigquery.Client.from_service_account_json(credentials_file)


def get_gcs_client(credentials_file: str) -> storage.Client:
    return storage.Client.from_service_account_json(credentials_file)


def list_parquet_uris(gcs_client: storage.Client, bucket_name: str, prefix: str) -> list[str]:
    """List all parquet files under a GCS prefix and return their gs:// URIs."""
    blobs = gcs_client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]


def load_table(
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    gcs_uris: list[str],
    partition_field: str,
    cluster_fields: list[str],
) -> None:
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    print(f"Loading {len(gcs_uris)} file(s) -> {table_ref} ...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=partition_field,
        ),
        clustering_fields=cluster_fields,
    )

    load_job = bq_client.load_table_from_uri(gcs_uris, table_ref, job_config=job_config)
    load_job.result()

    table = bq_client.get_table(table_ref)
    print(f"  -> {table.num_rows:,} rows | partitioned by {partition_field} (MONTH) | clustered by {cluster_fields}")


def run() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    credentials_file = os.environ["GCP_CREDENTIALS_FILE"]
    processed_bucket = os.environ["GCS_PROCESSED_BUCKET"]
    dataset_id = os.environ["BQ_DATASET_RAW"]

    bq_client = get_bq_client(credentials_file)
    gcs_client = get_gcs_client(credentials_file)

    for table_name, (gcs_prefix, partition_field, cluster_fields) in TABLE_CONFIG.items():
        uris = list_parquet_uris(gcs_client, processed_bucket, gcs_prefix)
        if not uris:
            raise FileNotFoundError(f"No parquet files found in gs://{processed_bucket}/{gcs_prefix}")
        load_table(bq_client, project_id, dataset_id, table_name, uris, partition_field, cluster_fields)

    print("BigQuery load complete.")


if __name__ == "__main__":
    run()
