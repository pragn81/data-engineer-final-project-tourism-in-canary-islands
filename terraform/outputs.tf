output "raw_bucket_name" {
  description = "GCS bucket for raw CSV files"
  value       = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  description = "GCS bucket for PySpark Parquet output"
  value       = google_storage_bucket.processed.name
}

output "bq_dataset_raw" {
  description = "BigQuery dataset for raw tables"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  description = "BigQuery dataset for dbt staging models"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_mart" {
  description = "BigQuery dataset for dbt mart models"
  value       = google_bigquery_dataset.mart.dataset_id
}

output "service_account_email" {
  description = "Service account used by the pipeline"
  value       = google_service_account.pipeline.email
}
