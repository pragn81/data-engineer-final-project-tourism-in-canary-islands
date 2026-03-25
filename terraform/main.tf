terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  credentials = file(var.credentials_file)
}

# ── GCS Buckets ──────────────────────────────────────────────────────────────

resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-tourism-raw"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "processed" {
  name          = "${var.project_id}-tourism-processed"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# ── BigQuery Datasets ─────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "tourism_raw"
  description = "Raw data loaded from GCS"
  location    = var.region
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "tourism_staging"
  description = "dbt staging models"
  location    = var.region
}

resource "google_bigquery_dataset" "mart" {
  dataset_id  = "tourism_mart"
  description = "dbt mart models for dashboards"
  location    = var.region
}

# ── Service Account ───────────────────────────────────────────────────────────

resource "google_service_account" "pipeline" {
  account_id   = "tourism-pipeline"
  display_name = "Tourism Pipeline Service Account"
}

resource "google_project_iam_member" "pipeline_gcs" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
}

resource "local_file" "pipeline_key_file" {
  content         = base64decode(google_service_account_key.pipeline_key.private_key)
  filename        = "${path.module}/../credentials/pipeline_sa.json"
  file_permission = "0600"
}
