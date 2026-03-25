variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "europe-west1"
}

variable "credentials_file" {
  description = "Path to a GCP service account JSON key with permissions to create resources"
  type        = string
  default     = "../credentials/service_account.json"
}
