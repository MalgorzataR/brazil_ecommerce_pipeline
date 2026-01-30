variable "credentials" {
  description = "Path to Google Cloud credentials JSON file"
  type        = string
}

variable "project" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for resources"
  type        = string
}

variable "location" {
  description = "GCP Location for storage and BigQuery"
  type        = string

}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name for data storage"
  type        = string

}

variable "gcs_storage_class" {
  description = "Storage class for GCS bucket"
  type        = string

}

variable "bq_dataset_name" {
  description = "BigQuery dataset name for data warehouse"
  type        = string
}
