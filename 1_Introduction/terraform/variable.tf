locals {
    data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
}

variable "region" {
    description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
    default = "us-central1"
    type = string
}

variable "storage_class" {
    description = "Storage class for GCS bucket. Choose as per your requirement: https://cloud.google.com/storage/docs/storage-classes"
    default = "STANDARD"
}

variable "BQ_DATASET" {
    description = "BigQuery Dataset that raw data (from GCS) will be written to"
    default = "trips_data_all"
    type = string
}