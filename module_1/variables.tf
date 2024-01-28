variable "credentials" {
  description = "My GCP Credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "My Project"
  default     = "upbeat-bolt-412612"
}

variable "region" {
  description = "Project Region"
  default     = "europe-west2"
}

variable "location" {
  description = "Project Location"
  default     = "EUROPE-WEST2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "upbeat-bolt-412612-ny-taxi-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}