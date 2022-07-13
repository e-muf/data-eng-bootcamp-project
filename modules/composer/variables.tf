variable "project_id" {
  description = "Google Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
  type        = string
}

variable "location" {
  description = "Google Cloud zone"
}

variable "project_bucket" {
  description = "Google Cloud Storage bucket"
  type        = string
}

variable "instance_name" {
  description = "Database instance name"
}

variable "database_name" {
  type = string
}
