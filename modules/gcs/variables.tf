variable "project_id" { 
  description = "Google Project ID"
  type        = string 
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
  type        = string 
}

variable "project_bucket" {
  description = "Google Cloud Storage bucket"
  type        = string
}

variable "data_path" {
  description = "Path to CSV files"
  type        = string
}
