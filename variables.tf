variable "project_id" { 
  description = "Google Project ID"
  type        = string 
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
  type        = string 
}

variable "credentials_file" {
  description = "Google Cloud credentials"
  type        = string
  sensitive   = true
}

variable "data_path" {
  description = "Path to CSV files"
  type        = string
}