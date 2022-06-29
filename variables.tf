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

variable "credentials_file" {
  description = "Google Cloud credentials"
  type        = string
  sensitive   = true
}

# Google Cloud Storage
variable "data_path" {
  description = "Path to CSV files"
  type        = string
}

# Cloud SQL
variable "instance_name" {
  description = "Database instance name"
  default     = "pg-data-bootcam7"
}

variable "instance_tier" {
  description = "SQL Instance tier"
  default     = "db-g1-small"
}

variable "database_version" {
  description = "PostgreSQL version"
  default     = "POSTGRES_14"
}

variable "database_name" {
  description = "Database project name"
  default    = "moviesdb"
}

variable "disk_space" {
  description = "Storage capacity in GB"
  default     = 10
}

variable "db_username" {
  description = "Database administrator username"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Database administrator password"
  type        = string
  sensitive   = true
}
