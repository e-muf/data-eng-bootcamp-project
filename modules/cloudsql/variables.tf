variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
  type        = string
}

variable "location" {
  description = "Google Cloud zone"
}

variable "instance_name" {
  description = "Database instance name"
}

variable "instance_tier" {
  description = "SQL Instance tier"
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