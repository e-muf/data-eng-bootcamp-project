variable "project_id" {
  description = "Google Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
  type        = string
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

variable "instance_ip_address" { }

variable "instance_name" {
  description = "Database instance name"
}

variable "database_name" {
  type = string
}
