terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.7.0"
    }
  }
}

module "gcs" {
  source         = "./modules/gcs"
  project_id     = var.project_id
  project_bucket = "${var.project_id}-bucket"
  data_path      = var.data_path
}

module "cloudsql" {
  source           = "./modules/cloudsql"
  region           = var.region
  location         = var.location
  instance_name    = var.instance_name
  instance_tier    = var.instance_tier
  database_version = var.database_version
  database_name    = var.database_name
  disk_space       = var.disk_space
  db_username      = var.db_username
  db_password      = var.db_password
}
