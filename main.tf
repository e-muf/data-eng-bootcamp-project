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
  gcs_dag_bucket = module.composer.airflow_dag_gcs_buket
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

module "sm" {
  source              = "./modules/sm"
  project_id          = var.project_id
  region              = var.region
  db_username         = var.db_username
  db_password         = var.db_password
  instance_ip_address = module.cloudsql.instance_ip_address
  instance_name       = module.cloudsql.instance_name
  database_name       = var.database_name
}

module "composer" {
  source         = "./modules/composer"
  project_id     = var.project_id
  region         = var.region
  location       = var.location 
  project_bucket = "${var.project_id}-bucket"
  instance_name  = module.cloudsql.instance_name
  database_name  = var.database_name
}
