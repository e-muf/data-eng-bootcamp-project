terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.7.0"
    }
  }
}

module "gcs" {
  source          = "./modules/gcs"

  project_id      = var.project_id
  project_bucket  = "${var.project_id}-bucket"
  data_path       = var.data_path
}