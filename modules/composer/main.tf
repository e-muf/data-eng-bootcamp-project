resource "google_composer_environment" "composer-cluster" {
  name   = "data-eng-bootcamp-composer-env"
  region = var.region

  config {
    node_count = 3
    node_config {
      zone         = var.location
      machine_type = "n1-standard-1"
      disk_size_gb = 30
    }

    software_config {
      image_version = "composer-1.19.1-airflow-2.2.5"
      python_version = 3
    }
  }
}
