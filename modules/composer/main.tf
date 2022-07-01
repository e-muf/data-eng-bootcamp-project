resource "google_composer_environment" "composer_cluster" {
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
      image_version  = "composer-1.19.1-airflow-2.2.5"
      python_version = 3

      env_variables = {
        "GCS_PROJECT_BUCKET"                 = var.project_bucket,
        "GCSQL_POSTGRES_USER"                = var.db_username
        "GCSQL_POSTGRES_PASSWORD"            = var.db_password
        "GCSQL_POSTGRES_PUBLIC_IP"           = var.instance_ip_address
        "GCP_REGION"                         = var.region
        "GCSQL_POSTGRES_INSTANCE_NAME_QUERY" = var.instance_name
        "GCSQL_POSTGRES_DATABASE_NAME"       = var.database_name
      }
    }
  }
}
