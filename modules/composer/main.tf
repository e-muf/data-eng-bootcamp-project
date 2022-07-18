resource "google_composer_environment" "composer_cluster" {
  name   = "data-eng-bootcamp-composer-env"
  region = var.region

  config {
    software_config {
      image_version  = "composer-2.0.19-airflow-2.2.5"
      airflow_config_overrides = {
        secrets-backend = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
        secrets-backend_kwargs = jsonencode({"project_id": var.project_id})
      }

      env_variables = {
        "GCS_SOURCE_DATA_BUCKET" = var.project_bucket
        "GCSQL_POSTGRES_DATABASE_NAME" = var.database_name
        "GCSQL_POSTGRES_INSTANCE_NAME_QUERY" = var.instance_name
        "BQ_RAW_DATASET" = var.raw_movies_dataset
        "BQ_DWH_DATASET" = var.dwh_movies_dataset
      }
    }
  }
}
