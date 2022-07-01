output "airflow_dag_gcs_buket" {
  value = substr(google_composer_environment.composer_cluster.config.0.dag_gcs_prefix, 5, 43)
}
