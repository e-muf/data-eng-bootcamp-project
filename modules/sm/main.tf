resource "google_secret_manager_secret" "secret-backend" {
  secret_id = "airflow-connections-public_postgres_tcp"

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "sql-connection" {
  secret      = google_secret_manager_secret.secret-backend.id
  secret_data = "gcpcloudsql://${var.db_username}:${var.db_password}@${var.instance_ip_address}:5432/${var.database_name}?database_type=postgres&project_id=${var.project_id}&location=${var.region}&instance=${var.instance_name}&use_proxy=False&use_ssl=False"
}


