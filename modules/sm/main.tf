resource "google_secret_manager_secret" "secret-backend" {
  secret_id = "airflow-connections-public_postgres_tcp"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "sql-connection" {
  secret      = google_secret_manager_secret.secret-backend.id
  secret_data = "gcpcloudsql://${var.db_username}:${var.db_password}@${var.instance_ip_address}:5432/${var.database_name}?database_type=postgres&project_id=${var.project_id}&location=${var.region}&instance=${var.instance_name}&use_proxy=False&use_ssl=False"
}

resource "google_secret_manager_secret" "secret-postgres-connection" {
  secret_id = "postgres_conn_id"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "postgres-connection" {
  secret      = google_secret_manager_secret.secret-postgres-connection.id
  secret_data = "jdbc:postgresql://${var.instance_ip_address}:5432/${var.database_name}"
}

resource "google_secret_manager_secret" "secret-postgres-user" {
  secret_id = "pg_user"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "postgres-user" {
  secret      = google_secret_manager_secret.secret-postgres-user.id
  secret_data = var.db_username
}

resource "google_secret_manager_secret" "secret-postgres-password" {
  secret_id = "pg_password"
  replication { automatic = true }
}

resource "google_secret_manager_secret_version" "postgres-password" {
  secret      = google_secret_manager_secret.secret-postgres-password.id
  secret_data = var.db_password
}
