resource "google_sql_database_instance" "sql_instance" {
  name             = var.instance_name
  database_version = var.database_version
  region           = var.region

  settings {
    tier      = var.instance_tier
    disk_size = var.disk_space
    disk_type = "PD_HDD"

    location_preference {
      zone = var.location
    }

    ip_configuration {
      authorized_networks {
        value = "0.0.0.0/0"
      }
    }
  }

  deletion_protection = false
}

resource "google_sql_database" "database" {
  name     = var.database_name
  instance = google_sql_database_instance.sql_instance.name
}

resource "google_sql_user" "admin_user" {
  name     = var.db_username
  password = var.db_password
  instance = google_sql_database_instance.sql_instance.name
}
