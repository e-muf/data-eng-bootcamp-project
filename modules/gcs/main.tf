resource "google_storage_bucket" "project_bucket" {
  name     = var.project_bucket
  location = var.region
}

resource "google_storage_bucket_object" "initial_data" {
  for_each = fileset(var.data_path, "*")

  bucket       = google_storage_bucket.project_bucket.name
  name         = "data/${each.value}"
  source       = "${var.data_path}/${each.value}"
  content_type = "application/csv"
}

resource "google_storage_default_object_access_control" "public_rule" {
  bucket = google_storage_bucket.project_bucket.name
  role   = "READER"
  entity = "allUsers"
}

resource "google_storage_bucket_object" "upload_dags" {
  for_each     = fileset(var.dags_path, "*")

  bucket       = var.gcs_dag_bucket
  name         = "dags/${each.value}"
  source       = "code/dags/${each.value}"
  content_type = "text/x-python"
}

resource "google_storage_bucket_object" "upload_spark_job" {
  name         = "code/transform_reviews.py"
  bucket       = google_storage_bucket.project_bucket.name
  content_type = "text/x-python"
  source       = "code/spark-jobs/transform_reviews.py"
}

resource "google_storage_bucket_object" "upload_postgres_driver" {
  name         = "resources/postgresql-42.4.0.jar"
  bucket       = google_storage_bucket.project_bucket.name
  content_type = "application/java-archive"
  source       = "resources/postgresql-42.4.0.jar"
}

resource "google_storage_bucket_object" "upload_signal" {
  name   = "data/signal/_SUCCESS"
  bucket = google_storage_bucket.project_bucket.name
  source = "signals/_SUCCESS"
}
