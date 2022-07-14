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

resource "google_storage_bucket_object" "upload_dag" {
  name         = "dags/ingest_gcp_sql.py"
  content_type = "text/x-python"
  source       = "code/dags/ingest_gcp_sql.py"
  bucket       = var.gcs_dag_bucket
}

resource "google_storage_bucket_object" "upload_spark_job" {
  name         = "code/transform_reviews.py"
  bucket       = google_storage_bucket.project_bucket.name
  content_type = "text/x-python"
  source       = "code/spark-jobs/transform_reviews.py"
}
