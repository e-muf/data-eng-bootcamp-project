resource "google_storage_bucket" "project_bucket" {
  name      = var.project_bucket
  location  = var.region
}

resource "google_storage_bucket_object" "initial_data" {
  for_each      = fileset(var.data_path, "*")

  bucket        = google_storage_bucket.project_bucket.name
  name          = "data/${each.value}"
  source        = "${var.data_path}/${each.value}"
  content_type  = "application/csv"
}
