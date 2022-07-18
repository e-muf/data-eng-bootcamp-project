resource "google_bigquery_dataset" "raw-movies-dataset" {
  dataset_id = var.raw_movies_dataset
}

resource "google_bigquery_dataset" "dwh-movies-dataset" {
  dataset_id = var.dwh_movies_dataset
}
