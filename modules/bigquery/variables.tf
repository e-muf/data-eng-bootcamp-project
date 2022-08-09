variable "raw_movies_dataset" {
  description = "BigQuery dataset which store raw data"
  type        = string
}

variable "dwh_movies_dataset" {
  description = "BigQuery dataset whcih store datawarehouse schema"
  type        = string
}
