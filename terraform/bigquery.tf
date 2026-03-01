resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "recast_staging"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = "recast_marts"
  location                   = var.region
  delete_contents_on_destroy = true
}
