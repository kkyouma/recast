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

resource "google_bigquery_table" "readings" {
  dataset_id               = google_bigquery_dataset.staging.dataset_id
  table_id                 = "generacion_real"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "DAY"
    field = "fecha_hora"
  }
  clustering = ["id_central", "tipo_tecnologia", "id_propietario"]

  schema = file("${path.module}/schemas/generacion_real.json")
}


resource "google_bigquery_table" "centrales_info" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  table_id   = "centrales_info"

  deletion_protection = false

  clustering = ["id_central", "tipo_central"]

  schema = file("${path.module}/schemas/centrales_info.json")
}
