resource "google_storage_bucket" "data_lake" {
  name          = "recast-data-lake-${var.project_id}"
  location      = var.region # Use the same region as the GCP project to avoid egress costs
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  versioning {
    enabled = true
  }
}
