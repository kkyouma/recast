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

resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-recast-brz"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  versioning {
    enabled = true
  }
}


resource "google_storage_bucket" "silver" {
  name          = "${var.project_id}-recast-slv"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  versioning {
    enabled = true
  }
}

