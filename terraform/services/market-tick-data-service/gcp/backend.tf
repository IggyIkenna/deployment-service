# Terraform Backend Configuration
# Stores state in GCS bucket for collaboration and persistence

terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/market-tick-data-service"
  }
}

# Note: Before first use, create the state bucket:
# gsutil mb -l asia-northeast1 gs://terraform-state-central-element-323112
# gsutil versioning set on gs://terraform-state-central-element-323112
