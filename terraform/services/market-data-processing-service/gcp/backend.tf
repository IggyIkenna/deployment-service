# Terraform Backend Configuration
# Stores state in GCS bucket for collaboration and persistence

terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/market-data-processing-service"
  }
}
