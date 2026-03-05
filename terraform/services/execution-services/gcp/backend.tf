# Terraform Backend Configuration

terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/execution-services"
  }
}
