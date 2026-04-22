# Terraform Backend Configuration
#
# $central-element-323112 placeholder substituted at deploy time via
# scripts/substitute-project-id.sh — matches features-onchain-service pattern.

terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/features-sports-service"
  }
}
