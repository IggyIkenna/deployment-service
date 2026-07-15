# Terraform Backend Configuration
#
# $central-element-323112 placeholder substituted at deploy time via
# scripts/substitute-project-id.sh — standard per-service backend pattern. Distinct state
# prefix from the legacy features-sports-service module (services/features-sports-service) —
# these are two separate, coexisting Cloud Run job/workflow sets during the retirement window.

terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/features-service-sports"
  }
}
