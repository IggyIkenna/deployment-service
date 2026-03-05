terraform {
  backend "gcs" {
    bucket = "terraform-state-central-element-323112"
    prefix = "services/features-multi-timeframe-service"
  }
}
