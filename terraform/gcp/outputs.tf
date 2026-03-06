# Outputs for GCP UCI bootstrap — p3-terraform-gcp

# =============================================================================
# GCS Bucket Names
# =============================================================================

output "market_data_bucket" {
  description = "Name of the market data GCS bucket (raw and normalized tick data)"
  value       = google_storage_bucket.market_data.name
}

output "models_bucket" {
  description = "Name of the ML model artifacts GCS bucket"
  value       = google_storage_bucket.models.name
}

output "features_bucket" {
  description = "Name of the computed feature store GCS bucket"
  value       = google_storage_bucket.features.name
}

output "deployment_state_bucket" {
  description = "Name of the deployment config and state GCS bucket"
  value       = google_storage_bucket.deployment_state.name
}

# =============================================================================
# BigQuery Dataset IDs
# =============================================================================

output "market_data_dataset_id" {
  description = "BigQuery dataset ID for market data tables"
  value       = google_bigquery_dataset.market_data.dataset_id
}

output "features_dataset_id" {
  description = "BigQuery dataset ID for computed feature tables"
  value       = google_bigquery_dataset.features.dataset_id
}

output "ml_models_dataset_id" {
  description = "BigQuery dataset ID for model metadata and metrics"
  value       = google_bigquery_dataset.ml_models.dataset_id
}

output "audit_dataset_id" {
  description = "BigQuery dataset ID for audit logs and compliance events"
  value       = google_bigquery_dataset.audit.dataset_id
}

# =============================================================================
# Service Account
# =============================================================================

output "service_account_email" {
  description = "Email of the unified-trading-sa service account"
  value       = google_service_account.unified_trading.email
}
