# Outputs for GCP UCI bootstrap — p3-terraform-gcp

# =============================================================================
# GCS Bucket Names
# =============================================================================

# market_data_cefi_bucket output REMOVED 2026-07-14 — its bucket resource was removed (legacy
# twin mid-async-purge-delete; see main.tf). No consumer references this output.

# instruments_cefi_bucket output REMOVED 2026-07-17 — its bucket resource was removed
# (terraform_instruments_cefi_armed_resurrection_2026_07_16.md: the physical bucket was deleted
# 2026-07-14 while the block stayed declared + in prod state, arming a recreate-as-empty-shell on
# the next apply; see main.tf). No consumer references this output (grep across all repos: the
# declaration itself was the only hit). Same shape as the market_data_cefi_bucket removal above.

# features_calendar_bucket output REMOVED 2026-07-15 — its bucket resource was removed
# (bucket_estate_consolidation Verify+Delete phase, bucketKey=features-calendar; flat
# features-calendar-{pid} deleted, canonical features-calendar-{env}-{pid} is the sole SSOT).
# No consumer referenced this output (grep found none).

# ml_models_bucket output REMOVED 2026-07-13 — google_storage_bucket.ml_models (the
# long-env `-${var.environment}-` resource) was deleted (REMOVE_STALE,
# bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0). Canonical
# `ml-models-store-{env}-{pid}` is now `google_storage_bucket.canonical["ml-models-store-…"]`
# in canonical_buckets.tf — no output was ever consumed downstream (grep found none).

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
  value       = google_bigquery_dataset.ml_models_bq.dataset_id
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
