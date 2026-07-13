# Outputs for GCP Shared Infrastructure Module

# =============================================================================
# Artifact Registry Outputs
# =============================================================================

output "artifact_registry_repositories" {
  description = "Map of service names to Artifact Registry repository URLs"
  value = {
    for service, repo in google_artifact_registry_repository.service_repos :
    service => "${var.region}-docker.pkg.dev/${var.project_id}/${repo.repository_id}"
  }
}

# =============================================================================
# GCS Bucket Outputs - Features / ML / Strategy / Execution — REMOVED 2026-07-13
# =============================================================================
#
# features_delta_one_buckets / features_volatility_buckets / features_onchain_buckets /
# ml_models_store_bucket / ml_predictions_store_bucket / ml_configs_store_bucket /
# ml_training_artifacts_bucket / strategy_store_buckets / execution_store_buckets all
# referenced the resources removed from main.tf (bucket_estate_consolidation_to_sub100_
# 2026_07_13.md Wave 0) — dropped in the same change; grep found no consumer of any of
# these outputs (`terraform output <name>` or a `module.shared_infrastructure.<name>`
# reference) anywhere in the workspace.

# =============================================================================
# GCS Bucket Outputs - Deployment
# =============================================================================

output "deployment_orchestration_bucket" {
  description = "Deployment orchestration state bucket name"
  value       = var.create_gcs_buckets ? google_storage_bucket.deployment_orchestration[0].name : ""
}

# =============================================================================
# Service Account Outputs
# =============================================================================

output "env_service_account_email" {
  description = "Email of the environment-specific data service account"
  value       = var.create_service_accounts ? google_service_account.env_sa[0].email : ""
}

output "env_service_account_name" {
  description = "Name of the environment-specific data service account"
  value       = var.create_service_accounts ? google_service_account.env_sa[0].name : ""
}

# =============================================================================
# Summary Outputs
# =============================================================================

output "all_bucket_names" {
  description = "List of all created bucket names"
  value = concat(
    var.create_gcs_buckets ? [google_storage_bucket.deployment_orchestration[0].name] : [],
  )
}
