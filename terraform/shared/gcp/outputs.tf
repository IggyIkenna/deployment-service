# Outputs for GCP Shared Infrastructure deployment

output "artifact_registry_repositories" {
  description = "Map of service names to Artifact Registry repository URLs"
  value       = module.shared_infrastructure.artifact_registry_repositories
}

# features_delta_one_buckets / features_volatility_buckets / features_onchain_buckets /
# ml_models_store_bucket / ml_predictions_store_bucket / ml_configs_store_bucket /
# strategy_store_buckets / execution_store_buckets / all_bucket_names REMOVED 2026-07-13 —
# the underlying module outputs were dropped along with the resources they referenced
# (bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform reconcile).

output "deployment_orchestration_bucket" {
  description = "Deployment orchestration state bucket name"
  value       = module.shared_infrastructure.deployment_orchestration_bucket
}

# `batch_processing_service_account_email` FIXED 2026-07-13 (pre-existing dangling
# reference, unrelated to the Wave 0 removal above but found in this same file): the
# module has never exposed an output by that name — only `env_service_account_email`
# (found while touching this file for the Wave 0 change; findings-triage "in your file
# → fix in same commit").
output "env_service_account_email" {
  description = "Email of the environment-specific data service account"
  value       = module.shared_infrastructure.env_service_account_email
}
