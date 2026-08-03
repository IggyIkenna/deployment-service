# AUTO-GENERATED import blocks — live-but-unimported prod resources (TF reconciliation 2026-06-19)
# Converges `tofu plan` toward 0 spurious recreate; these resources ALREADY EXIST live.
# Owning plan: bucket_name_ssot_legacy_dual_write_remediation_2026_06_01.md

# google_cloud_run_v2_job.plan_hygiene_sweep import REMOVED 2026-07-28 (RULE-11 prove-then-retire,
# plan_hygiene_precommit_and_agentic_resolution_2026_06_10.md / operator approval
# june_2026_vintage_audit_findings_2026_07_27.md §5-RESOLVED #21): superseded by the daily deep
# `plan-reconciler` agent (systemd timer on the central orchestrator VM, canary-watched). The resource
# block was removed from hygiene_sweep_scheduler.tf (file deleted) + the live Cloud Run Job
# `uts-prod-plan-hygiene-sweep` was deleted via `gcloud run jobs delete` — this import block must go
# too (an import whose `to` resource no longer exists in config errors `tofu plan`/`apply`).

import {
  to = google_cloud_scheduler_job.alerting_paging_cron
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-alerting-paging-cron"
}

import {
  to = google_cloud_scheduler_job.code_tarball_refresh_cron
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-code-tarball-refresh-cron"
}

import {
  to = google_cloud_scheduler_job.honest_coverage_daily
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/honest-coverage-daily"
}

import {
  to = google_cloud_scheduler_job.mtds_paper_smoke_cron
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-mtds-paper-smoke-cron"
}

import {
  to = google_cloud_scheduler_job.mtds_scenario_matrix_cron
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-mtds-scenario-matrix-cron"
}

# google_cloud_scheduler_job.plan_hygiene_sweep_cron import REMOVED 2026-07-28 (same RULE-11
# retirement as the plan_hygiene_sweep job import above) — the live Cloud Scheduler job
# `uts-prod-plan-hygiene-sweep-cron` was deleted via `gcloud scheduler jobs delete`.

import {
  to = google_storage_bucket.audit_records
  id = "central-element-323112/trading-audit-records-prd-central-element-323112"
}

# import google_storage_bucket.features_delta_one_cefi_test REMOVED 2026-07-19 (Wave-3 alias-sunset /
# terraform-drift reconciliation): its target resource was removed from main.tf + the
# features-delta-one-cefi-test bucket physically deleted (features-delta-one folded into features-{ag}),
# so this import block must go too (an import whose `to` resource no longer exists errors tofu plan/apply).

# import google_storage_bucket.features_sports REMOVED 2026-07-15
# (features_sports_service_consolidation_deploy_2026_07_15 FinishBucketAndDocs phase): its target
# resource was removed from main.tf + `tofu state rm`'d and the flat features-sports-{pid} bucket
# physically deleted, so this import block must go too (an import block whose `to` resource no longer
# exists in config errors `tofu plan`/`apply`). Canonical features-sports-prd-{pid} is the sole SSOT.

# REMOVED 2026-07-13 (bucket_estate_consolidation_to_sub100_2026_07_13.md Wave 0 terraform
# reconcile — the target resource no longer exists in config, so its import block must go
# too, or `terraform plan`/`apply` errors on an import target with no matching resource):
#   google_storage_bucket.features_volatility_cefi_test  (REMOVE_STALE)
#   google_storage_bucket.gas_fees_test                  (REMOVE_STALE)
#   google_storage_bucket.instruments_cefi_test          (double-declare — state-mv'd to
#                                                          google_storage_bucket.canonical)
#   google_storage_bucket.instruments_defi_test          (double-declare — state-mv'd)
#   google_storage_bucket.instruments_prediction_test    (REMOVE_STALE)
#   google_storage_bucket.instruments_sports_test         (double-declare — state-mv'd)
#   google_storage_bucket.instruments_tradfi_test         (double-declare — state-mv'd)
#   google_storage_bucket.market_data_cefi_test           (double-declare — state-mv'd)
#   google_storage_bucket.market_data_defi_test           (double-declare — state-mv'd)
#   google_storage_bucket.market_data_prediction_test     (REMOVE_STALE)
#   google_storage_bucket.market_data_sports_test         (double-declare — state-mv'd)
#   google_storage_bucket.market_data_tradfi_test         (double-declare — state-mv'd)
# See scratchpad/tf_state_surgery.sh for the corresponding `state rm` / `state mv` ops.

# google_storage_bucket.market_data_sports import REMOVED 2026-07-17 — the legacy MDT sports bucket
# is deleted (sports_legacy_bucket_cutover T5.4-MDT / OR-5b RESOLVED); resource block removed from
# main.tf + state entry removed, so there is nothing to import.

import {
  to = module.alerting_paging_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-alerting-paging"
}

import {
  to = module.batch_live_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-batch-live-reconciliation-service"
}

import {
  to = module.code_tarball_refresh_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/code-tarball-refresh"
}

import {
  to = module.honest_coverage_daily_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/honest-coverage-daily-launcher"
}

import {
  to = module.mtds_cefi_t1_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-market-tick-data-service-cefi-t1-recon"
}

import {
  to = module.mtds_fast_t1_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-market-tick-data-service-fast-t1-recon"
}

import {
  to = module.instruments_cefi_t1_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-instruments-service-cefi-t1-recon"
}

import {
  to = module.instruments_prediction_t1_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-instruments-service-prediction-t1-recon"
}

import {
  to = module.mtds_paper_smoke_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-mtds-paper-smoke"
}

import {
  to = module.mtds_scenario_matrix_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-mtds-scenario-matrix"
}

import {
  to = module.strategy_t1_recon_job.google_cloud_run_v2_job.job
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-strategy-service-t1-recon"
}

# unified_trading_sa_live_iam_drift_vs_terraform_2026_07_31.md P2 — 29 project-level IAM roles
# already live on unified-trading-sa (granted outside terraform) now declared in main.tf; these
# imports bind each new resource to the ALREADY-LIVE binding instead of terraform trying to
# (re-)create a binding that already exists. google_project_iam_member import ID format:
# "{project} {role} {member}" (three space-separated fields).
import {
  to = google_project_iam_member.unified_trading_artifactregistry_admin
  id = "central-element-323112 roles/artifactregistry.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_bigquery_admin
  id = "central-element-323112 roles/bigquery.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_bigquery_job_user
  id = "central-element-323112 roles/bigquery.jobUser serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudbuild_builds_editor
  id = "central-element-323112 roles/cloudbuild.builds.editor serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudbuild_builds_viewer
  id = "central-element-323112 roles/cloudbuild.builds.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudfunctions_viewer
  id = "central-element-323112 roles/cloudfunctions.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudkms_viewer
  id = "central-element-323112 roles/cloudkms.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudscheduler_admin
  id = "central-element-323112 roles/cloudscheduler.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudscheduler_viewer
  id = "central-element-323112 roles/cloudscheduler.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_cloudsql_admin
  id = "central-element-323112 roles/cloudsql.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_compute_admin
  id = "central-element-323112 roles/compute.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_datastore_owner
  id = "central-element-323112 roles/datastore.owner serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_datastore_user
  id = "central-element-323112 roles/datastore.user serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_service_account_admin
  id = "central-element-323112 roles/iam.serviceAccountAdmin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_iap_tunnel_resource_accessor
  id = "central-element-323112 roles/iap.tunnelResourceAccessor serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_logging_config_writer
  id = "central-element-323112 roles/logging.configWriter serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_logging_log_writer
  id = "central-element-323112 roles/logging.logWriter serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_logging_view_accessor
  id = "central-element-323112 roles/logging.viewAccessor serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_logging_viewer
  id = "central-element-323112 roles/logging.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_monitoring_alert_policy_editor
  id = "central-element-323112 roles/monitoring.alertPolicyEditor serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_pubsub_admin
  id = "central-element-323112 roles/pubsub.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_pubsub_publisher
  id = "central-element-323112 roles/pubsub.publisher serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_pubsub_viewer
  id = "central-element-323112 roles/pubsub.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_project_iam_admin
  id = "central-element-323112 roles/resourcemanager.projectIamAdmin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_run_admin
  id = "central-element-323112 roles/run.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_run_developer
  id = "central-element-323112 roles/run.developer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_secretmanager_admin
  id = "central-element-323112 roles/secretmanager.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_secretmanager_viewer
  id = "central-element-323112 roles/secretmanager.viewer serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}

import {
  to = google_project_iam_member.unified_trading_storage_full_admin
  id = "central-element-323112 roles/storage.admin serviceAccount:unified-trading-sa@central-element-323112.iam.gserviceaccount.com"
}
