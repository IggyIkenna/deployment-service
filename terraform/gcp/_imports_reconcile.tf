# AUTO-GENERATED import blocks — live-but-unimported prod resources (TF reconciliation 2026-06-19)
# Converges `tofu plan` toward 0 spurious recreate; these resources ALREADY EXIST live.
# Owning plan: bucket_name_ssot_legacy_dual_write_remediation_2026_06_01.md

import {
  to = google_cloud_run_v2_job.plan_hygiene_sweep
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-plan-hygiene-sweep"
}

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

import {
  to = google_cloud_scheduler_job.plan_hygiene_sweep_cron
  id = "projects/central-element-323112/locations/asia-northeast1/jobs/uts-prod-plan-hygiene-sweep-cron"
}

import {
  to = google_storage_bucket.audit_records
  id = "central-element-323112/trading-audit-records-prd-central-element-323112"
}

import {
  to = google_storage_bucket.features_delta_one_cefi_test
  id = "central-element-323112/features-delta-one-cefi-test-central-element-323112"
}

import {
  to = google_storage_bucket.features_sports
  id = "central-element-323112/features-sports-central-element-323112"
}

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

import {
  to = google_storage_bucket.market_data_sports
  id = "central-element-323112/market-data-tick-sports-central-element-323112"
}

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
