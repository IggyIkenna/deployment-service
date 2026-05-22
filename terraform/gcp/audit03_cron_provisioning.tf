# AUDIT-03 remediation — missing Cloud Run Jobs + cutover-gate schedulers
#
# Plan: audit03_deployment_cron_provisioning_2026_05_22.md
# Findings: F-39 (paper-smoke), F-40 (scenario-matrix), F-41 (t1-batch jobs), F-42 (alerting-paging)
# Re-verified: 2026-05-22 (Opus) — all four absent from workspace; provisioned here.
#
# Pattern: same as defi_collection_scheduler.tf — co-locate google_cloud_run_v2_job (container-job
# module) with google_cloud_scheduler_job so job + trigger are a single PR unit.
#
# Service accounts:
#   unified_trading SA (declared in main.tf:1429)  — runtime SA for containers
#   t1_batch SA       (declared in t1_batch_scheduler.tf:47) — invocation SA for schedulers

locals {
  # Images follow the same naming convention as mtds_image in defi_collection_scheduler.tf.
  # Single :latest tag — Cloud Build promotes per cloudbuild.yaml push step.
  blrs_image      = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/batch-live-reconciliation-service:latest"
  strategy_image  = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/strategy-service:latest"
  alerting_image  = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/alerting-service:latest"
}

# -------------------------------------------------------
# Phase 1 (F-41): Cloud Run Jobs for absent t1_batch targets
#
# t1_batch_scheduler.tf NOTE (L6-14) documented that these three scheduler
# cron entries point at Cloud Run Jobs that do NOT exist.  Fix: co-locate
# the job resource here per the defi_collection_scheduler.tf pattern.
# The NOTE is removed in t1_batch_scheduler.tf once these resources exist.
# -------------------------------------------------------

module "mtds_fast_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["market-tick-data-fast"].job_name
  name                  = "${local.env_prefix}-market-tick-data-service-fast-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.mtds_image
  cpu                   = "2"
  memory                = "8Gi"  # 4Gi OOM-killed on 2026-05-22; 8Gi verified SUCCEEDED
  timeout_seconds       = 3600
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # FAST T+1 phase: Sports odds, Prediction, TradFi (DeFi on-chain handled by
  # 11 per-operation jobs in defi_collection_scheduler.tf; CeFi delayed → cefi job)
  # Note: --operation download runs TickDataHandler (the correct T+1 batch handler).
  # --operation run was a planned meta-operation that was never implemented.
  args = ["--operation", "download", "--mode", "batch"]

  environment_variables = {
    GCP_PROJECT_ID         = var.project_id
    DEPLOYMENT_ENV         = var.environment
    CLOUD_PROVIDER         = "gcp"
    MANIFEST_PER_VM_SHARDS = "true"
  }

  service_name = "market-tick-data-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-fast"
    "finding" = "f-41"
  }
}

module "mtds_cefi_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["market-tick-data-cefi"].job_name
  name                  = "${local.env_prefix}-market-tick-data-service-cefi-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.mtds_image
  cpu                   = "4"
  memory                = "8Gi"
  timeout_seconds       = 7200  # 2h — Tardis data can be large for daily CeFi ticks
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # CeFi T+1: Tardis historical tick data (available ~6h after midnight UTC)
  # UTL STANDARD_CATEGORIES expects uppercase; canonical lowercase tracked in TODO below.
  args = ["--operation", "download", "--mode", "batch", "--asset-group", "CEFI"]

  environment_variables = {
    GCP_PROJECT_ID         = var.project_id
    DEPLOYMENT_ENV         = var.environment
    CLOUD_PROVIDER         = "gcp"
    MANIFEST_PER_VM_SHARDS = "true"
  }

  service_name = "market-tick-data-service"
  environment  = var.environment
  labels = {
    "purpose"      = "t1-batch-cefi"
    "finding"      = "f-41"
    "data-source"  = "tardis"
  }
}

module "batch_live_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["batch-live-reconciliation"].job_name
  name                  = "${local.env_prefix}-batch-live-reconciliation-service"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.blrs_image
  cpu                   = "2"
  memory                = "4Gi"
  timeout_seconds       = 7200  # 2h — polls GCS for upstream availability before reconciling
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # T+1 orchestrator: reads batch pipeline outputs, writes reconciliation report
  args = ["--operation", "reconcile", "--mode", "batch"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "batch-live-reconciliation-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-recon"
    "finding" = "f-41"
  }
}

# -------------------------------------------------------
# Phase 2 (F-39): mtds-paper-smoke — backtest-fidelity gate
#
# Runs a one-day backtest smoke against yesterday's MTDS data via the
# strategy-service config-grid script. Verifies that the data pipeline
# produces tradeable signals end-to-end: MTDS → features → strategy.
# Schedule: 05:30 UTC daily (after slow-phase pipeline completes at ~05:00)
# -------------------------------------------------------

module "mtds_paper_smoke_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-mtds-paper-smoke"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.strategy_image
  cpu                   = "2"
  memory                = "4Gi"
  timeout_seconds       = 3600  # 1h — single-day backtest; typically 5-15 min
  max_retries           = 0     # No retry — failure = signal that pipeline is broken
  parallelism           = 1
  task_count            = 1

  # Single-day backtest smoke: carry_staked_basis + arbitrage_price_dispersion.
  # --days 1 = yesterday only; --smoke exits with code 1 if no signals generated.
  args = [
    "python", "scripts/run_2yr_config_grid_backtest.py",
    "--days", "1",
    "--archetype", "carry_staked_basis",
    "--smoke"
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "strategy-service"
  environment  = var.environment
  labels = {
    "purpose" = "paper-smoke"
    "finding" = "f-39"
    "gate"    = "backtest-fidelity"
  }
}

resource "google_cloud_scheduler_job" "mtds_paper_smoke_cron" {
  name        = "${local.env_prefix}-mtds-paper-smoke-cron"
  description = "Backtest-fidelity gate — runs a 1-day carry_staked_basis backtest smoke after T+1 pipeline completes."
  schedule    = "30 5 * * *"  # 05:30 UTC daily; slow phase finishes by 05:00
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.mtds_paper_smoke_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 0
  }
}

# -------------------------------------------------------
# Phase 2 (F-40): mtds-scenario-matrix — scenario regression gate
#
# Runs the scenario matrix for carry_staked_basis to verify risk controls
# (kill-switch, circuit-breaker) trip correctly under stress scenarios.
# CROSS-PLAN DEP: requires DEFI_LST_DEPEG_STETH_5PCT scenario in UAC
# registry/scenarios/defi.py (audit03_carry_execution_safety_remediation
# Phase 1) before this job produces meaningful results.
# Schedule: 08:00 UTC daily (after reconciliation report at 09:00 is tight
#   — use 08:00 so scenario matrix result is available before operator review)
# -------------------------------------------------------

module "mtds_scenario_matrix_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-mtds-scenario-matrix"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.strategy_image
  cpu                   = "4"
  memory                = "8Gi"  # Scenario matrix runs multiple overlays; higher memory for parallelism
  timeout_seconds       = 10800  # 3h — full scenario matrix for carry_staked_basis
  max_retries           = 0
  parallelism           = 1
  task_count            = 1

  # Scenario matrix: carry_staked_basis × all registered scenarios (incl.
  # DEFI_LST_DEPEG_STETH_5PCT from audit03_carry_execution_safety_remediation Phase 1).
  args = [
    "python", "scripts/run_2yr_config_grid_backtest.py",
    "--scenario-matrix", "carry_staked_basis"
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "strategy-service"
  environment  = var.environment
  labels = {
    "purpose" = "scenario-regression"
    "finding" = "f-40"
    "gate"    = "scenario-matrix"
  }
}

resource "google_cloud_scheduler_job" "mtds_scenario_matrix_cron" {
  name        = "${local.env_prefix}-mtds-scenario-matrix-cron"
  description = "Scenario-regression gate — runs carry_staked_basis scenario matrix (incl. depeg kill-switch test)."
  schedule    = "0 8 * * *"  # 08:00 UTC daily
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.mtds_scenario_matrix_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 0
  }
}

# -------------------------------------------------------
# Phase 2 (F-42): alerting-paging — live-trading P&L / position-breach paging
#
# Runs the alerting-service subscriber loop on a schedule to page operators
# when P&L / position / kill-switch thresholds are breached during live trading.
# The paging CODE + Telegram secret already exist (alerting-service@phase4-done);
# only this scheduler was missing.
#
# Schedule: every hour. Job runs for 55 min (timeout=3300s) then terminates;
# next run starts at top of next hour — continuous coverage with 5-min gap.
# -------------------------------------------------------

module "alerting_paging_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-alerting-paging"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.alerting_image
  cpu                   = "1"
  memory                = "1Gi"
  timeout_seconds       = 3300  # 55 min; scheduler re-fires at top of next hour
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # Subscriber loop: processes PubSub alert events, pages via Telegram + PagerDuty
  # on CRITICAL/HIGH severity (P&L breach / position-breach / kill-switch arm).
  # RUN_DURATION_HOURS not set → terminates via Cloud Run Job timeout (SIGTERM).
  args = ["--operation", "alerts", "--mode", "live"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "alerting-service"
  environment  = var.environment
  labels = {
    "purpose" = "live-paging"
    "finding" = "f-42"
    "gate"    = "alerting-paging"
  }
}

resource "google_cloud_scheduler_job" "alerting_paging_cron" {
  name        = "${local.env_prefix}-alerting-paging-cron"
  description = "Live-trading P&L + position-breach paging — runs alerting-service subscriber for 55 min every hour."
  schedule    = "0 * * * *"  # every hour on the hour
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.alerting_paging_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}
