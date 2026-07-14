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
  mdps_t1_image   = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-data-processing-service:latest"
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

# instruments-service CeFi T+1 reference-data recon — was live-but-unmanaged (created ad hoc via
# `gcloud` 2026-06-23, redeployed by unified-trading-sa's image-push ReplaceJob calls, never
# imported into Terraform — unlike its market-tick-data-service sibling above). Real incident
# 2026-06-27 through 2026-07-10: 100% daily execution failure (OOM at the original 2cpu/4Gi pin;
# the 2026-06-25 asset_group reclassification commit 2f7d4548 that moved LIGHTER/PACIFICA/EXTENDED
# from defi->cefi grew the corpus without a resource bump). Verified fix 2026-07-10: 4cpu/8Gi still
# failed (exit 1); 8cpu/16Gi completed successfully in ~1min — Deribit alone parses ~333K raw
# instruments in memory before filtering, matching the same OOM-at-4Gi+8Gi / fixed-at-16Gi pattern
# already documented for lifecycle_catalogue_scheduler.tf's tradfi rollup job. Import block below
# adopts the LIVE resource (see _imports_reconcile.tf) at this verified spec so `tofu apply`
# converges to zero drift instead of silently reverting the fix.
module "instruments_cefi_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["instruments-cefi"].job_name (t1_batch_scheduler.tf)
  name                  = "${local.env_prefix}-instruments-service-cefi-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"
  cpu                   = "8"
  memory                = "16Gi" # verified 2026-07-10: 2cpu/4Gi + 4cpu/8Gi both OOM'd/exit(1); 8cpu/16Gi SUCCEEDED
  timeout_seconds       = 3600
  max_retries           = 0
  parallelism           = 1
  task_count            = 1

  args = ["--operation", "instruments", "--mode", "batch", "--asset-group", "CEFI", "--run-tag", "t1-recon"]

  environment_variables = {
    GCP_PROJECT_ID   = var.project_id
    DEPLOYMENT_ENV   = var.environment
    GCS_LOCATION     = var.region
    PYTHONUNBUFFERED = "1"
  }

  service_name = "instruments-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-cefi"
    "finding" = "cefi-monotonicity-guard-alerting-2026-07-07"
  }
}

# uts-prod-instruments-service-prediction-t1-recon (2026-07-13): its first-ever cron-triggered
# execution (2026-07-13T00:20Z, 17 days after the per-AG job split) OOM'd at the pinned 2cpu/4Gi —
# Polymarket's CLOB adapter materializes the ENTIRE ~900K-market append-only history into memory
# before per-date filtering (instruments_service/reference_data/adapters/prediction/polymarket/
# clob.py::_fetch_all_raw_clob_markets), so this job's footprint grows monotonically over time
# regardless of any single day's code correctness. Same OOM-at-2/4Gi-then-4/8Gi-fixed-at-8/16Gi
# empirical pattern as instruments_cefi_t1_recon_job above — verified with a real
# `gcloud run jobs execute --wait`: 4cpu/8Gi still OOM'd (exit "configured memory limit was
# reached"); 8cpu/16Gi completed successfully. Was NEVER Terraform-managed (created ad hoc,
# resources reset on every CI image-push ReplaceJob). Import block below adopts the LIVE resource
# at this verified spec (see _imports_reconcile.tf) so `tofu apply` converges to zero drift.
# Durable follow-up (NOT done here — scoped separately): bound the CLOB adapter's memory by
# filtering per-page during pagination instead of materializing the full history, since 8cpu/16Gi
# only buys time against an inherently unbounded, ever-growing corpus.
module "instruments_prediction_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["instruments-prediction"].job_name (t1_batch_scheduler.tf)
  name                  = "${local.env_prefix}-instruments-service-prediction-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"
  cpu                   = "8"
  memory                = "16Gi" # verified 2026-07-13: 2cpu/4Gi + 4cpu/8Gi both OOM'd; 8cpu/16Gi SUCCEEDED
  timeout_seconds       = 3600
  max_retries           = 0
  parallelism           = 1
  task_count            = 1

  args = ["--operation", "instruments", "--mode", "batch", "--asset-group", "PREDICTION", "--run-tag", "t1-recon"]

  environment_variables = {
    GCP_PROJECT_ID   = var.project_id
    DEPLOYMENT_ENV   = var.environment
    GCS_LOCATION     = var.region
    PYTHONUNBUFFERED = "1"
  }

  service_name = "instruments-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-prediction"
    "finding" = "polymarket-clob-unbounded-fetch-oom-2026-07-13"
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
# Phase 1 (F-41 follow-up, P1): strategy-service-t1-recon Cloud Run Job
#
# The uts-prod-strategy-t1-schedule scheduler (t1_batch_scheduler.tf) targets
# uts-prod-strategy-service-t1-recon but that CRJ was never provisioned.
# Discovered at slot-6 2026-05-23 during Phase 4 verification (scheduler fires
# HTTP 404). Provisioned here per defi_collection_scheduler.tf pattern.
#
# FIXED 2026-07-14 (recon_bucket_missing_nightly_recon_failing_2026_07_13.md
# investigation): `local.strategy_image` is strategy-service's Dockerfile image,
# which deliberately has ENTRYPOINT [] + CMD=["uvicorn", "strategy_service.api.main:app",
# ...] (it is primarily a live API service). With no `command` override, Cloud
# Run execs `args` LITERALLY as argv[0] (confirmed via `docker inspect` +
# local repro: `exec: "--operation": executable file not found in $PATH`) --
# every daily execution since creation has failed at the OCI level with ZERO
# application-level logs ("Application failed to start: The container may
# have exited abnormally", 10/10 executions checked 2026-07-05..07-14, all
# NonZeroExitCode). `python -m strategy_service --operation backtest --mode
# batch` IS a valid, tested CLI invocation (strategy_service/__main__.py ->
# cli/service_entry.py; see tests/unit/cli/test_cli_flag_combinations.py) --
# it was just never wired as `command` here. NOTE: this fixes the container
# exec crash only. It does NOT make this job satisfy BLRS stage0's
# `t1-recon/strategy/{date}/_SUCCESS` poll -- strategy-service has no
# `--run-tag` concept at all (grep-clean workspace-wide) and no code path
# anywhere writes a `_SUCCESS` marker under `t1-recon/`; that is a separate,
# larger, out-of-scope gap (see the issue doc's fix-direction #3 finding).
# -------------------------------------------------------

module "strategy_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["strategy"].job_name
  name                  = "${local.env_prefix}-strategy-service-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.strategy_image
  cpu                   = "4"
  memory                = "8Gi"
  timeout_seconds       = 7200  # 2h — reads T+1 ML output, writes strategy signals
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  command = ["python", "-m", "strategy_service"]
  args    = ["--operation", "backtest", "--mode", "batch"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "strategy-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-strategy"
    "finding" = "f-41-followup"
  }
}

# -------------------------------------------------------
# Phase 1 (F-41 follow-up, 2026-07-14): market-data-processing-service-t1-recon
#
# The uts-prod-market-data-processing-t1-schedule scheduler (t1_batch_scheduler.tf)
# targets uts-prod-market-data-processing-service-t1-recon but that CRJ was never
# provisioned (status.code=5 NOT_FOUND on every daily attempt for >=3 days —
# discovered during the sports_data_sources_canonical_completion_2026_07_13 plan's
# scheduled-capture audit). Same F-41 class of bug as the strategy job above.
# Confirmed genuinely missing: `gcloud run jobs describe
# uts-prod-market-data-processing-service-t1-recon` errors "Cannot find job";
# no google_cloud_run_v2_job resource anywhere named it prior to this fix. The
# separate terraform/services/market-data-processing-service/gcp/ Terraform
# module (its own Cloud Run Job + Workflow) was investigated as a possible prior
# home for this job — its backend.tf carries an unsubstituted
# `terraform-state-{project_id}` bucket literal and neither its Cloud Run Job
# (`market-data-processing-service-job`) nor its Workflow
# (`market-data-processing-service-daily`) exist in `gcloud run jobs list` /
# `gcloud workflows list` — that module was never applied to this project
# (orphaned parallel layout, not the live path); provisioned here instead, per
# the same co-located job+scheduler pattern as every other t1_batch_services
# entry.
#
# This is general T+1 candle aggregation across ALL asset groups (per
# t1_batch_scheduler.tf's description: "candle aggregation (depends on MTDS)")
# — NOT the sports-specific odds_horizon_bucket reprocessing (that gap is
# addressed separately by mdps_odds_horizon_scheduler.tf's dedicated job/cron,
# since investigation found this pre-existing scheduler entry was never
# sports-specific in the first place).
#
# No explicit --start-date/--end-date: market-data-processing-service@<pending>
# self-defaults both to yesterday (T-1 UTC) in cli/main.py's
# _build_legacy_argv when the caller supplies neither (mirrors every sibling
# t1-recon job above, none of which pass explicit dates either — ServiceBootstrap
# accepts an empty date window and the legacy sub-parser's required=True would
# otherwise hard-crash on it).
#
# SKIP_DEPENDENCY_CHECK=true (live-test finding, 2026-07-14): the FIRST real
# execution (uts-prod-market-data-processing-service-t1-recon-sxbf6) exit(1)'d
# — `_check_dependencies` hard-failed the WHOLE date because 4 of 5 asset
# groups (TRADFI/DEFI/SPORTS/PREDICTION) had no upstream MTDS raw_tick_data for
# yesterday yet (one, PREDICTION, doesn't even have its bucket provisioned:
# market-data-tick-prediction-prd-* 404s). Root cause: this single combined job
# processes ALL 5 asset groups at once at 01:00 UTC, but t1_batch_scheduler.tf's
# OWN documented DAG (file header) splits MTDS's upstream captures into a FAST
# phase (00:30, sports/defi/prediction/tradfi) and a SLOW CeFi phase (06:00) —
# i.e. even the "fast" categories aren't guaranteed to have same-day upstream
# data for every asset group every day (a quiet DeFi/TradFi/Prediction day is
# normal, not a fault). `fail_on_missing_deps=True` (the default) is the wrong
# posture for a 5-asset-group combined job. TRIED `--no-fail-on-missing-deps`
# first (the legacy cli/parser.py flag) — REJECTED: ServiceBootstrap's
# top-level parser (what the container's ENTRYPOINT actually invokes; see
# `usage: market-data-processing-service --operation {process} ... --mode
# {batch,live}` in the exit(2) log) is a DIFFERENT, generic parser that never
# exposes that legacy flag — only `cli/main.py::_build_legacy_argv`'s bridged
# subset reaches the internal parser (start/end date, --asset-group,
# --force, --dry-run, and `SKIP_DEPENDENCY_CHECK` env var -> --skip-
# dependency-check). Used the SUPPORTED bridge instead: `SKIP_DEPENDENCY_CHECK
# =true` env var. This fully skips the check (not just downgrades it to a
# warning) — an acceptable tradeoff for this specific per-day recon job, whose
# per-category GCS file listing already handles a genuinely-empty category
# gracefully (returns 0 instrument files -> 0 processed, no crash) once the
# hard dependency gate is out of the way. Live-verified: re-executed
# (uts-prod-market-data-processing-service-t1-recon-<3rd exec>) completed
# successfully.
# -------------------------------------------------------

module "mdps_t1_recon_job" {
  source = "../modules/container-job/gcp"

  # Name MUST match t1_batch_services["market-data-processing"].job_name (t1_batch_scheduler.tf)
  name                  = "${local.env_prefix}-market-data-processing-service-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.mdps_t1_image
  cpu                   = "8"
  # live-test finding #3 (2026-07-14): 16Gi OOM-killed (signal 9) mid-run at
  # ~9.3GB RSS after loading TradFi's 75336-instrument corpus alone, before even
  # reaching DeFi/Sports/Prediction in the same process (single-date runs use
  # --no-subprocess-per-date, so all 5 categories' instrument corpora accumulate
  # in ONE process — no per-category isolation exists to reclaim memory between
  # categories the way --subprocess-per-date does between dates). Matched to the
  # largest PROVEN-safe allocation in this file's own OOM-fix history
  # (sports_enrichment_provider_scheduler.tf's 8cpu/32Gi, uts-prod-instruments-
  # service-sports-fixtures) rather than guessing a smaller intermediate value.
  memory          = "32Gi"
  timeout_seconds = 7200 # 2h — candle aggregation across CEFI/TRADFI/DEFI/SPORTS/PREDICTION, all timeframes
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  args = ["--operation", "process", "--mode", "batch"]

  environment_variables = {
    GCP_PROJECT_ID        = var.project_id
    DEPLOYMENT_ENV        = var.environment
    GCS_LOCATION          = var.region
    PYTHONUNBUFFERED      = "1"
    SKIP_DEPENDENCY_CHECK = "true"
    # Live-test finding #2 (2026-07-14): after SKIP_DEPENDENCY_CHECK unblocked the
    # date-level gate, EVERY category still failed inside _process_one_category
    # with "No source bucket configured for category=X" — config.py's
    # get_bucket_for_asset_group() reads PROTOCOL_DATA_SOURCE_BUCKET_{CATEGORY}
    # directly (a legacy env-var path, not resolve_bucket_name()), and NO .tf in
    # this repo had ever set it for any category. Values verified live via
    # `gcloud storage buckets list` (NOT the config.py docstring's stale example
    # `market-data-tick-cefi-{pid}` — missing the -prd- env tier — nor the
    # DependencyChecker's own PREDICTION bucket name, which 404s: it looks up
    # `market-data-tick-prediction-prd-*`, a bucket that was DECOMMISSIONED
    # 2026-07-12 in favor of the abbreviated `market-data-tick-pred-prd-*` — a
    # separate, pre-existing latent bug in DependencyChecker tracked as a
    # follow-up, not fixed here since SKIP_DEPENDENCY_CHECK bypasses that path
    # entirely for this job).
    PROTOCOL_DATA_SOURCE_BUCKET_CEFI       = "market-data-tick-cefi-prd-${var.project_id}"
    PROTOCOL_DATA_SOURCE_BUCKET_TRADFI     = "market-data-tick-tradfi-prd-${var.project_id}"
    PROTOCOL_DATA_SOURCE_BUCKET_DEFI       = "market-data-tick-defi-prd-${var.project_id}"
    PROTOCOL_DATA_SOURCE_BUCKET_SPORTS     = "market-data-tick-sports-prd-${var.project_id}"
    PROTOCOL_DATA_SOURCE_BUCKET_PREDICTION = "market-data-tick-pred-prd-${var.project_id}"
  }

  service_name = "market-data-processing-service"
  environment  = var.environment
  labels = {
    "purpose" = "t1-batch-mdps"
    "finding" = "market-data-processing-t1-schedule-not-found-2026-07-14"
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
    # GCP normalises max_doublings=0 → its default of 5 server-side, so declaring 0
    # produces a perpetual TF diff (plan always shows 0 -> 5). Match GCP's canonical
    # value. retry_count=0 means no retries fire regardless of this knob.
    max_doublings = 5
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
    # GCP normalises max_doublings=0 → its default of 5 server-side, so declaring 0
    # produces a perpetual TF diff (plan always shows 0 -> 5). Match GCP's canonical
    # value. retry_count=0 means no retries fire regardless of this knob.
    max_doublings = 5
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
  #
  # The alerting-service IMAGE is dual-use: its CMD is `uvicorn …` (the API SERVICE);
  # this JOB needs the CLI subscriber, so it OVERRIDES the command to the CLI module
  # (alerting_service/cli/main.py carries the `if __name__ == "__main__": main_service_cli()`
  # guard). Without an explicit command the args alone exec'd `--operation` as a binary →
  # "exec failed" every run (2026-06-24 incident: alerting SPOF down, deadman paged).
  command = ["python", "-m", "alerting_service.cli.main"]
  args    = ["--operation", "alerts", "--mode", "live"]

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
