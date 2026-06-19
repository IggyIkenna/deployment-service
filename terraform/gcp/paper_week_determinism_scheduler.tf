# Epic: batch_live_symmetry_master
# Lifecycle: permanent
# ──────────────────────────────────────────────────────────────────────────────
# Paper-week daily-T+1 determinism cron — GCP Cloud Run Jobs + Cloud Scheduler
#
# Three-stage nightly pipeline (runs T+1 at 02:00 UTC so prior-day's paper run
# ledger is complete):
#
#   Stage A — paper engine run (strategy-service CLI, mode=paper)
#             TODO P7.1-A: add --operation run --mode paper --asset-group <ag>
#             CLI entrypoint does NOT exist yet; tracked as P-todo below.
#             Command placeholder: strategy-service --operation run --mode paper
#
#   Stage B — BLRS daily determinism reconcile
#             Uses the existing BLRS CLI:
#             python -m batch_live_reconciliation_service
#                    --operation reconcile --mode batch
#                    --start-date <YESTERDAY> --end-date <YESTERDAY>
#             NOTE: a dedicated --operation daily_determinism_stage is NOT yet
#             implemented; the existing reconcile operation covers the T+1 rerun.
#             Tracked as P-todo: add daily_determinism_stage.reconcile_day CLI.
#
#   Stage C — daily ledger Slack digest (client-reporting-api)
#             TODO P7.1-C: add --operation daily_ledger_digest CLI entrypoint.
#             No CLI subcommand exists yet; tracked as P-todo below.
#             Command placeholder: python -m client_reporting_api --operation
#                    daily_ledger_digest --mode batch --asset-group <ag>
#
# OPERATOR ACTIONS REQUIRED before enabling this scheduler (see runbook below):
#   1. Implement strategy-service --operation run --mode paper (P7.1-A)
#   2. Implement BLRS --operation daily_determinism_stage (P7.1-B, optional —
#      the existing reconcile op works for T+1 until this lands)
#   3. Implement client-reporting-api --operation daily_ledger_digest (P7.1-C)
#   4. Set var.paper_determinism_enabled = true in the environment tfvars
#
# Until all three CLI entrypoints exist, set paper_determinism_enabled = false
# (default) to keep the jobs declared but the scheduler trigger silent.
#
# Runbook:
#   owner:         batch_live_symmetry_master (vm-cefi, slot-1 daily tab)
#   cadence:       daily T+1 02:00 UTC (Cloud Scheduler cron: "0 2 * * *")
#   verifier:      gh run list --repo IggyIkenna/deployment-service --limit 5
#                  AND gcloud run jobs describe <name> --region <region>
#   last_executed: 2026-06-19 (terraform apply — jobs created, scheduler paused)
#
# SSOT: plans/active/citadel_paper_batch_live_reconciliation_2026_06_19.md (P7.1)
#       codex/09-strategy/operational/paper-batch-live-reconciliation.md §7
# ──────────────────────────────────────────────────────────────────────────────

variable "paper_determinism_enabled" {
  description = "Enable the Cloud Scheduler trigger for the paper-week T+1 determinism pipeline. Set true only after all three CLI entrypoints (strategy-service paper run / BLRS daily_determinism_stage / client-reporting-api daily_ledger_digest) are implemented."
  type        = bool
  default     = false
}

# ── Local helpers ─────────────────────────────────────────────────────────────

locals {
  # Image hosting all three services (same base image as manifest consolidator).
  # Stage A uses strategy-service; Stages B+C use their own images once wired.
  # For now all jobs use the unified-trading-library base which carries all deps.
  paper_det_base_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/strategy-service:latest"
  # blrs_image is declared in audit03_cron_provisioning.tf (same value) — reuse
  # local.blrs_image (a 2nd declaration breaks `tofu apply` for ALL of terraform/gcp/).
  reporting_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/client-reporting-api:latest"

  # Shared environment variables injected into every job container.
  paper_det_base_env = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Stage A — paper engine run (strategy-service, mode=paper)
#
# TODO P7.1-A [INFRA] P1. Add --operation run --mode paper --asset-group <ag>
#   CLI entrypoint to strategy-service before enabling this job.
#   strategy_service/cli/main.py: add "run" operation with PaperRunHandler.
#   Target repo: strategy-service.
# ──────────────────────────────────────────────────────────────────────────────

module "paper_engine_run_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-paper-engine-run"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.paper_det_base_image

  cpu             = "4"
  memory          = "8Gi"
  timeout_seconds = 3600  # 1h — a paper week-rerun may take O(minutes) per strategy
  max_retries     = 0     # no retry on first-run failures; next nightly will self-heal
  parallelism     = 1
  task_count      = 1

  # TODO P7.1-A: replace placeholder with real paper-run CLI args once entrypoint exists
  command = ["python"]
  args = [
    "-m", "strategy_service",
    "--operation", "run",   # TODO: implement this operation in strategy-service
    "--mode", "paper",
    "--asset-group", "cefi",
  ]

  environment_variables = local.paper_det_base_env

  service_name = "paper-engine-run"
  environment  = var.environment

  labels = {
    "purpose"  = "paper-week-determinism"
    "stage"    = "paper-run"
    "category" = "cefi"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Stage B — BLRS T+1 determinism reconcile
#
# Uses the EXISTING BLRS CLI (--operation reconcile --mode batch) for now.
# BLRS CLI (batch_live_reconciliation_service.cli.main) exposes:
#   python -m batch_live_reconciliation_service \
#          --operation reconcile --mode batch \
#          --start-date YESTERDAY --end-date YESTERDAY
#
# TODO P7.1-B [INFRA] P2. Add --operation daily_determinism_stage to BLRS CLI
#   with reconcile_day(DeterminismStage) subcommand.
#   Target repo: batch-live-reconciliation-service.
#   Until then --operation reconcile covers the T+1 batch rerun.
# ──────────────────────────────────────────────────────────────────────────────

module "blrs_daily_determinism_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-blrs-daily-determinism"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.blrs_image

  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800  # 30 min — reconcile_day over one day is fast
  max_retries     = 1     # one retry; BLRS reconcile is idempotent
  parallelism     = 1
  task_count      = 1

  # Uses $(date --date="yesterday" +%Y-%m-%d) shape via Cloud Scheduler override
  # or hard-coded to yesterday via a wrapper entrypoint. For now the image is
  # expected to accept RECONCILE_DATE env (BLRS handler reads it if present).
  command = ["python"]
  args = [
    "-m", "batch_live_reconciliation_service",
    "--operation", "reconcile",
    "--mode", "batch",
    # --start-date / --end-date are injected at runtime by the Scheduler
    # via overrides or a wrapper script; see TODO P7.1-B for the clean CLI.
  ]

  environment_variables = merge(local.paper_det_base_env, {
    # Cloud Scheduler injects RECONCILE_DATE=YYYY-MM-DD at launch time
    # (overrideable via the job's runtime args or env in the Scheduler body).
    RECONCILE_DATE = "OVERRIDE_AT_RUNTIME"
  })

  service_name = "blrs-daily-determinism"
  environment  = var.environment

  labels = {
    "purpose"  = "paper-week-determinism"
    "stage"    = "blrs-reconcile"
    "category" = "cefi"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Stage C — daily ledger Slack digest (client-reporting-api)
#
# TODO P7.1-C [INFRA] P2. Add --operation daily_ledger_digest CLI entrypoint to
#   client-reporting-api (batch-service type; no ServiceBootstrap needed for a
#   one-shot daily digest). The digest reads the day's InstructionLedger JSONL,
#   folds through materialize_position_ledger, and posts a Slack block summary.
#   Target repo: client-reporting-api.
#   cli/main.py: add "daily_ledger_digest" operation with DailyDigestHandler.
# ──────────────────────────────────────────────────────────────────────────────

module "daily_ledger_digest_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-daily-ledger-digest"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.reporting_image

  cpu             = "1"
  memory          = "2Gi"
  timeout_seconds = 600  # 10 min — digest is a lightweight read + Slack post
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # TODO P7.1-C: implement this CLI entrypoint before enabling the scheduler
  command = ["python"]
  args = [
    "-m", "client_reporting_api",
    "--operation", "daily_ledger_digest",  # TODO: add this operation
    "--mode", "batch",
    "--asset-group", "cefi",
  ]

  environment_variables = merge(local.paper_det_base_env, {
    SLACK_CHANNEL = "trading-daily-digest"
  })

  service_name = "daily-ledger-digest"
  environment  = var.environment

  labels = {
    "purpose"  = "paper-week-determinism"
    "stage"    = "daily-digest"
    "category" = "cefi"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Cloud Scheduler — daily T+1 cron at 02:00 UTC
#
# The scheduler triggers Stage A → B → C in sequence via three separate cron
# jobs with staggered offsets (A at 02:00, B at 02:30, C at 03:15). A proper
# step-function DAG (Cloud Workflows) is post-cutover scope; the staggered cron
# approach is safe because each stage is idempotent on re-run.
#
# The scheduler is PAUSED (paused = true) until all three CLI entrypoints are
# implemented. Set var.paper_determinism_enabled = true to activate.
# ──────────────────────────────────────────────────────────────────────────────

resource "google_cloud_scheduler_job" "paper_engine_run_cron" {
  count = var.paper_determinism_enabled ? 1 : 0

  name        = "${local.env_prefix}-paper-engine-run-cron"
  description = "Daily T+1 paper engine run at 02:00 UTC. Triggers Stage A of the paper-week determinism pipeline."
  schedule    = "0 2 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.paper_engine_run_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # Retry once (paper run is idempotent per run_id).
  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

resource "google_cloud_scheduler_job" "blrs_daily_determinism_cron" {
  count = var.paper_determinism_enabled ? 1 : 0

  name        = "${local.env_prefix}-blrs-daily-determinism-cron"
  description = "Daily T+1 BLRS reconcile at 02:30 UTC. Triggers Stage B (batch-live determinism recon) after paper run completes."
  schedule    = "30 2 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.blrs_daily_determinism_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

resource "google_cloud_scheduler_job" "daily_ledger_digest_cron" {
  count = var.paper_determinism_enabled ? 1 : 0

  name        = "${local.env_prefix}-daily-ledger-digest-cron"
  description = "Daily ledger Slack digest at 03:15 UTC. Triggers Stage C after BLRS reconcile completes."
  schedule    = "15 3 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.daily_ledger_digest_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "120s"
    max_doublings        = 1
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "paper_engine_run_job_name" {
  description = "Cloud Run Job name for Stage A (paper engine run). Fire manually: gcloud run jobs execute <name> --region <region>."
  value       = module.paper_engine_run_job.name
}

output "blrs_daily_determinism_job_name" {
  description = "Cloud Run Job name for Stage B (BLRS T+1 determinism reconcile)."
  value       = module.blrs_daily_determinism_job.name
}

output "daily_ledger_digest_job_name" {
  description = "Cloud Run Job name for Stage C (daily ledger Slack digest)."
  value       = module.daily_ledger_digest_job.name
}
