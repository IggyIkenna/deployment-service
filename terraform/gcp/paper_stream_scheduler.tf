# Epic: batch_live_symmetry_master
# Lifecycle: permanent
# ──────────────────────────────────────────────────────────────────────────────
# Paper-STREAM continuous-live companion (B2) — GCP Cloud Run Job + Cloud Scheduler
#
# The DAILY determinism cron (paper_week_determinism_scheduler.tf) is a PROOF run
# (paper(W) == batch-rerun(W), ε=0) over a T-1 window under client
# `firm-paper-determinism`. This is its CONTINUOUS-LIVE companion: a bounded loop
# (`--operation paper-stream`) that each tick re-runs the EXISTING paper-run
# machinery over a window ENDING TODAY, writing to a STABLE per-UTC-day run_id
# `paper-stream-{asset_group}-{YYYYMMDD}` under a SEPARATE client
# `firm-paper-stream`. As the DeFi continuous-capture forward-poll fills today's
# blocks, successive ticks book fresh trades → the 5s-poll
# `/paper-trading?client=firm-paper-stream` page renders the book live.
#
# Self-healing cadence: the loop runs --stream-duration-seconds (3300s = 55 min)
# then exits; the hourly Cloud Scheduler re-fires it → a finished loop self-heals
# into the next with a ~5-min gap (no overlap → no two loops racing the same
# run_id). batch=live preserved: every tick is a deterministic paper-run; the loop
# cadence is operational, NEVER in the ledger. The determinism client + the
# `daily_ledger_digest` (which keys off resolve_canonical_run for
# `firm-paper-determinism`) are UNTOUCHED — the stream is a separate client.
#
# Runbook:
#   owner:         batch_live_symmetry_master (continuous-live companion)
#   cadence:       hourly (Cloud Scheduler cron "0 * * * *"); each run loops 55 min
#   verifier:      gcloud run jobs executions list --job <name> --region <region>
#                  AND the firm-paper-stream ledger root grows a paper-stream-* run
#   last_executed: <pending first apply>
#
# SSOT: plans/active/data_completion_to_100_all_ag_2026_06_21.md (B2, continuous-live)
#       plans/active/citadel_paper_batch_live_reconciliation_2026_06_19.md (determinism)
# ──────────────────────────────────────────────────────────────────────────────

variable "paper_stream_enabled" {
  description = "Enable the hourly Cloud Scheduler trigger for the B2 continuous-live paper-stream loop. The job is always declared; this gates only the scheduler trigger. Default true — a `tofu apply` instantiates it. Set false to pause."
  type        = bool
  default     = true
}

# ── Stage — continuous-live paper-stream loop (strategy-service, mode=paper) ────

module "paper_stream_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-paper-stream"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.paper_det_base_image # strategy-service:latest (same base as paper-engine)

  cpu             = "4"
  memory          = "8Gi"
  timeout_seconds = 3600 # > --stream-duration-seconds 3300 (55-min loop) + margin
  max_retries     = 0    # no retry; the next hourly trigger self-heals
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "strategy_service",
    "--operation", "paper-stream",
    "--mode", "paper",
    "--asset-group", "DEFI", # CLI argparse choices are UPPERCASE (CEFI/TRADFI/DEFI/…)
    "--client-id", "firm-paper-stream",
    "--stream-duration-seconds", "3300", # 55-min bounded loop (gap before next hourly fire)
    "--stream-interval-seconds", "60",   # one deterministic paper-run tick / minute
    "--rolling-days", "7",
  ]

  environment_variables = local.paper_det_base_env

  service_name = "paper-stream"
  environment  = var.environment

  labels = {
    "purpose"  = "paper-stream-continuous-live"
    "stage"    = "paper-stream"
    "category" = "defi"
  }
}

# ── Hourly Cloud Scheduler trigger (gated by var.paper_stream_enabled) ──────────

resource "google_cloud_scheduler_job" "paper_stream_cron" {
  count = var.paper_stream_enabled ? 1 : 0

  name        = "${local.env_prefix}-paper-stream-cron"
  description = "Hourly trigger for the B2 continuous-live paper-stream loop (each run loops ~55 min, self-heals into the next hour)."
  schedule    = "0 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.paper_stream_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 0 # a missed fire self-heals on the next hour; no overlap
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

output "paper_stream_job_name" {
  description = "The continuous-live paper-stream Cloud Run job name."
  value       = module.paper_stream_job.name
}
