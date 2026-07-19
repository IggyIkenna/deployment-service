# Phase 5 Snapshot Governance Space Monitor — Cloud Run Job + Cloud Scheduler
#
# Plan: defi_governance_params_refresh_2026_06_20 Phase 5
#
# Background
#   Phase 5 adds a proactive governance monitor that polls aavedao, comp-vote,
#   and morpho Snapshot spaces every 6h.  When an active/pending proposal whose
#   title or body contains DeFi risk-parameter keywords (LTV, liquidation
#   threshold, borrow cap, supply cap, IRM, interest rate, kink, slope) is
#   detected, the handler emits a GOVERNANCE_PROPOSAL_LIVE event which is
#   routed to PAGERDUTY + TELEGRAM at HIGH severity by the alerting-service.
#
#   This is an ALERTING-ONLY job — no GCS writes, no manifest recorder.  The
#   container exits cleanly after a single poll cycle, so the Cloud Run Job
#   timeout can be small (120 s is generous for a GraphQL call).
#
# Schedule: every 6 hours (0 */6 * * *) — balances latency vs API quota.
#
# Why a separate tf file (not merged into defi_collection_scheduler.tf)
#   * Different lifecycle: this is monitoring, not data collection.
#   * Different resource naming prefix (mtds-monitor-* vs mtds-collect-*).
#   * Makes it easy to disable/tune independently without touching collection.

locals {
  snapshot_monitor_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"
}

# -------------------------------------------------------
# Cloud Run Job — Snapshot governance space monitor
# -------------------------------------------------------
module "governance_snapshot_monitor_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-mtds-monitor-snapshot-governance"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.snapshot_monitor_image
  # gen2 execution env (cpu always-allocated/unthrottled) rejects total cpu < 1; 0.5 was
  # a create-blocking config bug (job never deployed). 1 vCPU is the gen2 floor. 2026-07-19.
  cpu             = "1"
  memory          = "512Mi"
  timeout_seconds = 120
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  args = ["--operation", "monitor-snapshot-governance", "--mode", "batch", "--asset-group", "defi"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "market-tick-data-service"
  environment  = var.environment

  labels = {
    "purpose"   = "defi-governance-monitoring"
    "operation" = "monitor-snapshot-governance"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — every 6 hours
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "governance_snapshot_monitor_cron" {
  name        = "${local.env_prefix}-mtds-monitor-snapshot-governance-cron"
  description = "Poll aavedao/comp-vote/morpho Snapshot spaces every 6h; emit GOVERNANCE_PROPOSAL_LIVE on parameter-change proposals."
  schedule    = "0 */6 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.governance_snapshot_monitor_job.name}:run"

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

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "governance_snapshot_monitor_job_name" {
  description = "Cloud Run Job name for `gcloud run jobs execute` smoke tests."
  value       = module.governance_snapshot_monitor_job.name
}

output "governance_snapshot_monitor_cron_name" {
  description = "Cloud Scheduler cron name for ad-hoc `gcloud scheduler jobs run`."
  value       = google_cloud_scheduler_job.governance_snapshot_monitor_cron.name
}
