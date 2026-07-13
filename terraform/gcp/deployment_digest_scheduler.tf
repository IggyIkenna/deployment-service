# Deployment-estate daily digest — Cloud Run Job + daily cron
#
# Parity #5 of deployment_observability_parity_live_batch_paper_2026_06_22.md:
# a once-a-day Slack digest of the deployment estate rolled up per umbrella —
# LIVE (targets up), BATCH (completions + failures), PAPER (run status). The
# operator gets ONE morning glance instead of watching the real-time lifecycle
# stream all day. It is the deployment-estate companion to the client-reporting
# DAILY_LEDGER_DIGEST (the per-client book).
#
# Runtime
#   An ISOLATED Cloud Run Job built from the deployment-api image, running
#   ``python -m deployment_api.scripts.deployment_digest_worker``. Isolated (its
#   own memory envelope, off the live deployment-api service's request path) for
#   the same reason the data-pipeline fleet monitors run as dedicated jobs — the
#   digest loads the full inventory census, and the live service is memory-
#   sensitive. The worker builds an INFO AlertEvent and POSTs it to
#   alerting-service (the same /api/v1/alerts/rules/recent ingress the
#   client-reporting digest posts to); alerting-service routes INFO via the
#   catch-all rule to Slack.
#
# Alerting URL (honest no-op default)
#   The worker reads ALERTING_SERVICE_URL from config. When unset (the default),
#   the digest is an HONEST NO-OP — it logs the built message and skips the POST
#   (never a fabricated send). Set ``alerting_service_url`` to the ni-service /
#   alerting-service Cloud Run URL to activate posting. This keeps the cron green
#   from day one and lights up the moment the URL is provided.
#
# Schedule
#   ``30 7 * * *`` UTC — a single morning digest (after overnight batch settles).
#   exit-0-always + max_retries=0: a transient failure is logged and the next
#   day's cron re-runs the idempotent (stateless read-and-post) digest.
#
# Service account
#   * Scheduler invoker: t1_batch (OAuth; needs run.jobs.run on the job)
#   * Container runtime: unified_trading (reads the same inventory sources the
#     deployment-api service reads — already granted fleet-wide).
#
# Plan: unified-trading-pm/plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md #5

variable "alerting_service_url" {
  type        = string
  default     = ""
  description = "Base URL of alerting-service (ni-service Cloud Run URL) for the deployment digest POST. Empty = honest no-op (worker logs + skips the POST)."
}

locals {
  deployment_digest_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-api:latest"
}

module "deployment_digest_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-deployment-digest"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.deployment_digest_image

  # The digest loads the full inventory census (VM + Cloud Run rollup). 4Gi/cpu2
  # matches the census load with headroom; daily cadence makes this cheap.
  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 300
  max_retries     = 0 # exit-0-always; the next daily cron re-runs the idempotent digest
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "deployment_api.scripts.deployment_digest_worker",
  ]

  environment_variables = {
    GCP_PROJECT_ID       = var.project_id
    DEPLOYMENT_ENV       = var.environment
    CLOUD_PROVIDER       = "gcp"
    ALERTING_SERVICE_URL = var.alerting_service_url
  }

  service_name = "deployment-digest"
  environment  = var.environment

  labels = {
    "purpose" = "deployment-digest"
  }
}

resource "google_cloud_scheduler_job" "deployment_digest_cron" {
  name        = "${local.env_prefix}-deployment-digest-cron"
  description = "Daily per-umbrella deployment-estate digest (LIVE up / BATCH completions+failures / PAPER status) → alerting-service → Slack."
  schedule    = "30 7 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.deployment_digest_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries — the next daily cron re-runs the idempotent digest; avoids a
  # retry-storm when the inventory source or alerting-service is genuinely down.
  retry_config {
    retry_count          = 0
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }
}

output "deployment_digest_cron_name" {
  description = "Cloud Scheduler cron name for the daily deployment-estate digest."
  value       = google_cloud_scheduler_job.deployment_digest_cron.name
}
