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
#   sensitive. The worker emits an INFO ``DEPLOYMENT_DIGEST`` event via UTL
#   ``log_event`` → the ``lifecycle-events`` Pub/Sub topic → the ni-service
#   subscriber → ``#data-pipeline-alerts``. This is the SAME relay the deployment
#   lifecycle alerts and the data-pipeline fleet monitors already use — NO HTTP
#   URL to configure. The runtime SA (unified_trading) already holds
#   pubsub.publisher on lifecycle-events (it runs the monitor jobs on the same
#   topic).
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
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
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
