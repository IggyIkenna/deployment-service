# Tarball Cleanup — Cloud Run Job + daily Cloud Scheduler cron
#
# Plan: plans/active/issues/issue_docs_remediation_sweep_2026_06_02.md
#
# Background
#   SHA-versioned code tarballs accumulate in the deployment-scripts-{project_id}
#   GCS bucket under gs://deployment-scripts-{project_id}/code/. As of 2026-06-01,
#   ~366 live @sha objects exist, growing with every tarball push. The cleanup
#   script (scripts/vm/cleanup_old_tarballs.py --keep 5) trims each service to
#   the 5 most-recent SHA-versioned tarballs, deleting the rest.
#
#   This file ships the scheduling layer: one Cloud Run Job (single, not
#   per-bucket for_each — only one deployment-scripts bucket per project) +
#   one Cloud Scheduler cron at daily 02:00 UTC.
#
# Script entrypoint
#   deployment-service/scripts/vm/cleanup_old_tarballs.py
#   Args: --project <PROJECT_ID> --keep 5
#   The --bucket arg is omitted; the script defaults to
#   deployment-scripts-{project} which is the canonical bucket.
#
# Image
#   Reuses market-tick-data-service:latest (same registry/image already used
#   by manifest-consolidator jobs — no new build pipeline needed; UTL is a dep).
#
# Service accounts
#   * Container runtime: unified_trading_sa — needs storage.objectAdmin on
#     deployment-scripts-{project_id} to list and delete tarballs.
#   * Scheduler invoker: t1_batch_sa — already has roles/run.invoker.
#
# Schedule
#   Daily at 02:00 UTC — light off-peak window; ~5s per run when ~360 objects
#   exist. Does NOT conflict with 02:00 UTC t1 batch window (that is 02:00 UTC
#   for features-calendar, not a cleanup job).

module "tarball_cleanup_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-tarball-cleanup"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  # Reuses MTDS image (UTL bundled as a dep; same as manifest-consolidator jobs).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  # Cleanup is a list-and-delete pass; single-digit CPU + modest memory is
  # sufficient for ~400 objects. 300s (5 min) is generous for the expected ~5s
  # wall-time per run even with burst-parallel deletes.
  cpu             = "1"
  memory          = "512Mi"
  timeout_seconds = 300
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # Invoke cleanup_old_tarballs.py as a module entrypoint.
  # --bucket is omitted — the script defaults to deployment-scripts-{project}.
  # --keep 5 retains the 5 most recent SHA-versioned tarballs per service.
  command = ["python"]
  args    = ["deployment-service/scripts/vm/cleanup_old_tarballs.py", "--project", var.project_id, "--keep", "5"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "tarball-cleanup"
  environment  = var.environment

  labels = {
    "purpose" = "tarball-cleanup"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — daily 02:00 UTC
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "tarball_cleanup_cron" {
  name        = "${local.env_prefix}-tarball-cleanup-cron"
  description = "Daily cleanup of old SHA-versioned code tarballs in deployment-scripts-${var.project_id}."
  schedule    = "0 2 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.tarball_cleanup_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # Single retry on transient GCS/API error; the next day's cron covers
  # persistent failures. Idempotent: re-running keeps the same N most-recent.
  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "120s"
    max_doublings        = 1
  }
}

# -------------------------------------------------------
# Outputs — for verification scripts and dashboards
# -------------------------------------------------------
output "tarball_cleanup_job_name" {
  description = "Cloud Run Job name for tarball cleanup (ad-hoc fire: `gcloud run jobs execute`)."
  value       = module.tarball_cleanup_job.name
}

output "tarball_cleanup_cron_name" {
  description = "Cloud Scheduler cron name for daily tarball cleanup."
  value       = google_cloud_scheduler_job.tarball_cleanup_cron.name
}
