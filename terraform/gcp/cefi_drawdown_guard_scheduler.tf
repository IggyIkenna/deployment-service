# CeFi cumulative-drawdown guard — daily Cloud Run Job + Cloud Scheduler
#
# cefi_cumulative_drawdown_guard_2026_06_27.py was a read-only report script with NO
# scheduler entry anywhere (workspace-wide .tf/.yml grep: zero hits) — its own header
# said "# Delete-when: superseded by a CLI subcommand once wired into the catalogue
# build / QG (recurring need)". A thin-day-collapse / dark-venue outage (LIGHTER,
# PACIFICA) went undetected for 11+ days because nothing ran this script automatically.
#
# Pattern: same as honest_coverage_scheduler.tf — co-locate a container-job module
# with a google_cloud_scheduler_job http_target that :run's it.
#
# Fires daily at 07:00 UTC — after the CeFi t1-recon fetch job (06:00, ~1min) has had
# time to complete, so the day's capture is reflected before the guard reads it.
#
# References:
#   instruments-service/scripts/cefi_cumulative_drawdown_guard_2026_06_27.py
#   plans/active/issues/cefi_monotonicity_guard_alerting_and_dark_venues_2026_07_07.md

module "cefi_drawdown_guard_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-cefi-drawdown-guard-daily"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  # instruments-service image — the script lives at
  # /app/instruments-service/scripts/ (Dockerfile WORKDIR=/app/instruments-service, COPY . .).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = "1"
  memory          = "2Gi" # read-only single-parquet pandas groupby — far lighter than the fetch job
  timeout_seconds = 600
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args    = ["/app/instruments-service/scripts/cefi_cumulative_drawdown_guard_2026_06_27.py"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
  }

  service_name = "instruments-service"
  environment  = var.environment
  labels = {
    "purpose" = "cefi-drawdown-guard-cron"
    "finding" = "cefi-monotonicity-guard-alerting-2026-07-07"
  }
}

resource "google_cloud_scheduler_job" "cefi_drawdown_guard_daily" {
  project          = var.project_id
  region           = var.region
  name             = "${local.env_prefix}-cefi-drawdown-guard-daily"
  description      = "Daily 07:00 UTC trigger — CeFi cumulative-drawdown / thin-day-collapse guard (stdout only, alerting via CATALOGUE_SHRINK_BLOCKED covers the promotion-blocking half; this covers detection of collapses that still promote)."
  schedule         = "0 7 * * *"
  time_zone        = "UTC"
  attempt_deadline = "180s"

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${local.env_prefix}-cefi-drawdown-guard-daily:run"

    oauth_token {
      service_account_email = "cloud-scheduler@${var.project_id}.iam.gserviceaccount.com"
    }
  }

  depends_on = [module.cefi_drawdown_guard_job]
}
