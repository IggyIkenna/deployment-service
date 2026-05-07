# Client Reporting — Cloud Run Jobs + Cloud Scheduler
#
# Two jobs:
#   1. Hourly update — balance, equity curve, positions, trades, orders
#   2. On-demand backfill — triggered via API or gcloud CLI for new clients
#
# The update job reads credentials-registry.yaml at runtime, so new clients
# are picked up automatically — no redeployment needed.
#
# Cloud Run Jobs are created at runtime by backends/cloud_run.py (see main.tf NOTE).
# This file only provisions the Cloud Scheduler triggers.

# -------------------------------------------------------
# Hourly Update — runs every hour at :05
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "client_reporting_hourly_update" {
  name        = "${local.env_prefix}-client-reporting-hourly-update"
  description = "Hourly client reporting update — balance, equity, positions, trades, orders for all active clients"
  schedule    = "5 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${local.env_prefix}-client-reporting-update:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 2
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 3
  }
}

# -------------------------------------------------------
# Daily Full Snapshot — runs once at 00:15 UTC (after market close)
# Deeper data pull: full order history, extended trade window, MiFID fields
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "client_reporting_daily_snapshot" {
  name        = "${local.env_prefix}-client-reporting-daily-snapshot"
  description = "Daily full snapshot — extended trade/order history, MiFID data, equity curve update"
  schedule    = "15 0 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${local.env_prefix}-client-reporting-daily-snapshot:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "600s"
    max_doublings        = 3
  }
}
