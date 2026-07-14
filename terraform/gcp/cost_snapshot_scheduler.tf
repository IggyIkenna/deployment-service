# Cost-observability snapshot — 12h cron → in-service /api/costs/snapshot-run
#
# Plan: deployment_api_cache_oom_and_ui_latency_remediation_2026_07_13.md § 4b
# SSOT:  codex/05-infrastructure/billing-cost-observability.md
#
# Background
#   The deployment-ui /ops/costs page used to re-query BigQuery/Athena on every
#   request (55s cold + a metered scan; the +1.9GB residency came from caching the
#   raw fact rows). This cron drives the offline-snapshot model: every 12h a worker
#   scans each cloud ONCE and writes a per-cloud parquet to
#   ``gs://{state-bucket}/cost-snapshots/{cloud}.parquet`` (~3MB GCP); each API
#   instance downloads it and answers every view with a DuckDB SQL aggregation
#   locally — no per-request cloud scan, no full-table Python materialization.
#
# Runtime — IN the gen1 deployment-api SERVICE (NOT a Cloud Run Job)
#   Same reason as the data-status rollup (see data_status_rollup_scheduler.tf):
#   Cloud Run Jobs are gen2-only and native pyarrow compute can crash there. The
#   deployment-api service is gen1, so the scheduler POSTs its
#   ``/api/costs/snapshot-run`` endpoint (routes/costs.py). The cost compute is
#   light (~60s BQ query + a small parquet write), so it runs comfortably inside a
#   normal request thread (asyncio.to_thread) once per 12h.
#
# Storage
#   NO dedicated bucket — reuses the deployment-api state bucket
#   (``unified-deployment-state-{project}``, config.effective_state_bucket) under a
#   ``cost-snapshots/`` prefix. The runtime SA already has objectAdmin there.
#
# AWS slice
#   ``aws.parquet`` only populates once the service has AWS creds
#   (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from a Secret Manager secret, e.g.
#   ``*-worker-aws-creds`` — must carry Athena + CUR-S3 read perms). Absent creds,
#   the AWS cloud is skipped (honest degradation); GCP + GitHub populate regardless.
#   This is the same credential gap that keeps AWS cost data off the deployed API
#   today (the API works locally only via ~/.aws/credentials).

locals {
  # Snapshot every 12h; billing lags ~2 days so ≤12h staleness on the "today so far"
  # figure is immaterial (§ 4b decision 2). Offset from :00 so it doesn't collide
  # with the top-of-hour cron fleet.
  cost_snapshot_schedule = "7 */12 * * *"
  cost_snapshot_run_uri  = "https://uts-shared-deployment-api-${var.project_number}.${var.region}.run.app/api/costs/snapshot-run"
}

resource "google_cloud_scheduler_job" "cost_snapshot_cron" {
  name        = "${local.env_prefix}-cost-snapshot-cron"
  description = "Trigger the in-service cost-snapshot endpoint every 12h; deployment-ui /ops/costs reads the GCS parquet snapshot instead of querying BigQuery/Athena per request."
  schedule    = local.cost_snapshot_schedule
  time_zone   = "UTC"
  region      = var.region

  # The BQ scan + parquet write is ~60s; give generous headroom.
  attempt_deadline = "300s"

  http_target {
    http_method = "POST"
    uri         = local.cost_snapshot_run_uri

    # The deployment-api service is public (allUsers run.invoker) with app-auth
    # disabled today, so an OIDC token is not strictly required — but include one
    # (SA already holds run.invoker) as defence-in-depth and so this keeps working
    # if the service is later locked to IAM-private. If app-level X-API-Key auth is
    # re-enabled, add a `headers = { "X-API-Key" = <secret> }` block instead.
    oidc_token {
      service_account_email = local.t1_service_account_email
      audience              = local.cost_snapshot_run_uri
    }
  }

  # Idempotent (overwrite-by-name); the next 12h tick recomputes on failure, so no
  # retry-storm on a genuinely broken cloud.
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

output "cost_snapshot_cron_name" {
  description = "Cloud Scheduler cron that drives the 12h cost-snapshot refresh."
  value       = google_cloud_scheduler_job.cost_snapshot_cron.name
}
