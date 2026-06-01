# Manifest Consolidator Liveness Watchdog — Cloud Run Job + Scheduler cron
#
# Plan: manifest_consolidator_liveness_health_2026_06_01
# Code: unified-trading-library@3732ffaa
#   (`unified_trading_library.monitors.consolidator_liveness`)
#
# Background
#   The manifest consolidator (manifest_consolidator_scheduler.tf) heartbeats
#   every cycle, including no-op cycles — it touches the canonical
#   `_index/availability_index.parquet` mtime + emits MANIFEST_CONSOLIDATED.
#   Nothing watched for the heartbeat's ABSENCE. This watchdog does: per
#   bucket it reads the heartbeat age and emits CONSOLIDATOR_DOWN (ERROR) when
#   a bucket misses > N consolidation cycles (default 5 × 60s = 300s),
#   CONSOLIDATOR_RECOVERED when it returns. The CLI exits non-zero if any
#   bucket is DOWN, so the Cloud Run execution surfaces red as a second signal
#   alongside the alert event.
#
# Topology
#   A SINGLE job (not one-per-bucket like the consolidator) — the watchdog CLI
#   accepts every bucket in one `--buckets a,b,c` arg, and each check is a
#   cheap blob-metadata read. One cron, `*/2 * * * *` (half the consolidator
#   cadence — liveness doesn't need per-minute granularity).
#
# Image
#   Reuses `market-tick-data-service:latest` (UTL bundled as a dep). Requires
#   the image to be rebuilt at >= UTL 3732ffaa so the consolidator_liveness
#   module is present.
#
# Service account
#   * Scheduler invoker: `t1_batch_sa` (roles/run.invoker).
#   * Container runtime: `unified_trading_sa` (storage read on all manifest
#     buckets — needs blob.updated + list_blobs to compute heartbeat age).

locals {
  # Every bucket the consolidator covers (env-tiered + legacy + extended
  # Group B) is also watched for liveness. Dedup since legacy/env-tiered maps
  # can overlap conceptually; distinct() keeps the --buckets arg minimal.
  consolidator_liveness_buckets = distinct(concat(
    values(local.manifest_consolidator_buckets),
    values(local.manifest_consolidator_buckets_extended),
  ))
}

module "consolidator_liveness_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-consolidator-liveness-watchdog"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  # Lightweight — only blob-metadata reads + a list-blobs existence check per
  # bucket. No data download, no merge.
  cpu             = "1"
  memory          = "2Gi"
  timeout_seconds = 300
  max_retries     = 0 # exit-1-on-DOWN is the intended signal; next cron cycle re-checks
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "unified_trading_library.monitors.consolidator_liveness",
    "--buckets", join(",", local.consolidator_liveness_buckets),
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "consolidator-liveness-watchdog"
  environment  = var.environment

  labels = {
    "purpose" = "consolidator-liveness-watchdog"
  }
}

resource "google_cloud_scheduler_job" "consolidator_liveness_cron" {
  name        = "${local.env_prefix}-consolidator-liveness-watchdog-cron"
  description = "Watch manifest consolidator heartbeats across all buckets; emit CONSOLIDATOR_DOWN on missed cycles."
  schedule    = "*/2 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.consolidator_liveness_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries — the next cycle re-checks (the watchdog is stateless per run).
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

output "consolidator_liveness_job_name" {
  description = "Cloud Run Job name for the consolidator liveness watchdog (for manual ad-hoc fires)."
  value       = module.consolidator_liveness_job.name
}

output "consolidator_liveness_cron_name" {
  description = "Cloud Scheduler cron name for the consolidator liveness watchdog."
  value       = google_cloud_scheduler_job.consolidator_liveness_cron.name
}
