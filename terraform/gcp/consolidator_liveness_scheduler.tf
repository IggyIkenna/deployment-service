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

# -------------------------------------------------------
# Cloud Monitoring log-based alert — pages on this watchdog's OWN exit(1)
# -------------------------------------------------------
# Issue: plans/active/issues/defi_consolidator_cron_left_paused_2026_07_15.md
#   The defi consolidator's triggering Cloud Scheduler cron was left PAUSED
#   for 13.5h (2026-07-14 22:25Z -> 2026-07-15 11:56Z). This watchdog job
#   correctly logged "N bucket(s) DOWN ... Container called exit(1)" every
#   2-min cycle for 12+ consecutive hours, and its app-level path
#   (log_event(CONSOLIDATOR_DOWN) -> lifecycle-events Pub/Sub ->
#   alerting-service's CONSOLIDATOR_DOWN rule -> PagerDuty/Telegram) is coded
#   and DID fire per the incident's own logs -- but nothing paged a human, and
#   there was ALSO zero GCP-native monitoring on this job's own exit code (no
#   `google_monitoring_alert_policy` anywhere in terraform/gcp/ keyed off
#   resource.type="cloud_run_job" for this job). So the ONE thing watching the
#   consolidator (this watchdog) had no independent watcher of its own -- a
#   violation of the "each layer is independent of the one it watches" design
#   in codex/05-infrastructure/deployment-observability.md § "Out-of-band
#   liveness". This alert closes that specific gap: a GCP-native backstop that
#   does not depend on the app's own Pub/Sub/alerting-service pipeline at all,
#   so a break anywhere in THAT pipeline (the still-open, unverified half of
#   this issue's P1 todo) can no longer leave this watchdog's own failures
#   silent.
#
# Channel reuse: the same out-of-band bedrock as the other real (non-toothless)
# alerts in this file's directory --
# `google_monitoring_notification_channel.monitoring_deadman_email`
# (monitoring_deadman_scheduler.tf) -- deliberately NOT a new channel, and
# deliberately NOT the empty-default `var.cf_audit_alert_notification_channels`
# pattern in cf_manifest_audit_scheduler.tf (that pattern ships an alert policy
# that pages no one until a separate workspace-level wiring step happens; this
# alert must actually page from day one).
resource "google_monitoring_alert_policy" "consolidator_liveness_watchdog_failed" {
  display_name = "${local.env_prefix} consolidator-liveness-watchdog — exit(1) (bucket DOWN)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "uts-prod-consolidator-liveness-watchdog logged ERROR (bucket DOWN or execution failed)"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_run_job"
        resource.labels.job_name="${module.consolidator_liveness_job.name}"
        severity="ERROR"
      EOT

      label_extractors = {
        "job_name" = "EXTRACT(resource.labels.job_name)"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  alert_strategy {
    notification_rate_limit {
      # The watchdog cron is */2 min; while a bucket stays DOWN it re-logs
      # ERROR every cycle. Page promptly on the first occurrence, then at
      # most once/hour while the condition persists (not every 2 min).
      period = "3600s"
    }
    auto_close = "604800s" # 7 days — auto-close stale incidents
  }

  documentation {
    content   = "The consolidator liveness watchdog (uts-prod-consolidator-liveness-watchdog) logged an ERROR -- either a manifest bucket's consolidator heartbeat is stale (CONSOLIDATOR_DOWN) or the watchdog's own Cloud Run execution failed. Check `gcloud scheduler jobs describe uts-prod-manifest-consolidator-market-data-{bucket}-cron --location=asia-northeast1` for a PAUSED cron first (the 2026-07-15 root cause -- a paused scheduler never self-recovers) before assuming a transient consolidator error. Then `gcloud run jobs executions list --job=uts-prod-consolidator-liveness-watchdog --region=asia-northeast1 --limit=5` for the watchdog's own recent runs. SSOTs: codex/05-infrastructure/manifest-consolidator-ssot.md, plans/active/issues/defi_consolidator_cron_left_paused_2026_07_15.md."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "consolidator-liveness-watchdog-alert"
  }
}
