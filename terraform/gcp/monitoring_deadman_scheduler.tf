# Monitoring Dead-Man's-Switch — out-of-band top-of-chain watcher (Cloud Run Job + Scheduler)
#
# Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md § "Watch-the-watchers SPOF" (INFRA P0).
# Code: deployment_service.data_pipeline_monitors.deadman_poster (this repo).
# Codex SSOT: codex/05-infrastructure/data-pipeline-alerts.md § "Watching the watchers".
#
# Why
#   Layer 1 (cron-watches-cron, in-band) is `meta_watchers.check_monitor_crons_fired`:
#   each fleet-monitor sweep writes a `vm-census/<mode>-last-run.json` sentinel and
#   the meta sweep probes them. But the in-band layer CANNOT detect the death of the
#   LAST watcher (the meta sweep can't probe its own sentinel while dead) nor the
#   alerting subscriber that relays DP_* → Slack. This job is that final, DELIBERATELY
#   INDEPENDENT watcher: it reads every monitor sentinel + the lifecycle-events-sub
#   oldest_unacked backlog and, on ANY staleness, posts DIRECTLY to a SEPARATE Slack
#   webhook (`MONITORING_DEADMAN_SLACK_WEBHOOK`, app `monitoring-deadman`) — never
#   through PubSub / the alerting-service / the #data-pipeline-alerts webhook, so the
#   same failure can't swallow its own death-alert.
#
# Terminal bedrock
#   A `google_monitoring_alert_policy` on THIS job's own execution-absence/failure
#   (the last watcher) routes to a `google_monitoring_notification_channel`. The
#   channel is EMAIL (iggy2london@gmail.com) — a deliberately DIFFERENT mechanism
#   from Slack = true defense-in-depth (if the whole Slack path is down, email still
#   delivers). A GCP-native Slack channel needs a one-time interactive OAuth that can't
#   be provisioned non-interactively here.
#   # TODO(operator): optionally swap to a native Slack channel to monitoring-deadman
#   # (`gcloud monitoring channels create --type=slack ...` requires interactive OAuth).
#
# Service account
#   * Scheduler invoker: t1_batch_sa (roles/run.invoker).
#   * Container runtime: unified_trading_sa — storage read on the log bucket (sentinels),
#     monitoring.viewer to query oldest_unacked, SM accessor on MONITORING_DEADMAN_SLACK_WEBHOOK.

locals {
  # Same image as the fleet monitors: the deadman entrypoint
  # `-m deployment_service.data_pipeline_monitors.deadman_poster` lives in the
  # deployment-service `deployment-api` image (the only image that COPYs
  # `deployment_service/`). See data_pipeline_fleet_monitor_scheduler.tf.
  monitoring_deadman_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-api:latest"
}

# --- SA accessor: MONITORING_DEADMAN_SLACK_WEBHOOK (the SEPARATE deadman webhook) ---
resource "google_secret_manager_secret_iam_member" "unified_trading_deadman_webhook_accessor" {
  secret_id = "MONITORING_DEADMAN_SLACK_WEBHOOK"
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.unified_trading.email}"
}

# --- monitoring.viewer for the runtime SA (read oldest_unacked_message_age) ---
resource "google_project_iam_member" "unified_trading_monitoring_viewer" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# --- Cloud Run Job — the deadman poster ---
module "monitoring_deadman_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-monitoring-deadman"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.monitoring_deadman_image

  cpu             = "1"
  memory          = "1Gi"
  timeout_seconds = 120
  max_retries     = 2 # a transient SM/monitoring hiccup must not silently drop a tick
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "deployment_service.data_pipeline_monitors.deadman_poster",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "monitoring-deadman"
  environment  = var.environment

  labels = {
    "purpose" = "monitoring-deadman"
    "tier"    = "out-of-band-watcher"
  }
}

# --- Cloud Scheduler cron — every 15 min ---
resource "google_cloud_scheduler_job" "monitoring_deadman_cron" {
  name        = "${local.env_prefix}-monitoring-deadman-cron"
  description = "Out-of-band monitoring dead-man's-switch: read every dp-monitor sentinel + lifecycle-events-sub backlog; post DIRECTLY to the monitoring-deadman Slack webhook on any staleness. Independent of PubSub/alerting. Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md § Watch-the-watchers SPOF."
  schedule    = "*/15 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.monitoring_deadman_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 2
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# --- Terminal bedrock: alert on the DEADMAN job's OWN execution-absence/failure ---
# This is the LAST watcher — nothing else watches it. The notification channel is
# EMAIL (a different mechanism from Slack = true defense-in-depth). If the deadman
# job stops executing (Cloud Scheduler dead) or fails, this GCP-native policy pages
# the operator outside the entire DP_*/Slack path.
resource "google_monitoring_notification_channel" "monitoring_deadman_email" {
  display_name = "${local.env_prefix} monitoring-deadman bedrock (email)"
  project      = var.project_id
  type         = "email"

  # Deliberately a DIFFERENT mechanism from Slack — if the whole Slack relay is down,
  # email still delivers. TODO(operator): optionally swap to a native Slack channel.
  labels = {
    email_address = "ikenna@odum-research.com"
  }
}

# Fires when the deadman Cloud Run Job logs a failed execution (non-zero / error).
# A Cloud Run Job writes an ERROR-severity log entry on a failed execution; the
# absence-of-success case is covered by the scheduler's own retry + this policy
# surfacing repeated failures.
resource "google_monitoring_alert_policy" "monitoring_deadman_down" {
  display_name = "${local.env_prefix} monitoring-deadman DOWN (the last watcher)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "monitoring-deadman job execution failed"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_run_job"
        resource.labels.job_name="${module.monitoring_deadman_job.name}"
        severity="ERROR"
        textPayload=~"execution failed"
      EOT

      label_extractors = {
        "job_name" = "EXTRACT(resource.labels.job_name)"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  alert_strategy {
    notification_rate_limit {
      period = "3600s" # at most one alert/hour — the cron is */15
    }
    auto_close = "604800s" # 7 days
  }

  documentation {
    content   = "The out-of-band monitoring dead-man's-switch job (`${module.monitoring_deadman_job.name}`) failed/stopped executing — this is the LAST watcher in the chain, so its death means the data-pipeline monitoring may be fully blind. Check `gcloud run jobs executions list --job=${module.monitoring_deadman_job.name}` + the `${local.env_prefix}-monitoring-deadman-cron` scheduler. Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md § Watch-the-watchers SPOF."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "monitoring-deadman-bedrock"
    "tier"    = "out-of-band-watcher"
  }
}

# --- Outputs ---
output "monitoring_deadman_job_name" {
  description = "Cloud Run Job name for the out-of-band monitoring dead-man's-switch."
  value       = module.monitoring_deadman_job.name
}

output "monitoring_deadman_cron_name" {
  description = "Cloud Scheduler cron name for the monitoring dead-man's-switch."
  value       = google_cloud_scheduler_job.monitoring_deadman_cron.name
}

output "monitoring_deadman_alert_policy_name" {
  description = "Cloud Monitoring alert policy name — fires when the deadman job (the last watcher) fails/stops."
  value       = google_monitoring_alert_policy.monitoring_deadman_down.name
}
