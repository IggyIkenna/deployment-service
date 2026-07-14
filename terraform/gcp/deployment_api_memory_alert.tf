# deployment-api Cloud Run memory alert — D6 alerting (operator 2026-07-14).
#
# WHY: `uts-shared-deployment-api` (this project's deploy/launch + subscriptions
# backend, see critical_service_uptime.tf) OOM-crash-looped TWICE this week
# (2026-07-13 and 2026-07-14) and had ZERO memory alerting — the uptime check in
# critical_service_uptime.tf only fires AFTER the service is already down
# (post-crash-loop). This closes that gap one step earlier: fire while memory
# pressure is building so the NEXT regression pages before it crash-loops, not
# after.
#
# Channel reuse: same out-of-band bedrock as this service's existing uptime alert
# (`google_monitoring_alert_policy.critical_service_down["deployment-api"]` in
# critical_service_uptime.tf) —
# `google_monitoring_notification_channel.monitoring_deadman_email`
# (monitoring_deadman_scheduler.tf). Deliberately NOT a new channel: this keeps
# every deployment-api alert on the one bedrock email path that is independent of
# the Slack relay / alerting-service (see that file's header for the full
# rationale).

resource "google_monitoring_alert_policy" "deployment_api_memory_high" {
  display_name = "${local.env_prefix} deployment-api memory utilization HIGH (OOM risk)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "uts-shared-deployment-api container memory > 85% for 5m"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.label.service_name = \"uts-shared-deployment-api\"",
        "metric.type = \"run.googleapis.com/container/memory/utilizations\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0.85 # utilizations is a 0.0-1.0 fraction of the container memory limit
      duration        = "300s"

      aggregations {
        alignment_period     = "60s" # matches Cloud Run's own per-instance sampling interval
        per_series_aligner   = "ALIGN_PERCENTILE_99"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.service_name"]
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  # NOTE: notification_rate_limit is REJECTED by the API for metric-threshold
  # policies (same constraint documented in critical_service_uptime.tf) — de-dup
  # is handled by the 60s alignment period + 300s sustained duration above.
  alert_strategy {
    auto_close = "604800s" # 7 days
  }

  documentation {
    content   = "uts-shared-deployment-api's Cloud Run container memory utilization has exceeded 85% of its limit for 5+ minutes — this service OOM-crash-looped on 2026-07-13 and 2026-07-14. Check `gcloud run services describe uts-shared-deployment-api --region=asia-northeast1` for the current memory limit + recent revisions, and Cloud Logging for OOM-kill entries. Raise the Cloud Run memory limit (`scripts/cloud-run/deploy-shared.sh`) or investigate a leak/regression before this crash-loops again."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "cloud-run-memory-alert"
    "service" = "deployment-api"
  }
}
