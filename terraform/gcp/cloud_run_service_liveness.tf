# Generic Cloud Run Service liveness / OOM registry + alert policies.
#
# THE GAP THIS CLOSES: critical_service_uptime.tf monitors 5 hand-picked HTTP-liveness
# services (deployment-api, agent-orchestrator, alerting-service, dashboard, trading-ui).
# The 2026-08-07 infra-health audit confirmed the broader Cloud Run Service estate had
# zero per-target alerting — 3 services had been silently broken for 9.5–19 months.
#
# DETECTION LAYERS (3 alert policies per applicable service):
#
#   (a) Memory utilization > 85% sustained 5m — OOM risk BEFORE the crash (early warning,
#       same pattern as deployment_api_memory_alert.tf). Applies when `memory_alert=true`.
#
#   (b) Container restart count delta >= 2 in a 15m window — crash-loop detection (the
#       container is crashing on startup or OOM-killed repeatedly). Applies when
#       `restart_alert=true`.
#
#   (c) Active instance count below the expected minimum for 30m — never-started /
#       completely-offline detection. Applies when `min_instance_count > 0`; only for
#       services with a non-zero minScale that should always have warm instances.
#       `evaluation_missing_data = EVALUATION_MISSING_DATA_ACTIVE` treats the absence of
#       metric data (no containers ever launched) the same as a 0-instance report.
#
# HOW TO ADD A SERVICE: append an entry to `cloud_run_service_liveness_targets` in the
# locals block below, then run `tofu plan` + `tofu apply`.
#
# All alerts fire to `monitoring_deadman_email` (defined in monitoring_deadman_scheduler.tf)
# — the same out-of-band bedrock as critical_service_uptime.tf and deployment_api_memory_alert.tf,
# so pages are independent of the Slack relay / alerting-service.
#
# Issue: plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)

locals {
  # Cloud Run Service liveness registry.
  # Each entry:
  #   service_name       = Cloud Run service name (used as resource.label.service_name filter)
  #   min_instance_count = Expected minimum active instances (0 = scale-to-zero OK, no instance alert)
  #   memory_alert       = emit a memory-utilization > 85% alert for this service
  #   restart_alert      = emit a crash-loop (restart-count delta >= 2 / 15m) alert
  cloud_run_service_liveness_targets = {
    # Crash-looping since 2025-10-20 (resolved to wrong bucket; finding #1 in the audit).
    # Scales to 0 when idle so no instance-count alert (min_instance_count = 0).
    "market-data-query-service" = {
      service_name       = "market-data-query-service"
      min_instance_count = 0
      memory_alert       = true
      restart_alert      = true
    }
    # Never started — 19 months with 0 running instances despite minScale=2 (finding #11).
    # Instance-count alert is the primary signal here; memory/restart alerts added for
    # completeness (if a revision does start, it might OOM or crash before stabilising).
    "central-market-data-tardis-loader" = {
      service_name       = "central-market-data-tardis-loader"
      min_instance_count = 2
      memory_alert       = true
      restart_alert      = true
    }
    # OOM at 32Gi / 8vCPU ceiling (finding #3). No minScale guarantee known; scale-to-zero OK.
    "uts-prod-data-status-rollup-svc" = {
      service_name       = "uts-prod-data-status-rollup-svc"
      min_instance_count = 0
      memory_alert       = true
      restart_alert      = true
    }
  }

  # Derived sub-maps keyed by the same identifier — used in for_each below.
  _crs_memory_targets = {
    for k, v in local.cloud_run_service_liveness_targets : k => v if v.memory_alert
  }
  _crs_restart_targets = {
    for k, v in local.cloud_run_service_liveness_targets : k => v if v.restart_alert
  }
  _crs_instance_targets = {
    for k, v in local.cloud_run_service_liveness_targets : k => v if v.min_instance_count > 0
  }
}

# (a) Memory utilization > 85% for 5 minutes — OOM risk / early warning.
# Pattern mirrors deployment_api_memory_alert.tf.
resource "google_monitoring_alert_policy" "cloud_run_service_memory_high" {
  for_each = local._crs_memory_targets

  display_name = "${local.env_prefix} ${each.key} memory HIGH (OOM risk)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "${each.value.service_name} container memory > 85% for 5m"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.label.service_name = \"${each.value.service_name}\"",
        "metric.type = \"run.googleapis.com/container/memory/utilizations\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0.85 # utilizations is a 0.0-1.0 fraction of the container memory limit
      duration        = "300s"

      aggregations {
        alignment_period     = "60s"
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

  alert_strategy {
    auto_close = "604800s" # 7 days
  }

  documentation {
    content   = "${each.value.service_name} Cloud Run container memory utilization has exceeded 85% of its limit for 5+ minutes — OOM crash imminent. Check `gcloud run services describe ${each.value.service_name} --region=${var.region}` for the current memory limit, and Cloud Logging for OOM-kill entries (`gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=${each.value.service_name}' --limit=50 --format=json`). Raise the memory limit or investigate a leak before this crash-loops. Issue: infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "cloud-run-service-liveness"
    "service" = each.key
    "alert"   = "memory-high"
  }
}

# (b) Container restart count delta >= 2 in 15 minutes — crash-loop detection.
# Uses ALIGN_DELTA on the cumulative restart_count counter to measure restarts in the window.
# Fires immediately (duration="0s") once 2+ restarts occur in any 15m window.
resource "google_monitoring_alert_policy" "cloud_run_service_crash_loop" {
  for_each = local._crs_restart_targets

  display_name = "${local.env_prefix} ${each.key} crash-looping (2+ restarts / 15m)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "${each.value.service_name} container restarts >= 2 in 15m"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.label.service_name = \"${each.value.service_name}\"",
        "metric.type = \"run.googleapis.com/container/restart_count\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 1 # COMPARISON_GT 1 → fires on 2+ restarts (threshold is exclusive)
      duration        = "0s" # fire as soon as the 15m window crosses the threshold

      aggregations {
        alignment_period     = "900s" # 15-minute window for the delta sum
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = "${each.value.service_name} has restarted 2+ times in the last 15 minutes — the container is crash-looping. Check Cloud Logging for crash cause: `gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=${each.value.service_name} AND severity>=ERROR' --limit=50 --format=json`. Check `gcloud run revisions list --service=${each.value.service_name} --region=${var.region}` for failed revisions. Issue: infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "cloud-run-service-liveness"
    "service" = each.key
    "alert"   = "crash-loop"
  }
}

# (c) Active instance count below expected minimum for 30 minutes — never-started /
# completely-offline detection. Only for services with min_instance_count > 0 (those
# that have a non-zero minScale and should always have warm instances).
#
# evaluation_missing_data = EVALUATION_MISSING_DATA_ACTIVE: if the service has never
# launched (no metric time series at all), treat it as a failed threshold, firing
# the alert. This is the critical case for central-market-data-tardis-loader
# (minScale=2, 0 running instances for 19 months).
resource "google_monitoring_alert_policy" "cloud_run_service_instance_zero" {
  for_each = local._crs_instance_targets

  display_name = "${local.env_prefix} ${each.key} OFFLINE (0 active instances, expected >= ${each.value.min_instance_count})"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "${each.value.service_name} active instances < ${each.value.min_instance_count} for 30m"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.label.service_name = \"${each.value.service_name}\"",
        "metric.type = \"run.googleapis.com/container/instance_count\"",
        "metric.label.state = \"active\"",
      ])
      comparison              = "COMPARISON_LT"
      threshold_value         = each.value.min_instance_count
      duration                = "1800s" # 30 minutes — avoids cold-start false alarms
      evaluation_missing_data = "EVALUATION_MISSING_DATA_ACTIVE" # absent data = 0 instances = fire

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_MIN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = "${each.value.service_name} has had fewer than ${each.value.min_instance_count} active instances for 30+ minutes — this service has minScale=${each.value.min_instance_count} and should always have at least that many warm containers. No metric data triggers this alert too (the service may have never launched). Check: `gcloud run services describe ${each.value.service_name} --region=${var.region}` for deployment status and `gcloud run revisions list --service=${each.value.service_name} --region=${var.region}` for revision health. Issue: infra_health_audit_alert_coverage_gaps_2026_08_07.md § (A)."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "cloud-run-service-liveness"
    "service" = each.key
    "alert"   = "instance-zero"
  }
}
