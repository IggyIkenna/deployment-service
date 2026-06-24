# Critical-service liveness — GCP-native uptime checks (operator 2026-06-23).
#
# THE GAP THIS CLOSES: the dp-* monitors + the out-of-band deadman cover the DATA
# PIPELINE (backfill VMs / consolidators / watchdog / the alerting SUBSCRIBER's
# backlog). NOTHING monitored the HTTP LIVENESS of the critical SERVICES + UIs —
# there were ZERO GCP uptime checks. If `deployment-api`, the `agent-orchestrator`
# central VM, or the `alerting-service` go down, the company loses deploy/launch,
# agent orchestration, or ALL alerting — silently.
#
# WHY GCP-native uptime → the deadman bedrock EMAIL channel (NOT Slack/alerting):
# the alerting-service is the SPOF for every DP_* + DEPLOYMENT_* alert. A liveness
# check that routed THROUGH it could never page "alerting-service is down". The
# GCP-managed external probers + the email channel are fully independent of our
# Slack relay + PubSub + the alerting-service — the same out-of-band principle the
# deadman uses. Reuses `google_monitoring_notification_channel.monitoring_deadman_email`
# (monitoring_deadman_scheduler.tf).
#
# Tracked follow-ups (need per-service specifics before they can be added without
# false-alarming): `deployment-dashboard` (no clean /health path — 404/timeout on
# the probes), `unified-trading-system-ui` (deployed off Cloud Run — needs its
# public URL), and folding these into the deployment-observability SSOT. See
# `plans/active/issues/dp_alert_flood_triage_and_monitor_fixes_2026_06_23.md`.

locals {
  # host -> {path, accepted extra status (e.g. an auth-gated service that returns
  # 403 when ALIVE), human label}. A timeout / 5xx is "down" for all of them.
  critical_service_uptime_targets = {
    "deployment-api" = {
      host         = "uts-shared-deployment-api-cldtjniqvq-an.a.run.app"
      path         = "/health"
      extra_status = 0 # /health returns 200 unauthenticated
      label        = "deployment-api (deploy/launch + subscriptions backend)"
    }
    "agent-orchestrator" = {
      host         = "api.agent-orchestrator.odum-research.com"
      path         = "/health"
      extra_status = 0 # /health returns 200
      label        = "agent-orchestrator central VM (fleet orchestration)"
    }
    "alerting-service" = {
      host         = "dp-alerting-subscriber-cldtjniqvq-an.a.run.app"
      path         = "/health"
      extra_status = 403 # auth-gated: a 403 means ALIVE-but-protected (down = timeout/5xx)
      label        = "alerting-service (SPOF for ALL DP_* + DEPLOYMENT_* alerts)"
    }
    "deployment-dashboard" = {
      host         = "deployment-dashboard-cldtjniqvq-an.a.run.app"
      path         = "/health" # added to deployment-ui/nginx.conf 2026-06-23 (was 404/timeout)
      extra_status = 0
      label        = "deployment-dashboard (deployment-ui devops + deploy pane)"
    }
    "trading-ui" = {
      host         = "odum-research.com" # unified-trading-system-ui via Firebase Hosting (app/health → 200)
      path         = "/health"
      extra_status = 0
      label        = "unified-trading-system-ui (company-facing trading/client site)"
    }
  }
}

resource "google_monitoring_uptime_check_config" "critical_service" {
  for_each = local.critical_service_uptime_targets

  display_name = "${local.env_prefix} ${each.key} liveness"
  project      = var.project_id
  timeout      = "10s"
  period       = "300s" # every 5 min from multiple regions

  http_check {
    path         = each.value.path
    port         = 443
    use_ssl      = true
    validate_ssl = true

    # 2xx is healthy everywhere; an auth-gated service (alerting-service) returns
    # 403 when ALIVE, so accept it too — a genuine outage is a timeout / 5xx, which
    # neither class matches → the check fails → the alert fires.
    accepted_response_status_codes {
      status_class = "STATUS_CLASS_2XX"
    }
    dynamic "accepted_response_status_codes" {
      for_each = each.value.extra_status > 0 ? [each.value.extra_status] : []
      content {
        status_value = accepted_response_status_codes.value
      }
    }
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = each.value.host
    }
  }
}

resource "google_monitoring_alert_policy" "critical_service_down" {
  for_each = local.critical_service_uptime_targets

  display_name = "${local.env_prefix} ${each.key} DOWN (uptime check failing)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "${each.key} uptime check failing"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"uptime_url\"",
        "metric.type = \"monitoring.googleapis.com/uptime_check/check_passed\"",
        "metric.label.check_id = \"${google_monitoring_uptime_check_config.critical_service[each.key].uptime_check_id}\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 1 # >1 failed check across the probe regions in the window
      duration        = "300s"

      aggregations {
        alignment_period     = "1200s"
        per_series_aligner   = "ALIGN_NEXT_OLDER"
        cross_series_reducer = "REDUCE_COUNT_FALSE"
        group_by_fields      = ["resource.label.host"]
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  # NOTE: notification_rate_limit is REJECTED by the API for metric-threshold
  # policies ("only log-based alert policies may specify a notification rate
  # limit") — these are uptime/metric policies, so rate-limiting is not available;
  # de-dup is handled by the 1200s alignment window + 300s duration above.
  alert_strategy {
    auto_close = "604800s" # 7 days
  }

  documentation {
    content   = "${each.value.label} is failing its GCP-native uptime check (host `${each.value.host}${each.value.path}`). This is an out-of-band liveness probe (GCP external probers + email, independent of the Slack relay / alerting-service), so it fires even when the alerting path itself is down. Check `gcloud run services describe` (Cloud Run) / the central VM + nginx (agent-orchestrator). Plan: dp_alert_flood_triage_and_monitor_fixes_2026_06_23.md § critical-service liveness."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "critical-service-liveness"
    "service" = each.key
  }
}
