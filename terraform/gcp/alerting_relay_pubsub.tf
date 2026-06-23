# Alerting relay PubSub substrate — lifecycle-events topic + sub + subscriber IAM.
#
# Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md +
#   plans/active/issues/dp_event_pubsub_delivery_gap_2026_06_22.md (item c — durability).
#
# WHY this file exists
#   UTL `log_event` (mode=live) publishes ALL data-pipeline alerts (the `DP_*`
#   family) + `CONSOLIDATOR_DOWN` to the `lifecycle-events` topic (the
#   `PubSubEventSink` default — unified-trading-library/event_sink.py). The
#   alerting-service `alert_subscriber` consumes `lifecycle-events-sub` →
#   `route_event` → `#data-pipeline-alerts`. That topic + subscription + the
#   alerting VM's compute-SA `roles/pubsub.subscriber` grant were HAND-CREATED
#   during the 2026-06-22 relay bring-up (the gap doc) — they survive VM
#   relaunches (subscriptions are persistent) but a fresh-project bootstrap or a
#   broad `terraform apply` would NOT recreate them, silently breaking the whole
#   data-pipeline → Slack relay. This file codifies them so they are durable.
#
#   Same for `defi_data_quality_alerts`: the TOPIC is declared in
#   subgraph_health_probe_scheduler.tf, but its SUBSCRIPTION + subscriber IAM
#   were hand-created — codified here alongside lifecycle-events.
#
# ADOPTION (no recreate): the resources below already exist in GCP. Run
#   `tofu apply` with the matching `import` blocks (below) ONCE so OpenTofu
#   adopts them into state instead of trying to create-then-conflict. After the
#   first adopting apply the import blocks are inert (safe to leave or remove).
#
# The alerting VM runs under the DEFAULT compute SA
# (`{project_number}-compute@developer.gserviceaccount.com`), so the subscriber
# grant targets that SA — mirroring the existing margin-events binding the
# subscriber already relies on.

locals {
  # The alerting subscriber VM (alerting-quietness-*) runs under the default
  # compute service account; it pulls these subscriptions.
  alerting_subscriber_sa = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ── lifecycle-events: the UTL log_event default sink topic (DP_* + CONSOLIDATOR_DOWN) ──
resource "google_pubsub_topic" "lifecycle_events" {
  name    = "lifecycle-events"
  project = var.project_id
  labels  = merge(local.common_labels, { "purpose" = "event-bus", "relay" = "data-pipeline-alerts" })
}

resource "google_pubsub_subscription" "lifecycle_events_sub" {
  name    = "lifecycle-events-sub"
  topic   = google_pubsub_topic.lifecycle_events.id
  project = var.project_id

  # 24h retention — matches the hand-created sub; generous so a relaunching
  # subscriber re-pulls anything published during a brief gap.
  message_retention_duration = "86400s"
  retain_acked_messages      = false
  ack_deadline_seconds       = 30

  expiration_policy {
    ttl = "" # never expire
  }

  labels = merge(local.common_labels, { "purpose" = "event-bus", "relay" = "data-pipeline-alerts" })
}

resource "google_pubsub_subscription_iam_member" "lifecycle_events_sub_subscriber" {
  subscription = google_pubsub_subscription.lifecycle_events_sub.name
  project      = var.project_id
  role         = "roles/pubsub.subscriber"
  member       = local.alerting_subscriber_sa
}

# ── defi_data_quality_alerts: subscription + subscriber IAM (topic is in subgraph_health_probe_scheduler.tf) ──
resource "google_pubsub_subscription" "defi_data_quality_alerts_sub" {
  name    = "defi_data_quality_alerts"
  topic   = google_pubsub_topic.defi_data_quality_alerts.id
  project = var.project_id

  message_retention_duration = "86400s"
  retain_acked_messages      = false
  ack_deadline_seconds       = 30

  expiration_policy {
    ttl = ""
  }

  labels = merge(local.common_labels, { "purpose" = "event-bus", "relay" = "data-pipeline-alerts" })
}

resource "google_pubsub_subscription_iam_member" "defi_data_quality_alerts_subscriber" {
  subscription = google_pubsub_subscription.defi_data_quality_alerts_sub.name
  project      = var.project_id
  role         = "roles/pubsub.subscriber"
  member       = local.alerting_subscriber_sa
}

# ── Adoption imports (inert after the first adopting apply) ──
import {
  to = google_pubsub_topic.lifecycle_events
  id = "projects/central-element-323112/topics/lifecycle-events"
}

import {
  to = google_pubsub_subscription.lifecycle_events_sub
  id = "projects/central-element-323112/subscriptions/lifecycle-events-sub"
}

import {
  to = google_pubsub_subscription.defi_data_quality_alerts_sub
  id = "projects/central-element-323112/subscriptions/defi_data_quality_alerts"
}

output "alerting_relay_subscriptions" {
  description = "PubSub subscriptions the alerting subscriber consumes for the data-pipeline → Slack relay."
  value = [
    google_pubsub_subscription.lifecycle_events_sub.name,
    google_pubsub_subscription.defi_data_quality_alerts_sub.name,
  ]
}
