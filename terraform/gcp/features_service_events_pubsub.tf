# features-service-events PubSub topic + IAM
#
# Plan: prediction_venue_perps_and_live_clob_depth_2026_06_20.md (OPS P1)
#
# WHY this file exists
#   The `features-service-events` PubSub topic was created manually 2026-06-24
#   to fix a PERMISSION_DENIED crash in UTL ServiceBootstrap when the
#   prediction arb detector VM started (log_event("STARTED") publishes via
#   PubSubEventSink to f"{service_name}-events" = "features-service-events").
#   Without the topic the SDK raises TopicNotFound; without publisher IAM on
#   the VM's runtime SA it raises PERMISSION_DENIED — both crash the service
#   before data collection begins.
#
#   Two compute SAs need publisher rights:
#   1. Default compute SA — used by manually-launched VMs (launch-prediction-arb-detector.sh,
#      launch-prediction-features-vm.sh, launch-features-backfill-vm.sh, etc.) which
#      use --scopes=cloud-platform without --service-account, so they run as the
#      project default compute SA.
#   2. t1_batch SA — used by Cloud-Scheduler-triggered Cloud Run Jobs
#      (features-cross-instrument-t1-recon, etc.) that run features-service code.
#      Mirrors the existing t1_batch_market_tick_events_publisher grant.
#
# ADOPTION (no recreate): the topic already exists. The import block below
# adopts it into Terraform state on the first `tofu apply`. After that the
# import block is inert (safe to leave or remove).

resource "google_pubsub_topic" "features_service_events" {
  name    = "features-service-events"
  project = var.project_id
  labels  = merge(local.common_labels, { "purpose" = "service-events", "service" = "features-service" })
}

# Default compute SA — manually-launched GCE VMs (--scopes=cloud-platform, no --service-account)
resource "google_pubsub_topic_iam_member" "default_compute_sa_features_events_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.features_service_events.id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# t1_batch SA — Cloud-Scheduler-triggered Cloud Run Jobs running features-service
# Mirrors t1_batch_market_tick_events_publisher in qg_snapshot_scheduler.tf.
resource "google_pubsub_topic_iam_member" "t1_batch_features_events_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.features_service_events.id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

# ── Adoption import (inert after the first adopting apply) ──
import {
  to = google_pubsub_topic.features_service_events
  id = "projects/central-element-323112/topics/features-service-events"
}
