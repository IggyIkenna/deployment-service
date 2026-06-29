# Publisher IAM for the persist-* Pub/Sub topics.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md
#
# GCE VMs that call LiveEventFacadeSink run as the default compute SA
# ({project_number}-compute@developer.gserviceaccount.com).  This project-level
# binding grants pubsub.topics.publish on ALL topics in the project — the same
# pattern used in features_service_events_pubsub.tf for the features-events topic.
#
# unified-trading-sa also gets publisher so it can publish from non-VM contexts
# (local dev, Cloud Run jobs) without requiring a separate per-topic grant.

resource "google_project_iam_member" "default_compute_sa_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "unified_trading_sa_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:unified-trading-sa@${var.project_id}.iam.gserviceaccount.com"
}
