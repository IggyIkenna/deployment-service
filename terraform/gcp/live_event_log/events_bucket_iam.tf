# IAM bindings on the live-event-log GCS sink bucket (central-element-323112-events),
# granted per-SA rather than via a project-level or bucket-authoritative resource so
# each grant is independently additive/reversible and does not risk clobbering the
# other bindings already on this bucket (legacyBucketOwner/legacyBucketReader project
# roles + the Pub/Sub delivery SA's objectCreator grant — see main.tf's trailing
# comment for that grant's own manual-apply history).
#
# lifecycle-catalogue-regen (defined in ../lifecycle_catalogue_scheduler.tf) needs
# storage.objects.create here so its CATALOGUE_SHRINK_BLOCKED/similar structured
# events reach the event-log sink instead of silently 403ing (Cloud Logging still
# carries the signal; the structured event-log sink did not). Source:
# sports_satellite_ao_dispatch_batch3_2026_07_25.md.
resource "google_storage_bucket_iam_member" "lifecycle_catalogue_regen_events_sink_writer" {
  bucket = "central-element-323112-events"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:lifecycle-catalogue-regen@${var.project_id}.iam.gserviceaccount.com"
}
