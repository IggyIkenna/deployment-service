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

# uts-prd-sa / uts-test-sa (defined in ../bucket_iam_per_tier_sa.tf — a SEPARATE
# root module/state, hence the literal email here rather than a resource
# reference, mirroring the lifecycle-catalogue-regen grant above) run every VM
# backfill launched via the P2.2d tier-SA rollout (deployment-service@dd5f235,
# deployment-service@0ff5bc8). Their conditional storage.objectAdmin only covers
# Group A/B `-{prd,test}-` bucket-name-prefix families
# (bucket_iam_per_tier_sa.tf) -- this flat, non-tiered events-sink bucket was
# never in that set, so every such VM's GcsEventSink STARTED/STOPPED/
# PIPELINE_HEARTBEAT event uploads 403 and are dropped (Cloud Logging/run.log
# stdout still carries them, the structured event-log sink does not). This
# silently widens the DP-VM-002 "VM gone, no signal" blind spot for every
# tier-SA VM, independent of and in addition to the wrong-SA-selection bug
# dd5f235 fixed. Root-cause instance:
# instr-backfill-sports-pchk-0801102917-s-transfermarkt (agt-c0132b).
resource "google_storage_bucket_iam_member" "uts_prd_events_sink_writer" {
  bucket = "central-element-323112-events"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:uts-prd-sa@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "uts_test_events_sink_writer" {
  bucket = "central-element-323112-events"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:uts-test-sa@${var.project_id}.iam.gserviceaccount.com"
}
