# Catalogue regeneration — nightly Cloud Scheduler trigger
#
# Regenerates the four catalogue artefacts on GCS:
#   gs://strategy-store-cefi-central-element-323112/catalogue/{
#     envelope.md, envelope.json, availability.json, strategy_instruments.json
#   }
#
# Source scripts (live in unified-api-contracts/scripts/):
#   - enumerate_envelope.py  (envelope.md + envelope.json)
#   - enumerate_availability.py  (availability.json)
#   - enumerate_strategy_instruments.py --with-real-instruments
#       (joins envelope with instruments-store-{cefi,defi,sports,prediction}
#        parquet to produce ~17 MB JSON of real instrument keys per slot)
#
# The Cloud Run Job declaration is a follow-up — for now this scheduler entry
# documents the intended trigger and points at a job target that should be
# created via the catalogue-regen container-job module.
#
# Trigger time: 04:30 UTC daily — after the FAST-phase instruments-service
# refresh (00:00 UTC) so the parquet rolls are current; before strategy-service
# kicks off at 04:00 UTC so consumers downstream of the catalogue pick up the
# fresh artefacts on their own next run.
#
# References:
#   codex/09-strategy/architecture-v2/instruments-resolver-architecture.md
#   plans/active/dart_ui_strategy_filtering_and_onboarding_2026_04_24.plan.md (Phase 11 N2)

resource "google_service_account" "catalogue_regen" {
  project      = var.project_id
  account_id   = "catalogue-regen"
  display_name = "Catalogue Regen — nightly artefact rebuild"
}

resource "google_storage_bucket_iam_member" "catalogue_regen_strategy_store_writer" {
  bucket = "strategy-store-cefi-central-element-323112"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.catalogue_regen.email}"
}

# Read access to all instruments-store-* buckets (real parquet resolver)
resource "google_storage_bucket_iam_member" "catalogue_regen_instruments_reader" {
  for_each = toset([
    "instruments-store-cefi-central-element-323112",
    "instruments-store-defi-central-element-323112",
    "instruments-store-sports-central-element-323112",
    "instruments-store-prediction-central-element-323112",
  ])
  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.catalogue_regen.email}"
}

# UI Cloud Run services need read access to catalogue/* on the strategy bucket
# so the /api/catalogue/envelope?file=... GCS proxy route works on prod/staging
# (currently uses ADC locally; deploy-time SA needs explicit grant).
# odum-portal + visualizer-ui both run on the default compute SA today —
# narrow grant to only the catalogue/ prefix would require an Object Reader
# role binding scoped via condition, but storage.objectViewer at bucket level
# is acceptable since the rest of the bucket is non-sensitive catalogue data.
resource "google_storage_bucket_iam_member" "ui_runtime_catalogue_reader" {
  bucket = "strategy-store-cefi-central-element-323112"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

resource "google_cloud_scheduler_job" "catalogue_regen_nightly" {
  project          = var.project_id
  region           = var.region
  name             = "catalogue-regen-nightly"
  description      = "Regenerate catalogue artefacts (envelope.md/.json + availability.json + strategy_instruments.json) to GCS"
  schedule         = "30 4 * * *" # 04:30 UTC daily — after instruments-service refresh, before strategy-service kicks off
  time_zone        = "UTC"
  attempt_deadline = "1800s" # 30 min — real parquet read takes ~1-2 min, plus GCS upload

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/catalogue-regen:run"

    oauth_token {
      service_account_email = google_service_account.catalogue_regen.email
    }
  }
}
