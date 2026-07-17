# Catalogue regeneration — nightly Cloud Scheduler trigger
#
# Regenerates the four catalogue artefacts on GCS:
#   gs://strategy-store-central-element-323112/catalogue/{
#     envelope.md, envelope.json, availability.json, strategy_instruments.json
#   }
#
# Source scripts (live in unified-api-contracts/scripts/):
#   - enumerate_envelope.py  (envelope.md + envelope.json)
#   - enumerate_availability.py  (availability.json)
#   - enumerate_strategy_instruments.py --with-real-instruments
#       (joins envelope with instruments-store-{cefi,defi,tradfi,sports,prediction}
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
#   plans/active/dart_ui_strategy_filtering_and_onboarding_2026_04_24.md (Phase 11 N2)

resource "google_service_account" "catalogue_regen" {
  project      = var.project_id
  account_id   = "catalogue-regen"
  display_name = "Catalogue Regen — nightly artefact rebuild"
}

resource "google_storage_bucket_iam_member" "catalogue_regen_strategy_store_writer" {
  bucket = "strategy-store-central-element-323112"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.catalogue_regen.email}"
}

# Read access to all instruments-store-* buckets (real parquet resolver)
#
# EVERY entry is the SSOT canonical `-prd-` name — cloud-providers.yaml `instruments-store`
# resolves to `instruments-store-{ag}-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}` — matching the
# lifecycle_catalogue_scheduler.tf:81-87 precedent. cefi/defi/tradfi were REPOINTED 2026-07-17:
# they still carried the flat no-env legacy literals, whose buckets were physically DELETED
# 2026-07-14 (bucket_estate_consolidation W2). That deletion was never propagated here, so each
# grant planned `will be created` against a 404 bucket ⇒ the next full prod `tofu apply` would
# ERROR. The grants also covered nothing real: this job resolves buckets through UAC
# `bucket_name()`, which defaults env="prd" and can ONLY emit the `-prd-` form.
# Provenance: terraform_instruments_cefi_armed_resurrection_2026_07_16.md (sibling-sweep todo).
resource "google_storage_bucket_iam_member" "catalogue_regen_instruments_reader" {
  for_each = toset([
    "instruments-store-cefi-prd-central-element-323112",
    "instruments-store-defi-prd-central-element-323112",
    # tradfi added (slot-7 2026-06-08) — the regen job's strategy_instruments join
    # reads the tradfi instruments-store parquet too; it was missing from the reader
    # grant (the sibling lifecycle/instrument_catalogue schedulers already cover it).
    # DORMANT TODAY (measured 2026-07-17): UAC's per-AG facade maps (TRADFI, INSTRUMENTS) → None,
    # so the join falls back to venue tokens and never opens this bucket. Repointed rather than
    # dropped — the canonical bucket EXISTS and holds the tradfi lifecycle catalog.parquet
    # (lifecycle_catalogue_scheduler.tf:83 writes it), so the grant must already be in place if
    # the facade gains a real TRADFI template. Dropping it would re-open the 2026-06-08 gap.
    "instruments-store-tradfi-prd-central-element-323112",
    # sports → SSOT canonical `-prd-` (sports legacy-bucket cutover 2026-07-16 T1.4); the legacy
    # no-env `instruments-store-sports-{project_id}` bucket is DELETED at cutover.
    "instruments-store-sports-prd-central-element-323112",
    # prediction → SSOT canonical `pred-prd` (cloud-providers.yaml
    # instruments-store-prediction resolves to instruments-store-pred-${DEPLOYMENT_ENV_SHORT}-…);
    # the stale `-prediction-` literal doesn't exist as a live bucket, the actual
    # `prod/catalog.parquet` lives here (lifecycle_catalogue_scheduler.tf writer, 2026-07-06).
    "instruments-store-pred-prd-central-element-323112",
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
  bucket = "strategy-store-central-element-323112"
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
