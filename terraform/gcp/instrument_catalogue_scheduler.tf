# Instrument catalogue regeneration — nightly Cloud Scheduler trigger
#
# Catalogue plan P2.1 (2026-04-29). Sibling of catalogue_regen_scheduler.tf;
# kept separate so the two regens can fail/retry independently and we can
# carry distinct SA grants (the instrument catalogue reads from BOTH
# instruments-store-* AND market-data-tick-* buckets).
#
# Generator script: unified-api-contracts/scripts/generate_instrument_catalogue.py
#
# Outputs (written by the generator and uploaded to GCS):
#   gs://strategy-store-cefi-central-element-323112/catalogue/instrument/
#       instrument-catalogue.json   (drilldown-friendly entries)
#       instrument-catalogue.md     (human matrix with coverage band emoji)
#       shard-dynamics.json         (pure static spec dump)
#
# Trigger time: 02:00 UTC daily. Runs BEFORE the 04:30 UTC strategy
# catalogue regen so the instrument-shape SSOT is current when the strategy
# regen reads the same manifest.
#
# References:
#   plans/active/instrument_catalogue_availability_matrix_2026_04_29.md

resource "google_service_account" "instrument_catalogue_regen" {
  project      = var.project_id
  account_id   = "instrument-catalogue-regen"
  display_name = "Instrument Catalogue Regen — nightly artefact rebuild"
}

# Read access to all instruments-store-* buckets (manifest + parquet).
resource "google_storage_bucket_iam_member" "instrument_catalogue_instruments_reader" {
  for_each = toset([
    "instruments-store-cefi-central-element-323112",
    "instruments-store-defi-central-element-323112",
    "instruments-store-sports-central-element-323112",
    "instruments-store-prediction-central-element-323112",
  ])
  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.instrument_catalogue_regen.email}"
}

# Read access to all market-data-tick-* buckets (manifest probe).
resource "google_storage_bucket_iam_member" "instrument_catalogue_market_data_reader" {
  for_each = toset([
    "market-data-tick-cefi-central-element-323112",
    "market-data-tick-defi-central-element-323112",
    "market-data-tick-tradfi-central-element-323112",
    "market-data-tick-sports-central-element-323112",
    "market-data-tick-prediction-central-element-323112",
  ])
  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.instrument_catalogue_regen.email}"
}

# Write access to the catalogue artefact bucket.
resource "google_storage_bucket_iam_member" "instrument_catalogue_strategy_store_writer" {
  bucket = "strategy-store-cefi-central-element-323112"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.instrument_catalogue_regen.email}"
}

module "instrument_catalogue_regen_job" {
  source = "../modules/container-job/gcp"

  name                  = "instrument-catalogue-regen"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.instrument_catalogue_regen.email

  # Reuses the MTDS image (UAC bundled as a dep + UTL for read_availability_index).
  # Same image the manifest-consolidator job uses.
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800 # 30 min — manifest reads across 5 asset_group buckets + GCS upload
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # CLI entrypoint: the catalogue generator script bundled in unified-api-contracts.
  command = ["python"]
  args = [
    "/usr/src/unified-api-contracts/scripts/generate_instrument_catalogue.py",
    "--project-id",
    var.project_id,
    "--output-dir",
    "/tmp/instrument-catalogue",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "instrument-catalogue-regen"
  environment  = var.environment

  labels = {
    "purpose" = "instrument-catalogue-regen"
  }
}

resource "google_cloud_scheduler_job" "instrument_catalogue_regen_nightly" {
  project          = var.project_id
  region           = var.region
  name             = "instrument-catalogue-regen-nightly"
  description      = "Regenerate instrument-catalogue artefacts (catalogue/instrument/{json,md} + shard-dynamics.json) by joining DataTypeCapability registry with manifest"
  schedule         = "0 2 * * *" # 02:00 UTC daily
  time_zone        = "UTC"
  attempt_deadline = "1800s" # 30 min — manifest read across 5 buckets + GCS upload

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/instrument-catalogue-regen:run"

    oauth_token {
      service_account_email = google_service_account.instrument_catalogue_regen.email
    }
  }
}
