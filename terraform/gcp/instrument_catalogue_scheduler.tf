# Instrument catalogue regeneration — nightly Cloud Scheduler trigger
#
# Catalogue plan P2.1 (2026-04-29). Sibling of catalogue_regen_scheduler.tf;
# kept separate so the two regens can fail/retry independently and we can
# carry distinct SA grants (the instrument catalogue reads from BOTH
# instruments-store-* AND market-data-tick-* buckets).
#
# Generator script: unified-api-contracts/scripts/generate_instrument_catalogue.py
#
# Outputs (written by the generator and uploaded to GCS) — CORRECTED 2026-07-17: the unified
# FLAT strategy-store bucket, per strategy_store_split_brain_2026_07_13.md + the generator's own
# `strategy_store_bucket()` call. The per-AG `strategy-store-cefi-…` named here previously is 404.
#   gs://strategy-store-central-element-323112/catalogue/instrument/
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
#
# EVERY entry is the SSOT canonical `-prd-` name (cloud-providers.yaml `instruments-store` →
# `instruments-store-{ag}-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}`), mirroring
# lifecycle_catalogue_scheduler.tf:81-87. cefi/defi REPOINTED 2026-07-17 off the flat no-env
# legacy literals, whose buckets were physically DELETED 2026-07-14 (bucket_estate_consolidation
# W2) — a deletion never propagated to this IAM layer, so both planned `will be created` against
# a 404 bucket ⇒ the next full prod `tofu apply` would ERROR. generate_instrument_catalogue.py
# resolves via UAC `bucket_name()` (env defaults "prd"), so only the `-prd-` names are ever read.
# (No tradfi row: UAC maps (TRADFI, INSTRUMENTS) → None — tradfi's universe is the UAC registry,
# not a parquet — so this job never reads a tradfi instruments-store bucket.)
resource "google_storage_bucket_iam_member" "instrument_catalogue_instruments_reader" {
  for_each = toset([
    "instruments-store-cefi-prd-central-element-323112",
    "instruments-store-defi-prd-central-element-323112",
    # sports → SSOT canonical `-prd-` (sports legacy-bucket cutover 2026-07-16 T1.4); the legacy
    # no-env `instruments-store-sports-{project_id}` bucket is DELETED at cutover.
    "instruments-store-sports-prd-central-element-323112",
    # prediction → SSOT canonical `pred-prd` (per cloud-providers.yaml
    # instruments-store-prediction template resolves to instruments-store-pred-${DEPLOYMENT_ENV_SHORT}-…).
    # The stale `instruments-store-prediction-central-element-323112` literal doesn't exist as a live bucket;
    # the actual catalog.parquet lives here (lifecycle_catalogue_scheduler.tf writer confirmed 2026-07-06).
    "instruments-store-pred-prd-central-element-323112",
  ])
  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.instrument_catalogue_regen.email}"
}

# Read access to all market-data-tick-* buckets (manifest probe).
#
# EVERY entry is the SSOT canonical `-prd-` name (cloud-providers.yaml `market-data` →
# `market-data-tick-{ag}-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}`). cefi/defi/tradfi REPOINTED
# 2026-07-17 off the flat no-env legacy literals — those three buckets were physically DELETED
# 2026-07-14 (bucket_estate_consolidation W2) and the deletion never reached this IAM layer, so
# each planned `will be created` against a 404 bucket ⇒ the next full prod `tofu apply` would
# ERROR. The job reads the `-prd-` names at runtime via UAC `bucket_name(kind=MARKET_DATA)`.
resource "google_storage_bucket_iam_member" "instrument_catalogue_market_data_reader" {
  for_each = toset([
    "market-data-tick-cefi-prd-central-element-323112",
    "market-data-tick-defi-prd-central-element-323112",
    "market-data-tick-tradfi-prd-central-element-323112",
    # sports → SSOT canonical `-prd-` (sports legacy-bucket cutover 2026-07-16 T1.4).
    # CORRECTED 2026-07-17: the prior note here claimed the legacy no-env
    # `market-data-tick-sports-{project_id}` bucket "is DELETED at cutover" — measurably FALSE.
    # That bucket EXISTS and is DELIBERATELY RETAINED (blocked on OR-5b; 550,062 keys on 32 days
    # live only there — main.tf keeps its block for exactly this reason). The comment conflated
    # it with `instruments-store-sports-{project_id}`, which genuinely IS deleted (404). The
    # `-prd-` repoint itself is correct and unchanged — only the false claim is removed.
    "market-data-tick-sports-prd-central-element-323112",
    # prediction → SSOT canonical `pred-prd` (per cloud-providers.yaml).
    "market-data-tick-pred-prd-central-element-323112",
  ])
  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.instrument_catalogue_regen.email}"
}

# Write access to the catalogue artefact bucket.
#
# REPOINTED 2026-07-17 — was `strategy-store-cefi-central-element-323112`, which is 404 (deleted
# out-of-band; confirmed via the elevated Cloud Build SA) and therefore planned `will be created`
# ⇒ the next full prod `tofu apply` would ERROR. That per-AG name is the documented split-brain
# drift (strategy_store_split_brain_2026_07_13.md): the operator-ratified 2026-05-20 (D6 Phase 4)
# SSOT is the unified FLAT `strategy-store-{project_id}` bucket — cloud-providers.yaml kind
# `strategy-store`, UAC `STRATEGY_STORE_BUCKET_TEMPLATE`. The job agrees: generate_instrument_
# catalogue.py:565 uploads to `strategy_store_bucket(project_id)` = the flat name, so this SA
# held NO write grant on the bucket it actually writes to. The sibling
# catalogue_regen_strategy_store_writer already grants on the flat name — this matches it.
resource "google_storage_bucket_iam_member" "instrument_catalogue_strategy_store_writer" {
  bucket = "strategy-store-central-element-323112"
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
