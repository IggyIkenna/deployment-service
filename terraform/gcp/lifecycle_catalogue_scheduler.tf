# Instrument LIFECYCLE catalogue roll-up — daily Cloud Scheduler trigger (per asset_group)
#
# G1 foundation (master_data_canonicalisation_migration_catalogue_2026_06_07 §G1.schedule +
# proper_instrument_catalogue_lifecycle_rollup_2026_06_04): the "could-exist universe"
# root. This schedules instruments-service `scripts/build_instrument_catalogue.py` — the
# LIFECYCLE roll-up that rolls the maintained per-date
#   instrument_availability/by_date/day=…/venue=…/instruments.parquet
# snapshots into the cumulative available_from/available_to catalogue
#   gs://{instruments-store-<ag>}/{DEPLOYMENT_ENV}/catalog.parquet
# which `enumerate_expected_universe.py --enumerator-version v2` then cross-joins ×
# dates × data_types − existing manifest → seeds record_expected_unattempted.
#
# DISTINCT FROM the two sibling schedulers (do NOT conflate):
#   * catalogue_regen_scheduler.tf      → enumerate_envelope/availability/strategy_instruments (UAC artefacts)
#   * instrument_catalogue_scheduler.tf → generate_instrument_catalogue.py (UAC drilldown json/md)
# Neither of those runs build_instrument_catalogue.py — this file fills that gap so the v2
# enumerator always reads a FRESH lifecycle catalogue per AG.
#
# AG-PARAMETRIC: one Cloud Run Job + Scheduler per asset_group (for_each), so a per-AG
# failure/retry is isolated (every AG GREEN independently, per the data-pipeline-correctness
# rule). build_instrument_catalogue.py is already AG-parametric (--asset-group choices =
# cefi/defi/tradfi/sports/prediction; bucket via get_write_bucket_name("instruments", ag),
# env-tier). Sports keys its by-date snapshots under sports_reference/by_date so it carries
# the --by-date-prefix override.
#
# Trigger time: 01:00 UTC daily — AFTER the 00:00 UTC instruments-service FAST refresh
# (writes the by-date snapshots this rolls up) and BEFORE the 02:00 UTC
# instrument-catalogue-regen + 04:30 UTC catalogue-regen (downstream consumers of the
# rolled-up catalogue).
#
# NOT fire-and-forget: each AG is a bounded Cloud Run Job (STARTED/STOPPED/FAILED exit
# status + ServiceBootstrap events) with scheduler retry_config; post-apply the operator
# verifies each job RAN (T+10min: `gcloud run jobs executions list --job
# lifecycle-catalogue-regen-<ag>` = Succeeded) before the gate is GREEN.
#
# References:
#   plans/active/master_data_canonicalisation_migration_catalogue_2026_06_07.md §G1.schedule
#   plans/active/proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md
#
# KNOWN bucket-name discrepancy (PRE-EXISTING — flagged, not introduced here):
#   cloud-providers.yaml maps prediction → instruments-store-PRED-…, but the sibling
#   instrument_catalogue_scheduler.tf grants on instruments-store-PREDICTION-… . This file
#   matches the sibling's literals for consistency; reconcile both against the live bucket
#   when the bucket_name_ssot L6 decommission lands (bucket_name_ssot_legacy_dual_write_remediation).

locals {
  # asset_group → { bucket = instruments-store bucket, extra_args = per-AG CLI overrides }
  # CORRECTED 2026-06-11 (canonicalisation autonomous run): the prior literals were the
  # LEGACY flat buckets (no env segment) + a nonexistent instruments-store-prediction-…
  # (404 — the live bucket is the "pred" short key). The lifecycle roll-up reads/writes
  # the CANONICAL env-short buckets (where prod/catalog.parquet actually lives, verified
  # via gcloud storage ls 2026-06-11) per resolve_bucket_name / cloud-providers.yaml.
  lifecycle_catalogue_asset_groups = {
    cefi   = { bucket = "instruments-store-cefi-prd-central-element-323112", extra_args = [] }
    defi   = { bucket = "instruments-store-defi-prd-central-element-323112", extra_args = [] }
    tradfi = { bucket = "instruments-store-tradfi-prd-central-element-323112", extra_args = [] }
    sports = { bucket = "instruments-store-sports-prd-central-element-323112", extra_args = ["--by-date-prefix", "sports_reference/by_date"] }
    # prediction → flat "pred" short key per cloud-providers.yaml (instruments-store-prediction-… does not exist).
    prediction = { bucket = "instruments-store-pred-prd-central-element-323112", extra_args = [] }
  }
}

resource "google_service_account" "lifecycle_catalogue_regen" {
  project      = var.project_id
  account_id   = "lifecycle-catalogue-regen"
  display_name = "Lifecycle Catalogue Regen — daily per-AG instrument available_from/to roll-up"
}

# Read by-date snapshots + WRITE catalog.parquet on every per-AG instruments-store bucket.
resource "google_storage_bucket_iam_member" "lifecycle_catalogue_instruments_admin" {
  for_each = { for ag, cfg in local.lifecycle_catalogue_asset_groups : ag => cfg.bucket }
  bucket   = each.value
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.lifecycle_catalogue_regen.email}"
}

module "lifecycle_catalogue_regen_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.lifecycle_catalogue_asset_groups

  name                  = "lifecycle-catalogue-regen-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.lifecycle_catalogue_regen.email

  # instruments-service image — build_instrument_catalogue.py lives at
  # /app/instruments-service/scripts/ (Dockerfile WORKDIR=/app/instruments-service, COPY . .).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800 # 30 min — by-date snapshot read + lifecycle roll-up + catalog.parquet write
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = concat(
    [
      "/app/instruments-service/scripts/build_instrument_catalogue.py",
      "--asset-group",
      each.key,
    ],
    each.value.extra_args,
  )

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "lifecycle-catalogue-regen-${each.key}"
  environment  = var.environment

  labels = {
    "purpose"     = "lifecycle-catalogue-regen"
    "asset_group" = each.key
  }
}

resource "google_cloud_scheduler_job" "lifecycle_catalogue_regen_daily" {
  for_each = local.lifecycle_catalogue_asset_groups

  project          = var.project_id
  region           = var.region
  name             = "lifecycle-catalogue-regen-${each.key}-daily"
  description      = "Roll up ${each.key} instrument_availability/by_date snapshots into the available_from/to lifecycle catalogue (catalog.parquet) for the v2 expected-universe enumerator"
  schedule         = "0 1 * * *" # 01:00 UTC daily — after 00:00 IS FAST refresh, before 02:00/04:30 downstream catalogue regens
  time_zone        = "UTC"
  attempt_deadline = "1800s"

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/lifecycle-catalogue-regen-${each.key}:run"

    oauth_token {
      service_account_email = google_service_account.lifecycle_catalogue_regen.email
    }
  }
}
