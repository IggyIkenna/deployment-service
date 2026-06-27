# Daily IS instrument batch enumeration — Cloud Scheduler + Cloud Run Job (per asset_group)
#
# Closes the gap where IS instrument parquets (``instrument_availability/by_date/``) had
# NO daily automation: the existing Terraform only covers ``enumerate_expected_universe``
# (the denominator cross-join) and catalogue regen, NOT the CLOB/Gamma API scrape that
# writes the actual per-market CQG-partitioned parquets MTDS reads for its universe.
#
# Without this scheduler, the IS parquets for "today" are only produced by manually
# triggered VM backfills. This meant Polymarket BTC daily markets (listed ~13:00 UTC)
# were never automatically picked up — MTDS had 0 BTC subscription tokens and the arb
# detector produced 0 matched pairs indefinitely.
#
# TIMING — 13:30 UTC: Polymarket lists the day's BTC hourly/daily markets at ~13:00 UTC
# (empirically confirmed June 25/26/27 2026: parquets with BTC_UP_DOWN_DAILY CQG appear
# at ~13:05-13:14 UTC on the SAME day). Running at 13:30 catches those markets. All other
# AGs (CEFI/DEFI/TRADFI/SPORTS) are independent of this timing but run at the same hour.
#
# ROLLING WINDOW — 3 days: each run covers (today − 2) → today with --force, so:
# (1) any gap from a missed prior day is automatically caught up, and
# (2) today's parquet is overwritten even if a partial run existed from earlier.
#
# PER-AG ISOLATION: one Cloud Run Job + Scheduler per asset_group (for_each) — a failure
# in one AG's Polymarket CLOB API scrape does NOT block Kalshi CEFI enumeration etc.
# Cloud Run Job retry_count=1 means a transient API failure gets one automatic retry.
#
# MANIFEST SHARDS: IS batch enumeration writes per-VM shards (`_index/per_vm/{VM_NAME}.parquet`)
# under MANIFEST_PER_VM_SHARDS=true. VM_NAME=is-daily-enum-<ag> is stable per AG so
# every daily run overwrites only its own shard (last-writer-wins; the manifest consolidator
# merges it within its 1-min cron automatically — no companion force-consolidate needed,
# as fixed by the content-write-at marker 2026-06-19).
#
# DOWNSTREAM: after IS parquets land, MTDS reads the universe at its next restart via
# ``_read_prediction_is_universe_sync`` (7-day lookback; ``clob_token_ids`` populated by
# the CLOB supplement in the Polymarket adapter). MTDS restarts are handled separately
# (operator-driven or via the deployment UI). The manifest consolidator picks up new rows
# within 1 minute automatically.
#
# References:
#   instruments-service/scripts/daily_is_enumeration.py (the Cloud Run Job entrypoint)
#   deployment-service/scripts/vm/launch-instruments-backfill-vm.sh (manual equivalent)
#   deployment-service/terraform/gcp/expected_universe_v2_scheduler.tf (pattern followed)
#   plans/active/prediction_venue_perps_and_live_clob_depth_2026_06_20.md

locals {
  # asset_group → instruments-store GCS bucket (canonical resolved name).
  # IS writes instrument_availability/by_date/ parquets + _index/per_vm/ shards here.
  is_daily_enum_asset_groups = {
    cefi       = "instruments-store-cefi-prd-central-element-323112"
    defi       = "instruments-store-defi-prd-central-element-323112"
    tradfi     = "instruments-store-tradfi-prd-central-element-323112"
    sports     = "instruments-store-sports-prd-central-element-323112"
    prediction = "instruments-store-pred-prd-central-element-323112"
  }
}

# The Cloud Scheduler oauth_token authenticates as this SA to call the Cloud Run :run endpoint.
# The SA only needs roles/run.invoker on each job (not bucket/secret access — those belong to
# the execution SA below). Kept as a dedicated SA so the scheduler auth identity is narrow.
resource "google_service_account" "is_daily_enum" {
  project      = var.project_id
  account_id   = "is-daily-enum"
  display_name = "IS Daily Enumerator scheduler auth — oauth_token for Cloud Scheduler → Cloud Run :run"
}

# Reuse unified-trading-sa as the Cloud Run Job execution SA: it already holds objectAdmin on all
# instruments-store buckets (from IS VM runs) and secretmanager.secretAccessor at project level.
# This avoids needing storage.admin / resourcemanager.projectIamAdmin for the deploying SA.
data "google_service_account" "unified_trading_sa" {
  project    = var.project_id
  account_id = "unified-trading-sa"
}

# CRITICAL: the Cloud Scheduler job authenticates as the SA (oauth_token) and POSTs
# the Cloud Run Job :run endpoint — the SA MUST hold roles/run.invoker on each job.
# (Same gap fixed in expected_universe_v2_scheduler.tf 2026-06-22 — added pre-emptively.)
resource "google_cloud_run_v2_job_iam_member" "is_daily_enum_run_invoker" {
  for_each = local.is_daily_enum_asset_groups

  project  = var.project_id
  location = var.region
  name     = module.is_daily_enum_job[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.is_daily_enum.email}"
}

module "is_daily_enum_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.is_daily_enum_asset_groups

  name                  = "is-daily-enum-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = data.google_service_account.unified_trading_sa.email

  # instruments-service image — daily_is_enumeration.py lives at
  # /app/instruments-service/scripts/ (same image as lifecycle-catalogue-regen).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = "4"
  memory          = "8Gi"   # prediction enumerates 50k+ Polymarket markets (5+ GB RSS observed); 8 Gi prevents OOM
  timeout_seconds = 7200    # 2 hours — allow for rate-limited CLOB pagination across 3-day window
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "/app/instruments-service/scripts/daily_is_enumeration.py",
    "--asset-group", each.key,
    "--days-back", "3",
    "--force",    # re-fetch even if parquets exist — ensures 13:30 UTC run overwrites partial morning parquets
    "--log-level", "INFO",
  ]

  environment_variables = {
    GCP_PROJECT_ID         = var.project_id
    PROJECT_ID             = var.project_id
    DEPLOYMENT_ENV         = var.environment
    CLOUD_PROVIDER         = "gcp"
    CLOUD_MOCK_MODE        = "false"
    MANIFEST_PER_VM_SHARDS = "true"
    VM_NAME                = "is-daily-enum-${each.key}" # stable shard name; consolidator merges last-writer-wins
    PYTHONUNBUFFERED       = "1"
  }

  service_name = "is-daily-enum-${each.key}"
  environment  = var.environment

  labels = {
    "purpose"     = "is-daily-enum"
    "asset_group" = each.key
  }
}

resource "google_cloud_scheduler_job" "is_daily_enum" {
  for_each = local.is_daily_enum_asset_groups

  project          = var.project_id
  region           = var.region
  name             = "is-daily-enum-${each.key}"
  description      = "Daily IS instrument batch enumeration for ${each.key}: rolling 3-day window --force at 13:30 UTC (after Polymarket lists BTC daily markets ~13:00 UTC)"
  schedule         = "30 13 * * *" # 13:30 UTC — after Polymarket BTC daily listing ~13:00; all AGs run in parallel
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
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/is-daily-enum-${each.key}:run"

    oauth_token {
      service_account_email = google_service_account.is_daily_enum.email
    }
  }
}
