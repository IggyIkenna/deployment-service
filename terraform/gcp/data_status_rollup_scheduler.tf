# Data-status offline rollup — dedicated Cloud Run Service + */10 cron
#
# Plan: data_status_offline_rollup_2026_05_06.plan +
#       proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (fix (e))
#
# Background
#   The deployment-api ``/api/data-status/manifest`` endpoint took
#   ~310-410s per request for the full Jan 2018 → today range — GIL-bound
#   Python loops over ~1200 honest-coverage entries (5 asset_groups ×
#   ~30 venues × ~8 data_types). DEFI alone is ~300s and capped any
#   parallelism (verified: ProcessPool fork-pool only shaved 20%).
#
#   This file ships the offline-rollup pattern: a worker computes the
#   full-range coverage for every service every 10 min and writes
#   ``gs://{pid}-data-status-rollups/{service}/full.json.gz`` (and a
#   ``.beta.json.gz`` in beta mode). The API endpoint reads the rollup and
#   slices the user's date window in-memory in <500ms. Latency: 410s → ~200ms.
#
# Runtime (CHANGED 2026-06-14 — was a Cloud Run Job)
#   The worker runs inside a DEDICATED gen1 16Gi scale-to-zero Cloud Run
#   SERVICE, triggered via its ``/api/data-status/rollup-run`` endpoint —
#   NOT a Cloud Run Job. Cloud Run Jobs are gen2-only and the native
#   cell-grid compute crashes natively on gen2 (see the RETIRED-job note
#   below). The dedicated service is imperatively managed today (gcloud
#   replace + cloudbuild ``_ROLLUP_SVC`` image deploy); this file owns the
#   bucket, IAM, and the cron that drives it.
#
# Schedule
#   ``*/10 * * * *`` — every 10 minutes. The deployment-api fast-path treats
#   rollups older than 30 min as stale and falls through to the on-demand
#   compute, so 2 missed cron cycles still keep the API correct (beta mode
#   is staleness-exempt — the projected v9 index is read loud-or-nothing).
#
# Service account
#   * Scheduler invoker: ``t1_batch_sa`` (OIDC; needs run.invoker on the svc)
#   * Container runtime: ``unified_trading_sa`` (storage.objectAdmin on the
#     rollup bucket; readers on every manifest bucket the worker queries)

locals {
  data_status_rollup_bucket_name = "${var.project_id}-data-status-rollups"
}

# -------------------------------------------------------
# Output bucket — gzipped JSON rollups, 7-day retention
# -------------------------------------------------------
resource "google_storage_bucket" "data_status_rollups" {
  name          = local.data_status_rollup_bucket_name
  project       = var.project_id
  location      = var.region
  force_destroy = false

  # Rollups are written every 5 min; only the latest matters. Lifecycle
  # rule deletes anything older than 7 days to keep the bucket small.
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true

  labels = {
    purpose = "data-status-rollup"
  }
}

# Storage-objectAdmin for the runtime service account (writes + reads its own
# output). The worker also reads from the manifest buckets (instruments-store-*
# + market-data-tick-*) — those grants live in main.tf where the buckets are
# declared, and unified_trading_sa already has objectAdmin on them.
resource "google_storage_bucket_iam_member" "data_status_rollups_runtime" {
  bucket = google_storage_bucket.data_status_rollups.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.unified_trading.email}"
}

# -------------------------------------------------------
# RETIRED — Cloud Run Job (gen2-only native crash, 2026-06-14)
# -------------------------------------------------------
# The single Cloud Run Job that used to compute the rollup is GONE. ROOT
# CAUSE: Cloud Run *Jobs* run in the gen2 execution environment ONLY, and
# the native pyarrow/pandas full-range cell-grid compute in
# `_get_manifest_status_sync` crashes natively on gen2 (C-level abort, no
# Python exception, faulthandler-invisible). The identical compute runs
# clean in the gen1 environment that Cloud Run *services* default to. So
# the rollup now runs inside a dedicated gen1 16Gi scale-to-zero Cloud Run
# SERVICE (`${local.env_prefix}-data-status-rollup-svc`) via its
# `/api/data-status/rollup-run` endpoint, triggered by the cron below.
#
# The dedicated service is currently imperatively managed (deployed via
# `gcloud run services replace` from an exported YAML; image kept fresh by
# the deployment-api cloudbuild `_ROLLUP_SVC` deploy step). Removing the
# job module here makes a future `terraform apply` destroy the orphaned
# gen2 job (nothing references it). Full diagnosis + fix (e):
# `plans/active/proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md`.

# -------------------------------------------------------
# Cloud Scheduler cron — every 10 min
# -------------------------------------------------------
# Triggers the in-service rollup endpoint on the dedicated scale-to-zero
# rollup SERVICE (gen1; see the RETIRED-job note above for why not a job).
# 10-min cadence balances freshness vs the cold-start + full-range compute
# cost (instruments-service all-AG ~8.4Gi / a few minutes). Manifest-
# consolidator runs every 1 min, so the underlying data is at most 1 min
# stale before the rollup picks it up; total UI-visible staleness floor is
# ~10-12 min. The deployment-api fast-path treats rollups older than 30 min
# as stale (except beta mode, which is staleness-exempt).
resource "google_cloud_scheduler_job" "data_status_rollup_cron" {
  name        = "${local.env_prefix}-data-status-rollup-cron"
  description = "Trigger the in-service data-status rollup endpoint on the dedicated rollup service; deployment-api reads the offline rollup instead of computing on demand."
  schedule    = "*/10 * * * *"
  time_zone   = "UTC"
  region      = var.region

  # Full-range compute can take several minutes on a cold start — give the
  # request a long deadline so the scheduler does not time out mid-compute.
  attempt_deadline = "600s"

  http_target {
    http_method = "POST"
    uri         = "https://${local.env_prefix}-data-status-rollup-svc-${var.project_number}.${var.region}.run.app/api/data-status/rollup-run"

    # The dedicated rollup service is IAM-private (no allUsers invoker), so
    # the scheduler authenticates with an OIDC identity token whose audience
    # is the service URL. The scheduler SA (t1_batch) needs roles/run.invoker
    # on the service — granted imperatively alongside the service deploy.
    oidc_token {
      service_account_email = local.t1_service_account_email
      audience              = "https://${local.env_prefix}-data-status-rollup-svc-${var.project_number}.${var.region}.run.app"
    }
  }

  # No retries on failure — the next 10-min cron will pick up where we left
  # off (worker is idempotent: each service write is overwrite-by-name).
  # Avoids retry-storm when one service's compute is genuinely broken.
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "data_status_rollup_bucket" {
  description = "GCS bucket holding gzipped JSON rollups."
  value       = google_storage_bucket.data_status_rollups.name
}

output "data_status_rollup_cron_name" {
  description = "Cloud Scheduler cron name."
  value       = google_cloud_scheduler_job.data_status_rollup_cron.name
}
