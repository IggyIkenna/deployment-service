# Data-status offline rollup — Cloud Run Job + */5 cron
#
# Plan: data_status_offline_rollup_2026_05_06.plan.md
#
# Background
#   The deployment-api ``/api/data-status/manifest`` endpoint took
#   ~310-410s per request for the full Jan 2018 → today range — GIL-bound
#   Python loops over ~1200 honest-coverage entries (5 asset_groups ×
#   ~30 venues × ~8 data_types). DEFI alone is ~300s and capped any
#   parallelism (verified: ProcessPool fork-pool only shaved 20%).
#
#   This file ships the offline-rollup pattern: a Cloud Run Job runs
#   every 5 min, computes the full-range coverage for every service, and
#   writes ``gs://{pid}-data-status-rollups/{service}/full.json.gz``. The
#   API endpoint reads the rollup and slices the user's date window
#   in-memory in <500ms. Latency: 410s → ~200ms.
#
# Schedule
#   ``*/5 * * * *`` — every 5 minutes. The deployment-api fast-path treats
#   rollups older than 30 min as stale and falls through to the on-demand
#   compute, so 6 missed cron cycles still keep the API correct.
#
# Image
#   Reuses ``deployment-api:latest`` — has DataStatusService natively
#   (the worker imports from ``deployment_api.scripts.data_status_rollup_worker``).
#
# Service account
#   * Scheduler invoker: ``t1_batch_sa``
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
# Cloud Run Job — single job, runs the worker for ALL services per fire
# -------------------------------------------------------
# One job (not per-service) because the worker iterates services internally
# and the per-service compute is sequential by design (a single warm cache
# of the manifest lru_cache amortises across services). One Cloud Run Job
# is also cheaper than N — Cloud Run Jobs have a per-creation cost and
# cold-start overhead that's amortised across a longer wallclock.
module "data_status_rollup_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-data-status-rollup"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  # Reuses the deployment-api image — has DataStatusService + all deps.
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-api:latest"

  cpu             = "4"
  memory          = "8Gi"
  timeout_seconds = 1200 # 20 min — covers DEFI category at ~300s × 4 services worst case

  max_retries = 1
  parallelism = 1
  task_count  = 1

  command = ["python"]
  args = [
    "-m",
    "deployment_api.scripts.data_status_rollup_worker",
    "--project",
    var.project_id,
    "--bucket",
    local.data_status_rollup_bucket_name,
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
    # Force single gunicorn worker semantics inside the job (no FastAPI
    # serving here, but the DataStatusService instance loads VenueMapping
    # and other singletons once).
    WORKERS = "1"
  }

  service_name = "data-status-rollup"
  environment  = var.environment

  labels = {
    purpose = "data-status-rollup"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — every 5 min
# -------------------------------------------------------
# 5-min cadence balances freshness vs cost. Manifest-consolidator runs
# every 1 min, so the underlying data is at most 1 min stale before the
# rollup picks it up; total UI-visible staleness floor is 5-6 min.
resource "google_cloud_scheduler_job" "data_status_rollup_cron" {
  name        = "${local.env_prefix}-data-status-rollup-cron"
  description = "Compute offline data-status rollup for every service; deployment-api reads instead of computing on demand."
  schedule    = "*/5 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.data_status_rollup_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries on failure — the next 5-min cron will pick up where we left
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

output "data_status_rollup_job_name" {
  description = "Cloud Run Job name — for ad-hoc runs (`gcloud run jobs execute`)."
  value       = module.data_status_rollup_job.name
}

output "data_status_rollup_cron_name" {
  description = "Cloud Scheduler cron name."
  value       = google_cloud_scheduler_job.data_status_rollup_cron.name
}
