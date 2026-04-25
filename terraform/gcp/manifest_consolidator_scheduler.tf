# Manifest Consolidator — Cloud Run Job + per-bucket Scheduler crons
#
# Plan: manifest_429_per_vm_sharding_2026_04_25
#
# Background
#   Phase 1 of the per-VM-sharding fix landed in
#   `unified-trading-library` (commit c95480de): writers gated on
#   `manifest_per_vm_shards` write to `_index/per_vm/{instance}.parquet`
#   instead of the single canonical `_index/availability_index.parquet`,
#   eliminating the 429 thundering-herd.
#
#   This file ships Phase 2: a Cloud Run Job that runs
#   `python -m unified_trading_library.manifest_consolidator --bucket {X}`
#   once per minute per category bucket. The consolidator merges every
#   `_index/per_vm/*.parquet` shard into the canonical
#   `_index/availability_index.parquet`, so the deployment-api UI and
#   downstream readers always see a single coherent view.
#
#   On the FIRST run against a bucket that already has a populated
#   `_index/availability_index.parquet`, the consolidator copies it to
#   `_index/per_vm/_legacy_seed.parquet` so historical rows participate
#   in the merge. Idempotent: subsequent runs skip the copy.
#
#   Skew is tolerated: the reader fallback in `read_availability_index`
#   merges per-VM shards live whenever the consolidated blob is older
#   than `MANIFEST_CONSOLIDATED_STALENESS_SEC` (default 120s). One
#   missed cron cycle = readers transparently fall back, no UI breakage.
#
# Schedule
#   `*/1 * * * *` per category bucket — 10 buckets total (5 instruments,
#   5 market-data-tick) = 10 cron triggers, all firing the same Cloud
#   Run Job with the bucket name as a CLI arg.
#
# Image
#   Reuses `market-tick-data-service:latest` (which has UTL installed
#   as a dep). Same image as the defi_collection_scheduler jobs — no
#   new build pipeline needed.
#
# Service account
#   * Scheduler invoker: `t1_batch_sa` (already has roles/run.invoker).
#   * Container runtime: `unified_trading_sa` (storage.objectAdmin on
#     all category buckets — needed to read per-VM shards and write the
#     consolidated blob).

locals {
  # Category buckets to consolidate. Sourced from main.tf bucket
  # declarations — every bucket where ManifestWriter writes manifest
  # shards needs a consolidator schedule.
  manifest_consolidator_buckets = {
    "instruments-cefi"       = "instruments-store-cefi-${var.project_id}"
    "instruments-tradfi"     = "instruments-store-tradfi-${var.project_id}"
    "instruments-defi"       = "instruments-store-defi-${var.project_id}"
    "instruments-sports"     = "instruments-store-sports-${var.project_id}"
    "instruments-prediction" = "instruments-store-prediction-${var.project_id}"
    "market-data-cefi"       = "market-data-tick-cefi-${var.project_id}"
    "market-data-tradfi"     = "market-data-tick-tradfi-${var.project_id}"
    "market-data-defi"       = "market-data-tick-defi-${var.project_id}"
    "market-data-sports"     = "market-data-tick-sports-${var.project_id}"
    "market-data-prediction" = "market-data-tick-prediction-${var.project_id}"
  }
}

# -------------------------------------------------------
# Single Cloud Run Job — invoked per-bucket via separate cron triggers
# -------------------------------------------------------
# A single job is sufficient because each invocation is bucket-scoped
# (the bucket name is passed via job-level override at scheduler time).
# But Cloud Scheduler can't override args on a Cloud Run Job invocation
# — so we declare ONE job per bucket. This keeps logs and IAM scoped
# per-bucket and avoids any cross-bucket coupling.
module "manifest_consolidator_job" {
  for_each = local.manifest_consolidator_buckets
  source   = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-manifest-consolidator-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  # Reuses the MTDS image (UTL is bundled as a dep).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  cpu             = "1"
  memory          = "2Gi"
  timeout_seconds = 300 # consolidation is a single read-list-merge-write cycle, ~5-30s expected
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # CLI entrypoint exposed by `manifest_consolidator.main()`.
  command = ["python"]
  args    = ["-m", "unified_trading_library.manifest_consolidator", "--bucket", each.value]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "manifest-consolidator"
  environment  = var.environment

  labels = {
    "purpose"  = "manifest-consolidator"
    "category" = each.key
  }
}

# -------------------------------------------------------
# Cloud Scheduler crons — one per bucket, every minute
# -------------------------------------------------------
# `*/1 * * * *` is "every minute". The reader-fallback staleness
# threshold is 120s, so one missed cycle still keeps readers correct
# (they fall back to live shard merge for that read).
resource "google_cloud_scheduler_job" "manifest_consolidator_cron" {
  for_each = local.manifest_consolidator_buckets

  name        = "${local.env_prefix}-manifest-consolidator-${each.key}-cron"
  description = "Consolidate per-VM manifest shards in ${each.value} into the canonical _index/availability_index.parquet."
  schedule    = "*/1 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.manifest_consolidator_job[each.key].name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries on failure — the next minute's cron will pick up where we
  # left off (consolidator is idempotent). Avoids retry-storm when a
  # bucket is genuinely down.
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# -------------------------------------------------------
# Outputs — for verification scripts and dashboards
# -------------------------------------------------------
output "manifest_consolidator_job_names" {
  description = "Map of category → Cloud Run Job name for manual ad-hoc fires (`gcloud run jobs execute`)."
  value = {
    for cat, _ in local.manifest_consolidator_buckets :
    cat => module.manifest_consolidator_job[cat].name
  }
}

output "manifest_consolidator_cron_names" {
  description = "Map of category → Cloud Scheduler cron name."
  value = {
    for cat, _ in local.manifest_consolidator_buckets :
    cat => google_cloud_scheduler_job.manifest_consolidator_cron[cat].name
  }
}
