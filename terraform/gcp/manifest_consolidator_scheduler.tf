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
#   `*/1 * * * *` per category bucket — 10 buckets total (5 instruments-store,
#   5 market-data-tick), all prd-tiered (`{name}-${var.environment}-${var.project_id}`).
#   Prediction uses `pred` shortform per cloud-providers.yaml SSOT.
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
    "instruments-cefi"       = "instruments-store-cefi-${local.deployment_env_short}-${var.project_id}"
    "instruments-tradfi"     = "instruments-store-tradfi-${local.deployment_env_short}-${var.project_id}"
    "instruments-defi"       = "instruments-store-defi-${local.deployment_env_short}-${var.project_id}"
    "instruments-sports"     = "instruments-store-sports-${local.deployment_env_short}-${var.project_id}"
    "instruments-prediction" = "instruments-store-pred-${local.deployment_env_short}-${var.project_id}"
    "market-data-cefi"       = "market-data-tick-cefi-${local.deployment_env_short}-${var.project_id}"
    "market-data-tradfi"     = "market-data-tick-tradfi-${local.deployment_env_short}-${var.project_id}"
    "market-data-defi"       = "market-data-tick-defi-${local.deployment_env_short}-${var.project_id}"
    "market-data-sports"     = "market-data-tick-sports-${local.deployment_env_short}-${var.project_id}"
    "market-data-prediction" = "market-data-tick-pred-${local.deployment_env_short}-${var.project_id}"
    # Legacy buckets (no env-suffix) — MDPS launch scripts hardcode these names instead of
    # going through resolve_bucket_name. Added 2026-05-23 after ManifestReader reported
    # 101554s staleness on tradfi. Root cause: launch-mdps-sharded-backfill.sh line 205
    # `source_bucket="market-data-tick-${cat}-${PROJECT}"` — no DEPLOYMENT_ENV_SHORT.
    # QG STEP 5.69 violation: fix tracked in bucket_name_ssot_canonicalisation_2026_05_10.md
    # Phase 0f. Remove these entries once Phase 0f migrates MDPS to env-tiered buckets.
    "market-data-cefi-legacy"       = "market-data-tick-cefi-${var.project_id}"
    "market-data-tradfi-legacy"     = "market-data-tick-tradfi-${var.project_id}"
    "market-data-defi-legacy"       = "market-data-tick-defi-${var.project_id}"
    "market-data-sports-legacy"     = "market-data-tick-sports-${var.project_id}"
    "market-data-prediction-legacy" = "market-data-tick-prediction-${var.project_id}"
    # Legacy instruments-store buckets — launch-expected-universe-v2-vm.sh and other IS
    # scripts use `instruments-store-{category}-${PROJECT}` (no env suffix). Added 2026-05-23
    # after per-VM shard audit found cefi/tradfi/defi legacy shards with no consolidator_run_at
    # metadata. setup-data-pipeline-vm.sh covers these while running; crons provide persistence.
    # Remove once Phase 0f migrates IS scripts to env-tiered bucket names.
    "instruments-cefi-legacy"       = "instruments-store-cefi-${var.project_id}"
    "instruments-tradfi-legacy"     = "instruments-store-tradfi-${var.project_id}"
    "instruments-defi-legacy"       = "instruments-store-defi-${var.project_id}"
    "instruments-sports-legacy"     = "instruments-store-sports-${var.project_id}"
    "instruments-prediction-legacy" = "instruments-store-prediction-${var.project_id}"
  }

  # Per-category timeout override (seconds). Default 300 covers most categories
  # where the merge is < 30s. Heavy buckets (2M+ rows) need more headroom.
  #
  # 2026-05-05 sizing data:
  #   - instruments-store-sports:  37 shards / 2.09M rows / merges 60-80s
  #   - market-data-tick-cefi:   1243 shards / 2.22M rows / merges 70-90s
  # First bump (sports → 600s) was insufficient — observed 'Container terminated
  # on signal 9' mid-merge. Second bump: sports → 900s, plus cefi-market-data
  # added at 600s prophylactically since its shard count is 33x sports'.
  manifest_consolidator_timeouts = {
    "instruments-sports"        = 1800
    "market-data-sports"        = 1800
    "market-data-cefi"          = 1800
    "instruments-cefi"          = 1800
    "instruments-defi"          = 1800
    "market-data-defi"          = 1800
    "instruments-tradfi"        = 1800
    "market-data-tradfi"        = 1800
    "instruments-prediction"    = 1800
    "market-data-prediction"    = 1800
    # Legacy variants — same headroom as env-tiered equivalents.
    "market-data-tradfi-legacy"       = 1800
    "market-data-cefi-legacy"         = 1800
    "market-data-defi-legacy"         = 1800
    "market-data-prediction-legacy"   = 1800
    "market-data-sports-legacy"       = 1800
    "instruments-sports-legacy"       = 1800
    "instruments-cefi-legacy"         = 1800
    "instruments-defi-legacy"         = 1800
    "instruments-tradfi-legacy"       = 1800
    "instruments-prediction-legacy"   = 1800
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

  # 2026-05-06: bumped from 2Gi → 16Gi after the sports consolidator OOM
  # incident: 2.6M-row × 9M-input merge needs ~5-8GB working set during
  # pandas concat + dedup + parquet serialise. 2Gi caused every cycle to
  # SIGKILL mid-merge → canonical mtime stuck for 2h while readers
  # perpetually fell back to the slow 51-shard merge. Headroom for
  # manifest growth (every consolidator-instance pair shares this — sports
  # is the largest today; cefi/defi will catch up).
  #
  # Cloud Run gen2 memory ceilings: 1-2 vCPU → max 8Gi; 4 vCPU → max 16Gi;
  # 8 vCPU → max 32Gi. We pick 4 vCPU to unlock the 16Gi ceiling AND get
  # ~4× pandas-concat parallelism (multi-threaded BLAS / pyarrow). See
  # `feedback_manifest_consolidator_oom.md` (2026-05-06) for diagnosis.
  cpu             = "4"
  memory          = "16Gi"
  timeout_seconds = lookup(local.manifest_consolidator_timeouts, each.key, 300) # consolidation is a single read-list-merge-write cycle, ~5-30s typical; sports overridden to 600s for 2M+ row merges
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
