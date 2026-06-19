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

  # Per-category memory/cpu override. Default 4 vCPU / 16Gi covers buckets whose
  # per-VM shard set merges within 16Gi working set (~1600 shards). Heavy buckets
  # exceed that and SIGKILL (signal 9 / OOM) mid-merge — per-VM shards are never
  # pruned after consolidation, so the working set grows unboundedly with every
  # backfill VM. Cloud Run gen2 ceilings: 4 vCPU → 16Gi; 8 vCPU → 32Gi.
  #
  # 2026-05-26 OOM incident: market-data-tick-cefi (flat legacy) accumulated 2099
  # per-VM shards from active CeFi backfill VMs → consolidator OOM-killed every
  # cycle at 16Gi → canonical availability_index.parquet stuck 36h stale despite
  # scheduler firing every minute. market-data-tick-tradfi (1621 shards) was next
  # in line. Bumped both to 8 vCPU / 32Gi. Default stays 4/16 for the rest.
  manifest_consolidator_cpu = {
    "market-data-cefi-legacy"   = "8"
    "market-data-tradfi-legacy" = "8"
  }
  manifest_consolidator_memory = {
    "market-data-cefi-legacy"   = "32Gi"
    "market-data-tradfi-legacy" = "32Gi"
  }
  # DuckDB's own memory_limit (CONSOLIDATOR_DUCKDB_MEMORY_LIMIT) is deliberately
  # set BELOW the container memory (code default 8GB for the 16Gi tier). Bumping
  # ONLY the container does nothing — DuckDB still caps at 8GB and spills merge
  # state to tmpfs, which itself consumes container RAM. For the 32Gi tier we
  # raise DuckDB to 24GB (leaving ~8GB for Python/IO/tmpfs) so the heavy
  # catch-up merge runs in-memory and completes inside the 90s soft-lock TTL.
  manifest_consolidator_duckdb_memory = {
    "market-data-cefi-legacy"   = "24GB"
    "market-data-tradfi-legacy" = "24GB"
  }

  # Phase D — derived-data buckets (Group B naming: flat — env-split ROLLED BACK per
  # cloud-providers.yaml comment "Drop ${DEPLOYMENT_ENV_SHORT}- for ALL Group B kinds".
  # Exception: features-sports + features-calendar remain env-tiered per yaml SSOT.
  # Re-enable env-split when bucket_env_split_rollout_2026_06.md Phase 1 provisions + migrates.)
  manifest_consolidator_buckets_extended = {
    "features-delta-one-cefi"   = "features-delta-one-cefi-${var.project_id}"
    "features-delta-one-tradfi" = "features-delta-one-tradfi-${var.project_id}"
    "features-delta-one-defi"   = "features-delta-one-defi-${var.project_id}"
    "features-volatility-cefi"  = "features-volatility-cefi-${var.project_id}"
    "features-volatility-tradfi" = "features-volatility-tradfi-${var.project_id}"
    "features-onchain-cefi"     = "features-onchain-cefi-${var.project_id}"
    "features-onchain-defi"     = "features-onchain-defi-${var.project_id}"
    "features-sports"           = "features-sports-${local.deployment_env_short}-${var.project_id}"
    "features-calendar"         = "features-calendar-${local.deployment_env_short}-${var.project_id}"
    # strategy-store consolidated to a single flat bucket (D6 Phase 4, 2026-05-20)
    "strategy"                  = "strategy-store-${var.project_id}"
    "execution-cefi"            = "execution-store-cefi-${var.project_id}"
    "execution-tradfi"          = "execution-store-tradfi-${var.project_id}"
    "execution-defi"            = "execution-store-defi-${var.project_id}"
    "ml-training-artifacts"     = "ml-training-artifacts-${var.project_id}"
    # gas-fees reference bucket (flat name, Group B). Added 2026-06-19 after the
    # mtds-gas-fees backfill crashed with ManifestConsolidatorStaleError: the
    # collector writes per-VM shards here + read-preflights via
    # assert_consolidator_healthy(), but NO consolidator job covered this bucket
    # → the index was always stale → every gas-fees backfill VM died ~4min in.
    # Small bucket (~13 _index shards) → default 4vCPU/16Gi/300s is ample.
    "gas-fees"                  = "gas-fees-${var.project_id}"
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
  cpu             = lookup(local.manifest_consolidator_cpu, each.key, "4")     # 8 vCPU for heavy buckets (>1600 shards) to unlock 32Gi ceiling
  memory          = lookup(local.manifest_consolidator_memory, each.key, "16Gi") # 32Gi for heavy buckets; default 16Gi handles ~1600 shards
  timeout_seconds = lookup(local.manifest_consolidator_timeouts, each.key, 300) # consolidation is a single read-list-merge-write cycle, ~5-30s typical; sports overridden to 600s for 2M+ row merges
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # CLI entrypoint exposed by `manifest_consolidator.main()`.
  command = ["python"]
  args    = ["-m", "unified_trading_library.manifest_consolidator", "--bucket", each.value]

  # Heavy buckets get a raised DuckDB memory_limit (see local map); default
  # buckets fall back to the code default (8GB) by omitting the env var.
  environment_variables = merge(
    {
      GCP_PROJECT_ID = var.project_id
      DEPLOYMENT_ENV = var.environment
      CLOUD_PROVIDER = "gcp"
    },
    contains(keys(local.manifest_consolidator_duckdb_memory), each.key) ? {
      CONSOLIDATOR_DUCKDB_MEMORY_LIMIT = local.manifest_consolidator_duckdb_memory[each.key]
    } : {},
  )

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

# -------------------------------------------------------
# Phase D — derived-data buckets (Group B: features, strategy, execution, ml)
# -------------------------------------------------------

module "manifest_consolidator_job_extended" {
  for_each = local.manifest_consolidator_buckets_extended
  source   = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-manifest-consolidator-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  cpu             = "4"
  memory          = "16Gi"
  timeout_seconds = 1800
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

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

resource "google_cloud_scheduler_job" "manifest_consolidator_cron_extended" {
  for_each = local.manifest_consolidator_buckets_extended

  name        = "${local.env_prefix}-manifest-consolidator-${each.key}-cron"
  description = "Consolidate per-VM manifest shards in ${each.value} into the canonical _index/availability_index.parquet."
  schedule    = "*/1 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.manifest_consolidator_job_extended[each.key].name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}
