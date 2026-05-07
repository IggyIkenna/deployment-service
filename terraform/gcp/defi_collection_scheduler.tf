# DeFi Daily Collection — Cloud Run Jobs + Cloud Scheduler Crons
#
# Plan: residual_defi_pipeline_completion_2026_04_25
#
# Background
#   The MTDS DeFi manifest stayed 682+ days stale because Cloud Scheduler's
#   pre-existing "${env_prefix}-market-tick-data-service-fast-t1-recon"
#   target invokes "--operation run --mode batch --asset-group DEFI", which the
#   orchestrator (`engine/orchestrator.py`) explicitly skips for every DeFi
#   venue. The actual DeFi work happens via 11 dedicated `collect-*` ops
#   registered in `cli/main.py` — none of which had a scheduler entry.
#
#   This file declares both halves of the chain (Cloud Run Job spec + Cloud
#   Scheduler cron) per operation, so cutting them on/off is single-PR.
#
# Why 11 jobs (not 1 fan-out wrapper)
#   * Failure isolation — a stuck Subgraph endpoint must not gate gas-fee
#     collection.
#   * Per-job CPU/memory tuning — `dex-pools` writes 64k+ rows/day and
#     `gas-fees` does per-block scans; profile differs by an order of
#     magnitude.
#   * Per-op rate-limit / retry — TheGraph keys, Alchemy RPC, and DeFiLlama
#     have separate quotas.
#   * Per-op logs/alerting — Cloud Logging filter by resource.labels.job_name.
#   * Cost is trivial — Cloud Scheduler is $0.10/job/month → $1.10/month for
#     all 11.
#
# Stagger schedule (must finish by 02:25 UTC so features-onchain T+1 at
# 02:30 UTC sees fresh manifest entries — see t1_batch_scheduler.tf:124-128)
#   00:00 collect-gas-fees           Alchemy RPC per-block (heavy network)
#   00:05 collect-oracle-prices      Chainlink RPC (multi-chain)
#   00:15 collect-dex-pools          TheGraph subgraph (heavy memory)
#   00:30 collect-dex-swaps          TheGraph subgraph (heavy memory)
#   00:45 collect-lending-indices    Aave/Compound RPC + Subgraph
#   01:00 collect-lst-rates          Lido / RocketPool / Mantle RPC
#   01:15 collect-perp-funding       Hyperliquid / dYdX / GMX REST
#   01:30 collect-liquidations       Aave subgraph
#   01:45 collect-eigenlayer-rewards EigenLayer RPC
#   01:55 collect-evm-defi           Multi-source aggregate
#   02:05 collect-solana-defi        Solana RPC + DeFiLlama yields
#
# Why two service accounts
#   * `t1_batch_sa` (declared in t1_batch_scheduler.tf:40-51) — used by
#     Cloud Scheduler to invoke Cloud Run Jobs. Already has roles/run.invoker.
#   * `unified_trading` (declared in main.tf:1421-1455) — used by the
#     container at runtime. Has roles/storage.objectAdmin (DeFi buckets) +
#     roles/secretmanager.secretAccessor (alchemy / thegraph / coinglass keys)
#     + roles/pubsub.editor.
#
# Failure detection
#   The container itself emits `record_failed` rows to the v5 manifest with
#   error_reason classified via UAC `classify_venue_error()`. Scheduler retry
#   config below handles transient 5xx (Cloud Run start-up errors); silent
#   handler exits with `attempted_failed` rows are detected by the
#   alerting-service drilldown of `_index/availability_index.parquet`.

locals {
  # MTDS image — published by `market-tick-data-service/cloudbuild.yaml`
  # (`_REGISTRY_REPO=unified-trading-system`, `_SERVICE_NAME=market-tick-data-service`).
  # Single image for all 11 ops; CLI dispatches via `--operation collect-X`.
  mtds_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  # Per-operation tuning. CPU/memory + timeout sized from the production VM
  # logs captured during the 2026-04-23 backfill fleet (dex-pools writes 7k+
  # rows per day across 25 protocol/chain shards; gas-fees scans 8 chains'
  # blocks per day; oracle-prices queries 6-23 Chainlink feeds per chain
  # per day). Schedule staggered so no two TheGraph-keyed ops fire in the
  # same minute — TheGraph key sharding still rate-limits the bucket.
  defi_collect_operations = {
    "gas-fees" = {
      schedule    = "0 0 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1800
      description = "DeFi collect-gas-fees — per-block gas snapshots across 8 EVM chains via Alchemy RPC."
    }
    "oracle-prices" = {
      schedule    = "5 0 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-oracle-prices — Chainlink price feeds across ETHEREUM/ARBITRUM/BASE/OPTIMISM/POLYGON."
    }
    "dex-pools" = {
      schedule    = "15 0 * * *"
      cpu         = "2"
      memory      = "4Gi"
      timeout     = 2400
      description = "DeFi collect-dex-pools — UniV3 + Sushi + Pancake + Aerodrome + Camelot + Balancer + Curve + GMX pool snapshots via TheGraph."
    }
    "dex-swaps" = {
      schedule    = "30 0 * * *"
      cpu         = "2"
      memory      = "4Gi"
      timeout     = 2400
      description = "DeFi collect-dex-swaps — daily swap aggregates per pool via TheGraph (paginated 5000 rows / pool / chain)."
    }
    "lending-indices" = {
      schedule    = "45 0 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1800
      description = "DeFi collect-lending-indices — Aave/Compound/Morpho borrow/supply rates + utilization."
    }
    "lst-rates" = {
      schedule    = "0 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-lst-rates — 11 EVM LST exchange rates (Lido stETH, RocketPool rETH, etc.) at historical block."
    }
    "perp-funding" = {
      schedule    = "15 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-perp-funding — Hyperliquid + dYdX + GMX perpetual funding rates."
    }
    "liquidations" = {
      schedule    = "30 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-liquidations — Aave + Compound liquidation events via subgraph."
    }
    "eigenlayer-rewards" = {
      schedule    = "45 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-eigenlayer-rewards — restaking reward claims per operator + AVS."
    }
    "evm-defi" = {
      schedule    = "55 1 * * *"
      cpu         = "2"
      memory      = "4Gi"
      timeout     = 1500
      description = "DeFi collect-evm-defi — multi-source aggregate (DeFiLlama + Subgraph) for any chain not covered by op-specific jobs."
    }
    "solana-defi" = {
      schedule    = "5 2 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-solana-defi — Marinade / Jito / Orca / Raydium / Kamino / Marginfi / Solend via Solana RPC + DeFiLlama."
    }
  }
}

# -------------------------------------------------------
# Cloud Run Jobs — one per `collect-*` operation
# -------------------------------------------------------
module "defi_collect_job" {
  for_each = local.defi_collect_operations
  source   = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-mtds-collect-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.mtds_image
  cpu                   = each.value.cpu
  memory                = each.value.memory
  timeout_seconds       = each.value.timeout
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # Module-level command override — CLI dispatches the right handler.
  args = ["--operation", "collect-${each.key}", "--mode", "batch"]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
    # Manifest-429 per-VM sharding: every Cloud Run Job container writes
    # to `_index/per_vm/{instance}.parquet` instead of CAS-fighting on the
    # single canonical blob. Reader fallback in UTL keeps the
    # deployment-api UI coherent until the consolidator (declared in
    # manifest_consolidator_scheduler.tf) merges shards every minute.
    MANIFEST_PER_VM_SHARDS = "true"
  }

  service_name = "market-tick-data-service"
  environment  = var.environment

  # Note: container-job module already adds {managed-by, service, environment}
  # — only pass operation-specific extras here to avoid label-key collisions
  # (see the merge() call in modules/container-job/gcp/main.tf:102-109).
  labels = {
    "purpose"   = "defi-collection"
    "operation" = each.key
  }
}

# -------------------------------------------------------
# Cloud Scheduler crons — one per `collect-*` operation
# -------------------------------------------------------
# Invokes the Cloud Run Job declared above. Reuses the existing t1_batch_sa
# (granted roles/run.invoker in t1_batch_scheduler.tf) so no new IAM is
# needed.
resource "google_cloud_scheduler_job" "defi_collect_cron" {
  for_each = local.defi_collect_operations

  name        = "${local.env_prefix}-mtds-collect-${each.key}-cron"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.defi_collect_job[each.key].name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # One retry on transient 5xx (Cloud Run cold-start can throw 503).
  # Container-side retries (handler-level) are configured separately.
  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

# -------------------------------------------------------
# Outputs — for verification scripts and dashboards
# -------------------------------------------------------
output "defi_collect_job_names" {
  description = "Map of operation → Cloud Run Job name (for `gcloud run jobs execute` smoke tests)."
  value = {
    for op_key, _ in local.defi_collect_operations :
    op_key => module.defi_collect_job[op_key].name
  }
}

output "defi_collect_cron_names" {
  description = "Map of operation → Cloud Scheduler cron name (for `gcloud scheduler jobs run` ad-hoc fires)."
  value = {
    for op_key, _ in local.defi_collect_operations :
    op_key => google_cloud_scheduler_job.defi_collect_cron[op_key].name
  }
}
