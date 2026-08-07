# DeFi Daily Collection — Cloud Run Jobs + Cloud Scheduler Crons
#
# Plan: residual_defi_pipeline_completion_2026_04_25
#
# Background
#   The MTDS DeFi manifest stayed 682+ days stale because Cloud Scheduler's
#   pre-existing "${env_prefix}-market-tick-data-service-fast-t1-recon"
#   target invokes "--operation run --mode batch --asset-group DEFI", which the
#   orchestrator (`engine/orchestrator.py`) explicitly skips for every DeFi
#   venue. The actual DeFi work happens via 14 dedicated `collect-*` ops
#   registered in `cli/main.py` — none of which had a scheduler entry.
#
#   This file declares both halves of the chain (Cloud Run Job spec + Cloud
#   Scheduler cron) per operation, so cutting them on/off is single-PR.
#
# Why 14 jobs (not 1 fan-out wrapper)
#   * Failure isolation — a stuck Subgraph endpoint must not gate gas-fee
#     collection.
#   * Per-job CPU/memory tuning — `dex-pools` writes 64k+ rows/day and
#     `gas-fees` does per-block scans; profile differs by an order of
#     magnitude.
#   * Per-op rate-limit / retry — TheGraph keys, Alchemy RPC, and DeFiLlama
#     have separate quotas.
#   * Per-op logs/alerting — Cloud Logging filter by resource.labels.job_name.
#   * Cost is trivial — Cloud Scheduler is $0.10/job/month → $1.40/month for
#     all 14.
#
# Stagger schedule. Historically framed as "must finish by 02:25 UTC so
# features-onchain T+1 at 02:30 UTC sees fresh manifest entries" (citing
# t1_batch_scheduler.tf:124-128) — that citation was STALE: the consumer it
# pointed at, the "features-onchain" T+1 recon Cloud Scheduler job
# (t1_batch_scheduler.tf's now-removed "features-onchain" map entry, which
# created uts-{prod,dev,staging}-features-onchain-service-t1-recon), was
# deliberately DELETED 2026-07-13 per explicit operator ruling —
# deployment-service@b13f79b (terraform prune) + full cloud-side decommission,
# see plans/archive/issues/features_onchain_image_pipeline_gap_2026_07_13.md —
# and never replaced (confirmed 2026-07-28: zero features-onchain/T+1-recon
# entries anywhere in t1_batch_scheduler.tf or the wider terraform/gcp tree).
#
# RESOLUTION (2026-07-28, plans/active/issues/defi_t1_freshness_deadline_stagger_2026_07_28.md
# P2 item 2): since no live T+1 job depends on a hard 02:25/02:30 UTC cutover,
# option (b) was taken over re-staggering — collect-solana-defi (worst-case
# finish 02:30 UTC) and collect-bridge-events (worst-case finish 02:35 UTC,
# the tightest of the 14 jobs) are left on their current schedule/timeout
# unchanged. The times below are kept purely as a historical stagger (so no
# two TheGraph-keyed ops overlap and rate-limit each other), not an enforced
# deadline. This also resolves the 2026-07-22 residual_defi_pipeline_completion
# follow-up flag that solana-defi alone was already tight at ~02:30 — no live
# consumer was ever actually left unprotected by that tightness. Remaining
# follow-up (tracked separately, not this todo): re-check
# collect-lst-seasonal-rewards's 02:25 UTC start against this same resolution.
#   00:00 collect-gas-fees           Alchemy RPC per-block (heavy network)
#   00:05 collect-oracle-prices      Chainlink RPC (multi-chain)
#   00:15 collect-dex-pools          TheGraph subgraph (heavy memory)
#   00:30 collect-dex-swaps          TheGraph subgraph (heavy memory)
#   00:45 collect-lending-indices    Aave/Compound RPC + Subgraph
#   00:50 collect-risk-params        Aave/Spark/Compound-V3 Subgraph + IS-catalogue fallback (Morpho/Fluid/Solana)
#   01:00 collect-lst-rates          Lido / RocketPool / Mantle RPC
#   01:10 collect-vault-share-price  ERC-4626 convertToAssets (Yearn/Ethena/Maker/Frax/Morpho)
#   01:15 collect-perp-funding       Hyperliquid / Kalshi-Perp / Polymarket-Perp REST
#   01:30 collect-liquidations       Aave subgraph
#   01:35 collect-liquidation-events Aave V3 + Morpho LiquidationCall events via TheGraph subgraph
#   01:45 collect-eigenlayer-rewards EigenLayer RPC
#   01:50 collect-staking-yields     Lido / EtherFi / EigenLayer public REST APIs
#   01:55 collect-evm-defi           Multi-source aggregate
#   02:05 collect-solana-defi        Solana RPC + DeFiLlama yields
#   02:10 collect-mev-events         Flashbots relay API (MEV-Boost stats)
#   02:15 collect-bridge-events      ACROSS + STARGATE via Alchemy eth_getLogs
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
  # Single image for all 14 ops; CLI dispatches via `--operation collect-X`.
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
    "risk-params" = {
      schedule    = "50 0 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1800
      description = "DeFi collect-risk-params — per-market reserve config (ltv/liquidation_threshold/liquidation_bonus/reserve_factor/emode_category_id) via Aave/Spark/Compound-V3 subgraph + IS-catalogue fallback (Morpho/Fluid/Solana)."
    }
    "lst-rates" = {
      schedule = "0 1 * * *"
      # 4Gi -> 8Gi / 1 CPU -> 2 CPU (defi_hyperliquid_residual_manifest_rows_2026_08_04.md P2
      # backfill 2026-08-07): single-date historical backfill runs (2026-08-01/02/03 via REST API
      # override, execution f8m4d/q9mkx/vqg8n) OOM-killed at 4Gi with 1 CPU — Cloud Run constraint:
      # >4Gi requires >=2 CPU. Daily cron at 4Gi/1CPU succeeded for recent dates (yesterday); only
      # historical-date backfills exceeded 4Gi. `gcloud run jobs update --cpu=2 --memory=8Gi`
      # applied live 2026-08-07T20:20Z — this syncs IaC so `terraform apply` doesn't revert.
      cpu         = "2"
      # 2Gi -> 4Gi (defi_mtds_lst_rates_cloud_run_job_oom_2026_08_04.md): every scheduled run
      # 2026-08-02 through 2026-08-05 OOM-killed ("The configured memory limit was reached")
      # inside ManifestFreshnessCache.bulk_load()'s background warmup thread
      # (_gas_fee_helpers.bounded_freshness_warmup) racing the handler's own EVM/Solana RPC
      # fetch work against the now ~42M-row defi availability index — RSS measured at ~2040MiB
      # (at the 2Gi cgroup ceiling) moments before each OOM-kill. An ad-hoc `gcloud run jobs
      # update --memory=4Gi` + manual execution (ikenna@odum-research.com, 2026-08-05T16:14 UTC,
      # execution uts-prod-mtds-collect-lst-rates-b5f4t) already verified this resolves it live
      # (completed successfully in 2m53s, all 15 LST venues written) — this bump syncs the IaC
      # source of truth so the next `terraform apply` doesn't drift-revert that live fix back to
      # the OOM-ing 2Gi. Matches dex-pools/dex-swaps/evm-defi's existing 4Gi for the same class
      # of freshness/manifest-heavy defi collection job.
      memory      = "8Gi"
      timeout     = 1200
      description = "DeFi collect-lst-rates — 11 EVM LST exchange rates (Lido stETH, RocketPool rETH, etc.) at historical block. Bumped 2Gi->4Gi 2026-08-05: OOM on cron 2026-08-02..08-05. Bumped 4Gi/1CPU->8Gi/2CPU 2026-08-07: historical backfill (2026-08-01..03) OOM at 4Gi/1CPU (Cloud Run limit); 2CPU required for >4Gi. See defi_hyperliquid_residual_manifest_rows_2026_08_04.md."
    }
    "vault-share-price" = {
      schedule    = "10 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 900
      description = "DeFi collect-vault-share-price — ERC-4626 convertToAssets snapshots (Yearn V3, Ethena, Maker sDAI, Frax sFRAX, Morpho MetaMorpho USDC)."
    }
    "perp-funding" = {
      schedule    = "15 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-perp-funding — Hyperliquid + Kalshi-Perp + Polymarket-Perp perpetual funding rates (dYdX never implemented; GMX removed 2026-07-25 — see perp_funding_handler.py docstring)."
    }
    "liquidations" = {
      schedule    = "30 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-liquidations — Aave + Compound liquidation events via subgraph."
    }
    "liquidation-events" = {
      schedule    = "35 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1500
      description = "DeFi collect-liquidation-events — Aave V3 LiquidationCall + Morpho BadDebt/LiquidationEvent on-chain events via TheGraph subgraph."
    }
    "eigenlayer-rewards" = {
      schedule    = "45 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-eigenlayer-rewards — restaking reward claims per operator + AVS."
    }
    "staking-yields" = {
      schedule    = "50 1 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-staking-yields — protocol-REST staking APY (Lido, EtherFi, EigenLayer public APIs/DefiLlama)."
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
    "mev-events" = {
      schedule    = "10 2 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 900
      description = "DeFi collect-mev-events — MEV-Boost relay stats via Flashbots relay API."
    }
    "bridge-events" = {
      schedule    = "15 2 * * *"
      cpu         = "1"
      memory      = "2Gi"
      timeout     = 1200
      description = "DeFi collect-bridge-events — ACROSS + STARGATE transfer events via Alchemy eth_getLogs."
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
  # Iterate the resource's own in-state instances (not local.defi_collect_operations keys) so the
  # graph still evaluates when an instance is transiently absent from state (e.g. mid `import`).
  value = {
    for op_key, job in module.defi_collect_job :
    op_key => job.name
  }
}

output "defi_collect_cron_names" {
  description = "Map of operation → Cloud Scheduler cron name (for `gcloud scheduler jobs run` ad-hoc fires)."
  # See defi_collect_job_names — iterate in-state instances for partial-state resilience.
  value = {
    for op_key, cron in google_cloud_scheduler_job.defi_collect_cron :
    op_key => cron.name
  }
}
