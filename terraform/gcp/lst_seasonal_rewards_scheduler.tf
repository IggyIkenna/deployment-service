# LST Seasonal Rewards Daily Collection — Cloud Run Job + Cloud Scheduler
#
# Plan: leveraged_leg_controller_2026_05_01 (Phase 6 — restaking-reward
# realisation pipeline)
#
# What this schedules
#   Once per day, scan every distributor in
#   ``unified_api_contracts.internal.architecture_v2.LST_REWARD_STREAMS``
#   for Transfer / claim events on the previous day, normalise into
#   ``SeasonalRewardEvent`` rows, and write the canonical
#   ``lst_seasonal_rewards`` parquet partitioned by issuer + chain via
#   ``OnChainFeatureWriter.write_seasonal_rewards``.
#
#   Strategy-service's per-tick driver (Phase6Driver) reads the parquet
#   through ``ParquetDustLoader`` to install ``DustToken`` baskets on
#   each engine; the dust-router then realises rewards through the
#   matching engine and feeds the fills back to the LegController for
#   target-net-delta-preserving cash sweeps.
#
# Why a separate scheduler entry (not a fan-out under defi_collect)
#   * Different image — features-service (onchain family), not market-tick-data-service.
#   * Different cadence — fires AFTER all the upstream DeFi collection
#     ops finish (the dex-pools / oracle-prices / lst-rates jobs in
#     defi_collection_scheduler.tf complete by 02:05 UTC) and BEFORE the
#     features-onchain-service T+1 recon at 02:30 UTC, so the recon
#     pass picks up the fresh ``lst_seasonal_rewards/by_date/day=...``
#     parquet for the previous day.
#   * Different secret surface — needs Web3 RPCs (Alchemy ETH / Base /
#     Arbitrum / Optimism) + Solana RPC + venue API keys via
#     ApiKeyReloader. The runtime SA `unified_trading` already has
#     roles/secretmanager.secretAccessor; no new IAM needed.
#
# Stagger relative to the rest of the daily DAG
#   00:00–02:05  defi_collection_scheduler.tf — 11 collect-* ops fan out
#   02:05–02:25  buffer (gives the heaviest collect ops headroom)
#   02:25 UTC    THIS — collect-lst-seasonal-rewards (fires at 02:25)
#   02:30 UTC    features-onchain T+1 recon (t1_batch_scheduler.tf:131-135)
#   03:00 UTC    ml-inference T+1
#   04:00 UTC    strategy T+1
#
# How the daily collector chooses which day to scan
#   The script's ``--date`` flag accepts an explicit ISO date. When the
#   Cloud Scheduler invokes the Cloud Run Job, the args below pass
#   ``--date $(yesterday-utc)`` so we always pick up the just-closed UTC
#   day. The collector's `block_range_resolver` in
#   ``lst_rewards_bootstrap.py`` then maps that day to per-chain
#   block ranges via UAC chain_env metadata.
#
# Failure detection
#   The collector emits ``ADAPTER_FETCH_FAILED`` events through UAC
#   ``classify_venue_error()`` per shard; chain-level failures don't
#   gate other chains (D10 shard isolation). Cloud Scheduler retry
#   handles transient 5xx Cloud Run start-up errors. Persistent failures
#   surface in ``_index/availability_index.parquet`` for the alerting-
#   service drilldown.

locals {
  # features-service image — published by `features-service/cloudbuild.yaml`
  # (trigger `features-service-build`) to the shared `unified-trading-system`
  # Artifact Registry repo. The onchain family lives INSIDE features-service
  # (`features_service.onchain`) since the features repo consolidation —
  # the standalone `features-onchain-service` repo was archived 2026-05-08
  # (see unified-trading-pm plans/archive/issues/
  # features_repo_consolidation_preaudit_2026_05_08.md) and its image name
  # `unified-trading-system/features-onchain-service:latest` never had a
  # producer, so every cron fire failed with INVALID_ARGUMENT until this
  # was repointed (2026-07-13, issue doc
  # features_onchain_image_pipeline_gap_2026_07_13.md).
  #
  # NB: per-service standalone Terraform under
  # `terraform/services/features-onchain-service/gcp/terraform.tfvars`
  # uses a DIFFERENT path (`features-onchain-service/features-onchain-service:latest`)
  # which is a stale convention pre-dating the shared-repo migration —
  # don't follow that pattern here.
  features_onchain_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/features-service:latest"

  # Single op, so just one entry. Modelled as a map for symmetry with
  # defi_collect_operations + future-proofing if more reward-realisation
  # collectors land (per-protocol airdrop scanners, etc.).
  lst_rewards_operations = {
    "lst-seasonal-rewards" = {
      schedule    = "25 2 * * *"
      cpu         = "2"
      memory      = "4Gi"
      timeout     = 2400
      description = "LST seasonal rewards — scan every distributor in LST_REWARD_STREAMS for the previous UTC day's Transfer / claim events and write to lst_seasonal_rewards/by_date/."
    }
  }
}

# -------------------------------------------------------
# Cloud Run Job — daily LST seasonal-rewards collection
# -------------------------------------------------------
module "lst_rewards_collect_job" {
  for_each = local.lst_rewards_operations
  source   = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-features-onchain-collect-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email
  image                 = local.features_onchain_image
  cpu                   = each.value.cpu
  memory                = each.value.memory
  timeout_seconds       = each.value.timeout
  max_retries           = 1
  parallelism           = 1
  task_count            = 1

  # Override the image entrypoint to fire the standalone daily script.
  # The script lives outside the main CLI on purpose — Phase 6 daily
  # collection is a one-shot batch job, not an asset_group operation
  # that fits the `--operation/--mode/--asset-group` matrix.
  #
  # `--date` is filled by the wrapper shell at runtime; passing it
  # directly here would freeze the date at terraform-apply time.
  # Script path is the consolidated features-service layout
  # (`scripts/onchain/…`, not the retired standalone repo's `scripts/…`).
  # Invoked by FILE PATH, not `python -m scripts.…` — the editable-installed
  # unified-api-contracts sibling also exposes a top-level `scripts` package,
  # so the `-m` form resolves against the WRONG repo's scripts dir.
  command = ["/bin/bash", "-lc"]
  args = [
    "python scripts/onchain/collect_lst_seasonal_rewards_daily.py --date $(date -u -d 'yesterday' +%Y-%m-%d) --project-id ${var.project_id} --env mainnet"
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
    # The collector writes through the same v5 manifest infrastructure
    # MTDS uses; per-VM sharding keeps it from CAS-fighting any concurrent
    # write (the `_index/per_vm/{instance}.parquet` partial gets merged
    # by the consolidator in manifest_consolidator_scheduler.tf).
    MANIFEST_PER_VM_SHARDS = "true"
  }

  service_name = "features-onchain-service"
  environment  = var.environment

  # container-job module already sets {managed-by, service, environment}
  # — only pass operation-specific labels here.
  labels = {
    "purpose"   = "lst-reward-realisation"
    "operation" = each.key
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — fires the Cloud Run Job daily
# -------------------------------------------------------
# Reuses the t1_batch_sa (declared in t1_batch_scheduler.tf, granted
# roles/run.invoker) — no new IAM bindings required.
resource "google_cloud_scheduler_job" "lst_rewards_collect_cron" {
  for_each = local.lst_rewards_operations

  name        = "${local.env_prefix}-features-onchain-collect-${each.key}-cron"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.lst_rewards_collect_job[each.key].name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # One retry on transient 5xx (Cloud Run cold-start can throw 503).
  # Per-shard retries inside the collector are handled by
  # `classify_venue_error()` + `ADAPTER_FETCH_FAILED` events.
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
output "lst_rewards_collect_job_names" {
  description = "Map of operation → Cloud Run Job name (for `gcloud run jobs execute` smoke tests)."
  value = {
    for op_key, _ in local.lst_rewards_operations :
    op_key => module.lst_rewards_collect_job[op_key].name
  }
}

output "lst_rewards_collect_cron_names" {
  description = "Map of operation → Cloud Scheduler cron name (for `gcloud scheduler jobs run` ad-hoc fires)."
  value = {
    for op_key, _ in local.lst_rewards_operations :
    op_key => google_cloud_scheduler_job.lst_rewards_collect_cron[op_key].name
  }
}
