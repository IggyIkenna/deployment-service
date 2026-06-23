# DeFi Live Forward-Poll Scheduler — high-frequency (*/5) Cloud Scheduler → GCE VM
#
# Plan: data_completion_to_100_all_ag_2026_06_21.md (P1 "DeFi continuous live
#   market-data capture")
#
# What this file adds
#   Three Cloud Scheduler jobs (every 5 minutes, staggered) that each boot an
#   ephemeral defi-fwd-<op>-* VM running the MTDS DeFi handler in LIVE mode
#   (--mode live → pipeline_mode=live_*) for the PRICE-SENSITIVE data_types that
#   feed the live DeFi arb / paper engine:
#       collect-dex-swaps      DEX swap events  (TheGraph subgraph)
#       collect-dex-pools      DEX pool state   (TheGraph subgraph)
#       collect-oracle-prices  Chainlink/Pyth   (Alchemy RPC + Hermes)
#   The VM launch is the SAME instances.insert pattern as
#   batch_live_smoke_matrix_scheduler.tf (Cloud Scheduler http_target → Compute
#   Engine REST), and runs the SAME setup-data-pipeline-vm.sh generic VM_TASK
#   branch that the manual launcher scripts/vm/launch-defi-forward-poll.sh uses
#   (VM_OPERATION + VM_MODE=live read from metadata). Live = batch: identical
#   handler code path, only the pipeline_mode partition differs.
#
# Why VM-launch (not a Cloud Run Job like the daily batch in
#   defi_collection_scheduler.tf)
#   The forward-poll uses the per-VM-shard isolation convention
#   (MANIFEST_PER_VM_SHARDS=true + VM_NAME) + the consolidated-index staleness
#   override (MANIFEST_CONSOLIDATED_STALENESS_SEC=86400) that the existing
#   launch-defi-forward-poll.sh already wires through the VM metadata path. A VM
#   keeps that parity with the manual launcher (single code path, one debugging
#   surface) and is trivially cheap (e2-standard-8 self-deletes after ~15-30 min;
#   at */5 the runs do not overlap because the VM finishes well inside 5 min for
#   a single T-0 day).
#
# Why */5 (not per-block)
#   Near-real-time without per-block streaming infra. The price-sensitive ops
#   re-poll the current day every 5 minutes so the live engine sees fresh DEX /
#   oracle prices. The SLOW ops (lst-rates, lending-indices) stay on the daily
#   Cloud Run cadence in defi_collection_scheduler.tf — they do not need
#   high-frequency refresh.
#
# Singleton / overlap
#   The VM name is deterministic per op (defi-fwd-<op>-poll), so a still-running
#   prior VM makes the instances.insert return 409 Conflict and the new run is
#   skipped (the same desired behaviour as batch_live_smoke_matrix). This mirrors
#   the per-operation singleton lock in launch-defi-forward-poll.sh.
#
# Staggered minutes so the three TheGraph/Alchemy-keyed ops never insert in the
#   same second (they share per-key compute-unit budgets):
#       dex-swaps      */5         (:00 :05 :10 …)
#       dex-pools      1-59/5      (:01 :06 :11 …)
#       oracle-prices  2-59/5      (:02 :07 :12 …)
#
# Gating: var.enable_defi_forward_poll (default true). Set false to pause all
#   three jobs without editing this file.

locals {
  defi_fwd_zone        = "asia-northeast1-c"
  defi_fwd_startup_url = "gs://deployment-scripts-${var.project_id}/vm/setup-data-pipeline-vm.sh"

  # The price-sensitive DeFi forward-poll operations. Each maps to an MTDS
  # handler that accepts --mode live and tags pipeline_mode=live_* on its writes.
  # Staggered schedule keys off the shared TheGraph/Alchemy quota.
  defi_fwd_operations = var.enable_defi_forward_poll ? {
    "dex-swaps" = {
      operation   = "collect-dex-swaps"
      schedule    = "*/5 * * * *"
      description = "DeFi LIVE collect-dex-swaps every 5 min — DEX swap events via TheGraph (pipeline_mode=live_onchain_subgraph)."
    }
    "dex-pools" = {
      operation   = "collect-dex-pools"
      schedule    = "1-59/5 * * * *"
      description = "DeFi LIVE collect-dex-pools every 5 min — DEX pool state via TheGraph (pipeline_mode=live_onchain_subgraph)."
    }
    "oracle-prices" = {
      operation   = "collect-oracle-prices"
      schedule    = "2-59/5 * * * *"
      description = "DeFi LIVE collect-oracle-prices every 5 min — Chainlink/Pyth via Alchemy RPC (pipeline_mode=live_chainlink / live_pyth_hermes)."
    }
  } : {}
}

resource "google_cloud_scheduler_job" "defi_forward_poll" {
  for_each = local.defi_fwd_operations

  name        = "defi-fwd-${each.key}-${local.deployment_env_short}"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${local.defi_fwd_zone}/instances"
    http_method = "POST"
    body = base64encode(jsonencode({
      # Deterministic name → a still-running prior VM returns 409 Conflict on
      # insert (per-op singleton; the new tick is skipped, never double-runs).
      name        = "defi-fwd-${each.key}-poll"
      zone        = "projects/${var.project_id}/zones/${local.defi_fwd_zone}"
      machineType = "zones/${local.defi_fwd_zone}/machineTypes/e2-standard-8"
      disks = [{
        boot = true
        initializeParams = {
          sourceImage = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
          diskSizeGb  = "50"
        }
        autoDelete = true
      }]
      networkInterfaces = [{ accessConfigs = [{ type = "ONE_TO_ONE_NAT" }] }]
      serviceAccounts = [{
        email  = google_service_account.t1_batch.email
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
      }]
      metadata = {
        items = [
          { key = "startup-script-url", value = local.defi_fwd_startup_url },
          # Generic VM_TASK → setup-data-pipeline-vm.sh runs
          #   --operation $VM_OPERATION --mode $VM_MODE --asset-group $VM_ASSET_GROUP
          { key = "VM_TASK", value = "defi-live-${each.key}" },
          { key = "VM_SERVICE", value = "market_tick_data_service" },
          { key = "VM_OPERATION", value = each.value.operation },
          { key = "VM_MODE", value = "live" },
          { key = "VM_ASSET_GROUP", value = "DEFI" },
          { key = "DEPLOYMENT_ENV", value = var.environment },
          # Daily catalog → use the consolidated index (avoids OOM on per-VM
          # shard merge) and write per-VM shards for isolation.
          { key = "MANIFEST_CONSOLIDATED_STALENESS_SEC", value = "86400" },
          { key = "MANIFEST_PER_VM_SHARDS", value = "true" },
          { key = "VM_SHUTDOWN_ON_COMPLETION", value = "true" },
          { key = "GCP_PROJECT_ID", value = var.project_id },
          { key = "WORKSPACE_ROOT", value = "/home/unified/workspace" },
        ]
      }
      labels = { purpose = "defi-forward-poll", operation = each.key, env = var.environment }
    }))

    oauth_token {
      service_account_email = google_service_account.t1_batch.email
    }
  }

  retry_config {
    retry_count = 0
  }

  depends_on = [google_project_iam_member.t1_batch_compute_instance_admin]
}
