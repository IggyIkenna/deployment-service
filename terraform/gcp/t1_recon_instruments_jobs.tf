# instruments-service per-AG T+1 recon Cloud Run JOBS — codified into IaC.
#
# The schedulers that TRIGGER these jobs are already terraformed
# (t1_batch_scheduler.tf, keys "instruments-defi"/"instruments-tradfi"/
# "instruments-prediction"/"instruments-cefi"), but the JOB DEFINITIONS
# themselves (image/args/resources/env) were only ever created imperatively
# (a one-off `gcloud run jobs create`) — no tracked source. This is exactly
# how the cefi fixed-date-range drift and the missing all-AG producer went
# invisible for weeks (instruments_cefi_g1_g5_gate_execution_2026_07_24.md):
# nothing in the repo could show what a job's args/env actually were, or
# diff them against intent.
#
# Specs below were read verbatim off the live jobs
# (`gcloud run jobs describe uts-prod-instruments-service-<ag>-t1-recon`,
# 2026-07-26) so importing these resources into state produces ZERO plan
# diff — this file is descriptive of current reality, not a re-design.
# Each AG's args/resources/env genuinely differ (grown ad hoc over time,
# e.g. cefi/prediction run bigger + shorter, defi/tradfi run smaller +
# longer with per-VM manifest shards) — preserved as-is rather than
# normalized, so codifying doesn't silently change live behavior.
#
# cefi's own job (uts-prod-instruments-service-cefi-t1-recon) is triggered
# by the scheduler at 06:00 UTC (Tardis lag) per t1_batch_scheduler.tf.

locals {
  t1_recon_instruments_jobs = {
    defi = {
      cpu             = "4"
      memory          = "8Gi"
      timeout_seconds = 10800
      max_retries     = 1
      args            = ["--operation=instruments", "--mode=batch", "--asset-group=DEFI", "--run-tag=t1-recon"]
      extra_env = {
        MANIFEST_PER_VM_SHARDS = "true"
        VM_NAME                = "is-defi-t1-recon-job"
      }
    }
    tradfi = {
      cpu             = "4"
      memory          = "8Gi"
      timeout_seconds = 10800
      max_retries     = 1
      args            = ["--operation=instruments", "--mode=batch", "--asset-group=TRADFI", "--run-tag=t1-recon"]
      extra_env = {
        MANIFEST_PER_VM_SHARDS = "true"
        VM_NAME                = "is-tradfi-t1-recon-job"
      }
    }
    cefi = {
      cpu             = "8"
      memory          = "16Gi"
      timeout_seconds = 3600
      max_retries     = 0
      args            = ["--operation", "instruments", "--mode", "batch", "--asset-group", "CEFI", "--run-tag", "t1-recon"]
      extra_env       = {}
    }
    prediction = {
      cpu             = "8"
      memory          = "16Gi"
      timeout_seconds = 3600
      max_retries     = 0
      args            = ["--operation", "instruments", "--mode", "batch", "--asset-group", "PREDICTION", "--run-tag", "t1-recon"]
      extra_env       = {}
    }
  }
}

module "t1_recon_instruments_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.t1_recon_instruments_jobs

  name                  = "${local.env_prefix}-instruments-service-${each.key}-t1-recon"
  project_id            = var.project_id
  region                = var.region
  service_account_email = data.google_service_account.unified_trading_sa.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = each.value.cpu
  memory          = each.value.memory
  timeout_seconds = each.value.timeout_seconds
  max_retries     = each.value.max_retries
  parallelism     = 1
  task_count      = 1

  command = []
  args    = each.value.args

  environment_variables = merge(
    {
      DEPLOYMENT_ENV   = var.environment
      GCP_PROJECT_ID   = var.project_id
      GCS_LOCATION     = var.region
      PYTHONUNBUFFERED = "1"
    },
    each.value.extra_env,
  )

  service_name = "instruments-service-${each.key}-t1-recon"
  environment  = var.environment

  labels = {
    "purpose"     = "t1-recon"
    "asset_group" = each.key
  }
}
