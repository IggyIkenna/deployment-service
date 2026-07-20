# DeFi on-chain removal probe (Option B truth-gate) — daily Cloud Scheduler trigger.
#
# Reads gs://instruments-store-defi-prd-…/prod/catalog.parquet, probes each currently-LIVE
# EVM-addressed DeFi instrument on-chain (eth_getCode at latest block), and writes/merges the
# removal side-artifact gs://…/_cache/defi_removals.json that build_instrument_catalogue.py
# reads to set delisted_at (branch 1 — the seam the Option A carve-out preserves). Only
# POSITIVELY confirmed-gone contracts are recorded; every uncertainty stays live, so the probe
# can never re-create the false-delisting the available_to fix removed.
#
# Runs at 00:30 UTC — AFTER the 00:00 IS FAST refresh and BEFORE the 01:00
# lifecycle-catalogue-regen-defi run, so a same-day confirmed removal is applied by that day's
# catalogue rebuild. Registered as a BATCH DeploymentTarget in cloud_run_job_registry.py
# ("defi-removal-probe"). Entrypoint: instruments-service image, scripts/run_defi_removal_probe.py.
#
# Plan: defi_catalogue_available_to_false_delisting_2026_07_20 (Option B) →
# defi_consolidated_closeout_2026_07_18 Track 3.

locals {
  defi_removal_probe_bucket = "instruments-store-defi-prd-central-element-323112"
}

resource "google_service_account" "defi_removal_probe" {
  project      = var.project_id
  account_id   = "defi-removal-probe"
  display_name = "DeFi Removal Probe — daily on-chain contract-removal truth-gate"
}

# READ prod/catalog.parquet + READ/WRITE _cache/defi_removals.json on the defi instruments-store bucket.
resource "google_storage_bucket_iam_member" "defi_removal_probe_store_admin" {
  bucket = local.defi_removal_probe_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.defi_removal_probe.email}"
}

module "defi_removal_probe_job" {
  source = "../modules/container-job/gcp"

  name                  = "defi-removal-probe"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.defi_removal_probe.email

  # instruments-service image — run_defi_removal_probe.py lives at
  # /app/instruments-service/scripts/ (Dockerfile WORKDIR=/app/instruments-service, COPY . .).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = "1"
  memory          = "2Gi"
  timeout_seconds = 3600 # 60 min — bounded RPC probes over the live EVM-addressed defi universe
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "/app/instruments-service/scripts/run_defi_removal_probe.py",
    "--apply",
  ]

  environment_variables = {
    GCP_PROJECT_ID   = var.project_id
    DEPLOYMENT_ENV   = var.environment
    CLOUD_PROVIDER   = "gcp"
    PYTHONUNBUFFERED = "1"
  }

  service_name = "defi-removal-probe"
  environment  = var.environment

  labels = {
    "purpose"     = "defi-removal-probe"
    "asset_group" = "defi"
  }
}

# The Cloud Scheduler job authenticates as this SA and POSTs the job's :run endpoint — the SA
# MUST hold roles/run.invoker ON THE JOB or the trigger is rejected with PERMISSION_DENIED and
# the job never executes (the same silent gap called out in lifecycle_catalogue_scheduler.tf).
resource "google_cloud_run_v2_job_iam_member" "defi_removal_probe_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = module.defi_removal_probe_job.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.defi_removal_probe.email}"
}

resource "google_cloud_scheduler_job" "defi_removal_probe_daily" {
  project          = var.project_id
  region           = var.region
  name             = "defi-removal-probe-daily"
  description      = "Confirm on-chain DeFi contract removals (eth_getCode absent) → _cache/defi_removals.json for the lifecycle catalogue delisted_at truth-gate (Option B)"
  schedule         = "30 0 * * *" # 00:30 UTC — after 00:00 IS FAST refresh, before 01:00 lifecycle-catalogue-regen-defi
  time_zone        = "UTC"
  attempt_deadline = "1800s" # scheduler only fires the :run POST; the JOB's timeout_seconds=3600 bounds the probe

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/defi-removal-probe:run"

    oauth_token {
      service_account_email = google_service_account.defi_removal_probe.email
    }
  }
}
