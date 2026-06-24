# TradFi OHLCV Wave-Launcher — Cloud Run Job + Scheduler (autonomous backfill driver)
#
# Epic: tradfi_master
# Plan: parent epic plans/epics/tradfi_master.md
# Pattern source: cf_manifest_audit_scheduler.tf (Cloud Run Job + Cloud Scheduler shape)
#   + manifest_consolidator_scheduler.tf
#
# Background
#   The TradFi OHLCV backfill (databento GLBX.MDP3 CME / DBEQ.BASIC equities /
#   XCBF.PITCH CFE) was being driven by MANUALLY-launched waves of per-venue
#   backfill VMs — so it STALLED the moment a wave's VMs finished (no scheduler).
#   This file ships the autonomous driver: a Cloud Run Job that, every 3h, reads
#   the tradfi availability manifest, computes the cells still needing work
#   (expected_unattempted + attempted_failed — the latter is the failed-cell
#   retry), and tops the running tradfi-bf-* fleet back up to MAX_CONCURRENT by
#   launching per-venue OHLCV backfill VMs. It launches NOTHING once the in-scope
#   OHLCV gap is zero (logs "TRADFI BACKFILL COMPLETE" + emits a DP_* event).
#
# Concurrency safety (HARD CAP)
#   WAVE_MAX_CONCURRENT (default 12) bounds the running tradfi-bf-* fleet. The
#   wave-launcher counts running VMs each tick and launches at most
#   (MAX_CONCURRENT - running). The launcher clamps to a HARD CEILING of 20 in
#   code regardless of the env value — the env can only LOWER it.
#
# Schedule
#   `0 */3 * * *` UTC — every 3 hours. A backfill VM year-shard completes in
#   ~30-90 min, so a 3h cadence refills the fleet a couple of VM-lifetimes after
#   the prior wave drains, without piling launches on top of a still-busy fleet.
#
# Runtime SA — IMPORTANT
#   The job SHELLS OUT to `gcloud compute instances create` (via the per-venue
#   launchers), so the runtime SA needs compute.instanceAdmin.v1 +
#   iam.serviceAccountUser (to act-as the VM SA). The IAM bindings below grant
#   these to the unified_trading runtime SA. It also reads
#   availability_index.parquet (storage.objectViewer on the tradfi tick bucket —
#   already granted fleet-wide by the consolidator config).
#
# Image
#   Uses the deployment-service image (carries scripts/wave_launcher.py +
#   scripts/vm/launch-tradfi-bf-*-ohlcv-1m.sh + UTL + gcloud SDK). The equity
#   launchers (NASDAQ/NYSE) resolve the ticker universe from UAC at
#   $WORKSPACE_ROOT/unified-api-contracts — the image must carry UAC +
#   .venv-workspace at WORKSPACE_ROOT (set to /app below) for the equity legs.

variable "wave_launcher_image" {
  description = "Container image for the tradfi wave-launcher job. Defaults to the deployment-service image (carries wave_launcher.py + the per-venue launchers + gcloud SDK)."
  type        = string
  default     = "" # empty = fall through to local default below
}

variable "wave_launcher_max_concurrent" {
  description = "HARD CAP on concurrently-running tradfi-bf-* VMs the wave-launcher will allow. Default 12. The launcher clamps to a hard ceiling of 20 in code regardless."
  type        = number
  default     = 12
}

locals {
  wave_launcher_image_resolved = var.wave_launcher_image != "" ? var.wave_launcher_image : "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-service:latest"
}

# -------------------------------------------------------
# Runtime SA IAM — the job creates VMs, so it needs compute admin + act-as.
# -------------------------------------------------------
resource "google_project_iam_member" "wave_launcher_compute_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_project_iam_member" "wave_launcher_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.unified_trading.email}"
}

# -------------------------------------------------------
# Cloud Run Job — single task, runs one wave-launcher tick.
# -------------------------------------------------------
# max_retries = 0: a tick is idempotent against the manifest, but we do not want
# a retry to double-launch on a transient gcloud blip; the next 3h cron re-checks.
module "wave_launcher_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-tradfi-wave-launcher"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.wave_launcher_image_resolved

  # Manifest read (~7M rows) + gcloud calls — IO/memory bound, not CPU-heavy.
  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800 # 30 min — a tick launches at most MAX_CONCURRENT VMs.

  max_retries = 0
  parallelism = 1
  task_count  = 1

  # The Dockerfile WORKDIR is /app and COPYs scripts/ → /app/scripts/ — there is NO
  # /app/deployment-service/ dir in the image. The prior /app/deployment-service/scripts/…
  # path did not exist → `python3 <missing-file>` exited 2 on EVERY tick (the wave-launcher
  # never ran autonomously; the early waves were launched by hand). Fixed 2026-06-24.
  command = ["python3", "/app/scripts/wave_launcher.py"]
  args    = []

  environment_variables = {
    GCP_PROJECT_ID       = var.project_id
    PROJECT_ID           = var.project_id
    DEPLOYMENT_ENV       = var.environment
    DEPLOYMENT_ENV_SHORT = "prd"
    CLOUD_PROVIDER       = "gcp"
    WAVE_MAX_CONCURRENT  = tostring(var.wave_launcher_max_concurrent)
    # Equity launchers resolve the ticker universe from UAC at $WORKSPACE_ROOT.
    WORKSPACE_ROOT = "/app"
  }

  service_name = "tradfi-wave-launcher"
  environment  = var.environment

  labels = {
    "purpose" = "tradfi-wave-launcher"
    "tier"    = "backfill-driver"
    "epic"    = "tradfi-master"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — every 3 hours.
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "wave_launcher_cron" {
  name        = "${local.env_prefix}-tradfi-wave-launcher-cron"
  description = "Every 3h: read the tradfi manifest, refill the tradfi-bf-* fleet to MAX_CONCURRENT with per-venue OHLCV backfill VMs (incl. the attempted_failed retry). Autonomous backfill driver for epic tradfi_master."
  schedule    = "0 */3 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.wave_launcher_job.name}:run"

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

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "tradfi_wave_launcher_job_name" {
  description = "Cloud Run Job name for the tradfi wave-launcher (ad-hoc run: `gcloud run jobs execute`)."
  value       = module.wave_launcher_job.name
}

output "tradfi_wave_launcher_cron_name" {
  description = "Cloud Scheduler cron name for the 3-hourly tradfi wave-launcher."
  value       = google_cloud_scheduler_job.wave_launcher_cron.name
}
