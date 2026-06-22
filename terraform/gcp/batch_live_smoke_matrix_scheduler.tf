# Batch+Live Smoke Matrix Scheduler — daily Cloud Scheduler → GCE VM
#
# Plan: batch_live_smoke_matrix_2026_06_19.md (P2 NICE-TO-HAVE todo)
#
# What this file adds:
#   A Cloud Scheduler job (09:00 UTC daily) that boots an ephemeral VM,
#   runs validate_batch_live_smoke_matrix.py --live-window 8 across all 5 AGs
#   with GCS network on, then shuts down.  The L2 runtime-tick dimension runs
#   continuously; L1-only runs in the QG (--smoke, network-free).
#
# Schedule: 09:00 UTC — after QG snapshot (06:00 UTC); staggered from the
#   honest-coverage cron to avoid contention on the same credentials bucket.
# Cost: e2-small ~$0.013/h × ≤30 min ≈ $0.01/day.
#
# Singleton: VM name is "batch-live-smoke-matrix-daily"; if a prior run is still
#   alive the instances.insert call is rejected by Compute Engine (409 Conflict),
#   which is the desired behaviour (skip silently rather than double-run).
#
# Output: stdout/stderr captured by Cloud Logging via the startup script; the
#   per-cell verdict table is written to the VM serial log for operator review.

locals {
  blsm_zone        = "asia-northeast1-c"
  blsm_startup_url = "gs://deployment-scripts-${var.project_id}/vm/setup-data-pipeline-vm.sh"
  blsm_cmd = join(" ", [
    "python3 /home/unified/workspace/e2e-testing/scripts/validation/validate_batch_live_smoke_matrix.py",
    "--live-window 8",
  ])
}

resource "google_cloud_scheduler_job" "batch_live_smoke_matrix_daily" {
  name        = "batch-live-smoke-matrix-daily-${local.deployment_env_short}"
  description = "Daily batch+live smoke matrix (L2 --live-window 8) across all 5 AGs"
  schedule    = "0 9 * * *"
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${local.blsm_zone}/instances"
    http_method = "POST"
    body = base64encode(jsonencode({
      name        = "batch-live-smoke-matrix-daily"
      zone        = "projects/${var.project_id}/zones/${local.blsm_zone}"
      machineType = "zones/${local.blsm_zone}/machineTypes/e2-small"
      disks = [{
        boot = true
        initializeParams = {
          sourceImage = "projects/debian-cloud/global/images/family/debian-12"
          diskSizeGb  = "30"
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
          { key = "startup-script-url",        value = local.blsm_startup_url },
          { key = "VM_TASK",                   value = "batch-live-smoke-matrix" },
          { key = "VM_SERVICE",                value = "batch_live_smoke_matrix" },
          { key = "VM_OPERATION",              value = "batch-live-smoke-matrix" },
          { key = "VM_BACKFILL_CMD",           value = local.blsm_cmd },
          { key = "DEPLOYMENT_ENV",            value = var.environment },
          { key = "VM_SHUTDOWN_ON_COMPLETION", value = "true" },
          { key = "GCP_PROJECT_ID",            value = var.project_id },
          { key = "WORKSPACE_ROOT",            value = "/home/unified/workspace" },
        ]
      }
      labels = { purpose = "batch-live-smoke-matrix", env = var.environment }
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
