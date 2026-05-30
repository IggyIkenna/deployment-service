# VM Periodic Serial Capture — Cloud Run Job + 6-hourly Scheduler cron
#
# Plan: canonical_vm_log_archival_2026_05_27 (task -002)
#
# Background:
#   The GCE serial console ring buffer is ~1 MB. For LONG_LIVED_LIVE and
#   SCHEDULED_RECURRING VMs that run for days/weeks the one-shot pre-kill
#   capture (backup-vm-logs.sh) misses early-boot output once the buffer
#   wraps. This job captures the serial console periodically so early boot
#   diagnostics are always preserved.
#
# Schedule:
#   Every 6 hours (00:30 / 06:30 / 12:30 / 18:30 UTC). Offset by 30 min to
#   avoid collision with the daily 02:00 UTC log-archival job.
#
# Targeting:
#   Only LONG_LIVED_LIVE and SCHEDULED_RECURRING VMs (per VM_PREFIX_TO_BUCKET
#   lifecycle_class in vm_zombie_watchdog.py). EPHEMERAL_BATCH /
#   EPHEMERAL_EXPERIMENT VMs are short-lived and covered by the pre-kill hook.
#
# Archive path:
#   gs://deployment-scripts-{project}/log-archive/periodic-serial/{vm}/{YYYYMMDD_HH00}/serial-console.txt
#
# Idempotency:
#   Timestamp is truncated to the hour; reruns within the same cron window
#   are no-ops (existing GCS object is detected and skipped).
#
# Image:
#   Reuses deployment-service:latest (contains UTL + google-cloud-compute).
#
# Service account:
#   Container runtime: unified_trading_sa — needs:
#     * storage.objectAdmin on deployment-scripts bucket (write archive)
#     * compute.instances.getSerialPortOutput on all instances

resource "google_cloud_run_v2_job" "vm_serial_capture" {
  name     = "vm-serial-capture-${local.deployment_env_short}"
  location = var.region
  project  = var.project_id

  template {
    parallelism = 1
    task_count  = 1

    template {
      timeout         = "1800s"  # 30 minutes — generous for large fleets
      service_account = google_service_account.unified_trading.email

      containers {
        image = "asia-northeast1-docker.pkg.dev/${var.project_id}/unified-trading-library/deployment-service:latest"

        command = [
          "python",
          "-m",
          "scripts.vm.vm_serial_capture_cron"
        ]

        env {
          name  = "DEPLOYMENT_ENV"
          value = var.environment
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "CLOUD_PROVIDER"
          value = "gcp"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

resource "google_cloud_scheduler_job" "vm_serial_capture" {
  name        = "vm-serial-capture-${local.deployment_env_short}"
  description = "Every-6h serial console capture for LONG_LIVED_LIVE + SCHEDULED_RECURRING VMs"
  schedule    = "30 0,6,12,18 * * *"  # 00:30 / 06:30 / 12:30 / 18:30 UTC
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.vm_serial_capture.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.t1_batch.email
    }
  }

  retry_config {
    retry_count = 2
  }
}
