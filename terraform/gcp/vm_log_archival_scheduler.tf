# VM Log Archival — Cloud Run Job + daily Scheduler cron
#
# Plan: canonical_vm_log_archival_2026_05_27
#
# Background:
#   VMs stream logs to gs://deployment-scripts-{project}/vm-logs/ with
#   a 14-day lifecycle rule. This job archives those logs daily to
#   gs://deployment-scripts-{project}/log-archive/rolling/{date}/ which
#   has no expiry, preserving VM logs for forensics and compliance.
#
# Schedule:
#   Daily at 02:00 UTC to minimize conflict with active VM workloads.
#
# Image:
#   Reuses deployment-service:latest which has UTL installed as a dep.
#
# Service account:
#   * Scheduler invoker: t1_batch_sa (already has roles/run.invoker)
#   * Container runtime: unified_trading_sa (storage.objectAdmin on
#     deployment-scripts bucket for read/write access)

resource "google_cloud_run_v2_job" "vm_log_archival" {
  name     = "vm-log-archival-${local.deployment_env_short}"
  location = var.region
  project  = var.project_id

  # Maintenance job — safe to replace on image/command changes (no persistent state).
  deletion_protection = false

  template {
    parallelism = 1
    task_count  = 1

    template {
      timeout         = "1800s"  # 30 minutes
      service_account = google_service_account.unified_trading.email

      containers {
        # deployment-service jobs image (api stage + scripts/). The previous path
        # unified-trading-library/deployment-service:latest never existed in Artifact
        # Registry (job creation errored "Image not found", 2026-06-02). Built+pushed by
        # cloud-build/deployment-service-jobs-image.cloudbuild.yaml.
        image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-service:latest"

        # File-path invocation (scripts/vm/ has no __init__.py, so `python -m
        # scripts.vm....` would not import). WORKDIR is /app; script is at /app/scripts/.
        command = [
          "python",
          "scripts/vm/vm_log_archival_cron.py",
          "--project-id",
          var.project_id,
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
            cpu    = "2"
            memory = "2Gi"
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

resource "google_cloud_scheduler_job" "vm_log_archival" {
  name        = "vm-log-archival-${local.deployment_env_short}"
  description = "Daily archival of VM logs from 14-day TTL to durable storage"
  schedule    = "0 2 * * *"  # Daily at 02:00 UTC
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.vm_log_archival.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.t1_batch.email
    }
  }

  retry_config {
    retry_count = 2
  }
}