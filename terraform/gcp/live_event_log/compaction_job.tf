# Daily cold compaction Cloud Run Job + Cloud Scheduler trigger.
# Plan: live_persist_03_infra_pubsub_sinks_2026_06_26.md
#
# The compactor reads 5-minute warm parquet chunks, compacts them into daily
# cold parquet files, and applies GCS lifecycle per SINK_MATRIX retention_class.
#
# Region: asia-northeast1 (matches VM fleet zone).
# Schedule: 2 AM UTC daily (after daily windows close).

resource "google_cloud_run_v2_job" "live_event_log_compactor" {
  name     = "live-event-log-compactor"
  location = "asia-northeast1"
  project  = var.project_id

  template {
    template {
      containers {
        image = var.compactor_image

        # Shared deployment-service maintenance-jobs image (see variables.tf) defaults its CMD
        # to a generic import-check; override per-job like uts-prod-tarball-cleanup /
        # vm-log-archival-prd do. deployment_service/jobs/ is a proper package (has __init__.py),
        # so -m invocation (not a file path) is correct here.
        command = ["python"]
        args    = ["-m", "deployment_service.jobs.live_event_log_compactor"]

        # Raised from 512Mi→4Gi→16Gi: the (cefi, book_snapshot_5) shard has ~1500 warm files/day,
        # each ~170 MB NDJSON. The io.BytesIO cold-file buffer accumulates the full compressed
        # Parquet output (~6 GB) before the GCS upload, requiring ~7–8 GB peak RAM. 4Gi OOM'd.
        # 16Gi provides safe headroom. CPU raised to 4 for encoding throughput.
        # Issue: plans/active/issues/cefi_live_event_cold_compactor_oom_and_legacy_path_check_2026_08_07.md
        resources {
          limits = {
            memory = "16Gi"
            cpu    = "4"
          }
        }

        env {
          name  = "WARM_BUCKET"
          value = var.warm_gcs_bucket
        }
        env {
          name  = "COLD_BUCKET"
          value = var.cold_gcs_bucket
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "DEPLOYMENT_ENV"
          value = var.environment
        }
      }

      service_account = var.compactor_sa_email
      max_retries     = 3
      timeout         = "28800s"
    }
  }

  labels = {
    managed_by      = "terraform"
    deployment_type = "cloud-run-job"
    lifecycle_class = "permanent"
    purpose         = "live-event-log-cold-compaction"
  }
}

resource "google_cloud_scheduler_job" "live_event_log_compactor_daily" {
  name             = "live-event-log-compactor-daily"
  description      = "Daily cold compaction of live event-log warm GCS sink. Runs at 2 AM UTC."
  schedule         = "0 2 * * *"
  time_zone        = "UTC"
  project          = var.project_id
  region           = "asia-northeast1"
  attempt_deadline = "600s"

  http_target {
    http_method = "POST"
    uri         = "https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/live-event-log-compactor:run"

    oauth_token {
      service_account_email = var.compactor_sa_email
    }
  }

  retry_config {
    retry_count = 3
  }
}
