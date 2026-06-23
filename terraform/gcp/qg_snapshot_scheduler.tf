# QG Snapshot Scheduler — daily Cloud Scheduler → GCE VM via Compute Engine API
#
# Plan: cefi_venue_backfill_coverage_remediation_2026_05_27.md (task 015)
#
# Root cause: Cloud Scheduler was never created after the 2026-05-14 smoke test.
# The launch-qg-snapshot-vm.sh script documents the gcloud command needed but it
# was never executed; the VM prefix is registered in vm_zombie_watchdog.py and the
# snapshot.sh + snapshot_to_parquet.py scripts are complete and tested.
#
# What this file adds:
#   1. IAM: t1_batch SA → roles/compute.instanceAdmin.v1 (needed to create GCE instances)
#   2. Cloud Scheduler job: daily 06:00 UTC → Compute Engine instances.insert to boot
#      a qg-snapshot-daily VM running snapshot.sh → snapshot_to_parquet.py → GCS parquet.
#
# Schedule: 06:00 UTC — after the T+1 batch pipeline (~05:xx UTC); before daily planning.
# Cost: e2-small ~$0.013/h × ≤30 min ≈ $0.01/day.
#
# Singleton: VM name is "qg-snapshot-daily"; if a prior run is still alive the
# instances.insert call is rejected by Compute Engine (duplicate name → 409 Conflict),
# which is the desired behaviour (skip silently rather than double-run).
#
# Archive path: gs://{project_id}-deployment-events/quality_gates_snapshot/

resource "google_project_iam_member" "t1_batch_compute_instance_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Required for the batch SA to attach itself as the runtime SA when creating GCE VMs.
# Without this, instances.insert with serviceAccounts=[t1_batch] returns
# SERVICE_ACCOUNT_ACCESS_DENIED (iam.serviceAccountUser missing).
# Granted ad-hoc 2026-06-23 to fix DeFi forward-poll singleton VMs silently failing.
resource "google_project_iam_member" "t1_batch_service_account_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Required for the batch SA (as the GCE VM's runtime SA) to download the startup script URL.
# Without this, setup-data-pipeline-vm.sh fetch returns 403 Forbidden from GCS metadata server.
# Granted ad-hoc 2026-06-23 at bucket level (deployment-scripts-{project_id}).
resource "google_storage_bucket_iam_member" "t1_batch_deployment_scripts_reader" {
  bucket = "deployment-scripts-${var.project_id}"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Required for the batch SA (as the GCE VM's runtime SA) to write GCS-tee run.log to
# deployment-scripts-{project_id}/vm-logs/<vm-name>/run.log.
# Without this, vm-exec-with-gcs-tee.sh silently fails to write logs (gsutil cp exits non-0 but
# is not fatal). Granted ad-hoc 2026-06-23 via gsutil iam ch.
resource "google_storage_bucket_iam_member" "t1_batch_deployment_scripts_writer" {
  bucket = "deployment-scripts-${var.project_id}"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Required for UTL ServiceRuntime bootstrap (log_event("STARTED")) in live mode, which
# publishes to market-tick-data-service-events via PubSubEventSink. Without this the
# VM process crashes with PERMISSION_DENIED at startup before any data collection.
# Granted ad-hoc 2026-06-23 via gcloud pubsub topics add-iam-policy-binding.
resource "google_pubsub_topic_iam_member" "t1_batch_market_tick_events_publisher" {
  project = var.project_id
  topic   = "market-tick-data-service-events"
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

# Required for UTL ServiceRuntime bootstrap (log_event("STARTED")) — secondary topic
# referenced in the PubSubEventSink initialisation. Same root cause as above.
# Granted ad-hoc 2026-06-23 via gcloud pubsub topics add-iam-policy-binding.
resource "google_pubsub_topic_iam_member" "t1_batch_deployment_events_publisher" {
  project = var.project_id
  topic   = "deployment-events"
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

locals {
  qg_snapshot_zone        = "asia-northeast1-c"
  qg_snapshot_startup_url = "gs://deployment-scripts-${var.project_id}/vm/setup-data-pipeline-vm.sh"
  qg_snapshot_backfill_cmd = join(" | ", [
    "bash /home/unified/workspace/unified-trading-pm/scripts/quality_gates/snapshot.sh",
    "python3 /home/unified/workspace/unified-trading-pm/scripts/quality_gates/snapshot_to_parquet.py --project-id ${var.project_id}",
  ])
}

resource "google_cloud_scheduler_job" "qg_snapshot_daily" {
  name        = "qg-snapshot-daily-${local.deployment_env_short}"
  description = "Daily QG snapshot across all repos → GCS parquet (B-018 Phase 4.A)"
  schedule    = "0 6 * * *"
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${local.qg_snapshot_zone}/instances"
    http_method = "POST"
    body = base64encode(jsonencode({
      name        = "qg-snapshot-daily"
      zone        = "projects/${var.project_id}/zones/${local.qg_snapshot_zone}"
      machineType = "zones/${local.qg_snapshot_zone}/machineTypes/e2-small"
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
          { key = "startup-script-url",        value = local.qg_snapshot_startup_url },
          { key = "VM_TASK",                   value = "qg-snapshot" },
          { key = "VM_SERVICE",                value = "qg_snapshot" },
          { key = "VM_OPERATION",              value = "qg-snapshot" },
          { key = "VM_BACKFILL_CMD",           value = local.qg_snapshot_backfill_cmd },
          { key = "DEPLOYMENT_ENV",            value = var.environment },
          { key = "VM_SHUTDOWN_ON_COMPLETION", value = "true" },
          { key = "GCP_PROJECT_ID",            value = var.project_id },
          { key = "WORKSPACE_ROOT",            value = "/home/unified/workspace" },
        ]
      }
      labels = { purpose = "qg-snapshot", env = var.environment }
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
