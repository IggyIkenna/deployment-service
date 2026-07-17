# Honest-coverage daily cron — Cloud Run Job + Cloud Scheduler
#
# Fires daily at 00:30 UTC via Cloud Scheduler, running a Cloud Run Job that
# downloads launch-measure-honest-coverage-vm.sh from GCS and launches a GCE VM.
# The VM runs instruments-service/scripts/measure_honest_coverage.py for all
# asset_groups and writes gs://central-element-323112-honest-coverage/{date}/coverage.json.
#
# The deployment-api GET /api/data-status/honest-coverage reads this file and
# returns 404 when missing (slot-7 found the UI 404 on 2026-05-14; UI half
# already fixed by deployment-ui@365c32f; cron half fixed here).
#
# References:
#   deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh
#   deployment-service/scripts/vm/setup-honest-coverage-scheduler.sh
#   plans/active/issues/honest_coverage_cron_vm_scheduling_2026_05_14.md
#   vm_zombie_watchdog.py — "measure-honest-coverage-" prefix registered (bucket=None)
#
# Implementation notes (2026-05-15 slot-2):
#   - Cloud Run Job created via gcloud (not terraform apply) since Terraform
#     state not set up for Harsh's operator account. Terraform here is the SSOT.
#   - Cloud Scheduler job requires cloudscheduler.jobs.create (Ikenna/owner);
#     see setup-honest-coverage-scheduler.sh for the one-liner.
#   - SA: instruments-service-cloud-run@ — needs compute.instanceAdmin.v1 (on the SA)
#     AND iam.serviceAccountUser/actAs on the VM's attached SA (the default compute SA).
#     The latter was missing → launcher failed daily; see the IAM block below.
#   - Image: gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine (public GCS SDK)

module "honest_coverage_daily_job" {
  source = "../modules/container-job/gcp"

  name                  = "honest-coverage-daily-launcher"
  project_id            = var.project_id
  region                = var.region
  service_account_email = "instruments-service-cloud-run@${var.project_id}.iam.gserviceaccount.com"

  image           = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
  cpu             = "1"
  memory          = "512Mi"
  timeout_seconds = 300
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # Download the launcher from the code bucket and execute it.
  # The launcher creates the GCE VM which runs measure_honest_coverage.py and
  # self-deletes via VM_SHUTDOWN_ON_COMPLETION=true.
  #
  # 2026-07-17: the launcher script gained a `source lib/launcher_common.sh`
  # dependency (fleet-wide launch-*.sh rollout, deployment-service@b5bd336),
  # but this job's fetch command was never updated to also download the vm/lib/
  # directory — it silently failed every run with "No such file or directory"
  # from the moment the new script was published (2026-07-16) until fixed here.
  # `gsutil cp -r 'gs://.../vm/lib/*' /tmp/lib/` mirrors the launcher's own
  # expectation (`source "$(dirname script)/lib/launcher_common.sh"`).
  command = ["/bin/sh"]
  args = [
    "-c",
    "mkdir -p /tmp/lib && gsutil cp gs://deployment-scripts-${var.project_id}/vm/launch-measure-honest-coverage-vm.sh /tmp/launcher.sh && gsutil -m cp -r 'gs://deployment-scripts-${var.project_id}/vm/lib/*' /tmp/lib/ && chmod +x /tmp/launcher.sh && bash /tmp/launcher.sh",
  ]

  environment_variables = {
    DEPLOYMENT_ENV = var.environment
    GCP_PROJECT_ID = var.project_id
  }

  service_name = "honest-coverage-daily-launcher"
  environment  = var.environment

  labels = {
    "purpose" = "honest-coverage-cron"
    "type"    = "daily"
  }
}

# The launcher SA needs iam.serviceAccountUser (actAs) on the GCE VM's ATTACHED
# service account. launch-measure-honest-coverage-vm.sh's `gcloud compute instances
# create` does NOT pass --service-account, so the VM attaches the DEFAULT COMPUTE SA —
# and creating a VM with an attached SA requires actAs on THAT SA. compute.instanceAdmin.v1
# (already granted to the launcher SA) is necessary but NOT sufficient without this.
# Missing this binding silently failed the launcher Cloud Run Job (exit 1, logs swallowed)
# every day from ~2026-05-18 to 2026-06-16 → no coverage.json was produced and the
# data-status "Honest Coverage" panel showed "Coverage data not yet computed for this date".
# Granted imperatively 2026-06-16 (ADC admin); this block makes it the IaC SSOT.
resource "google_service_account_iam_member" "honest_coverage_launcher_actas_default_compute" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.project_number}-compute@developer.gserviceaccount.com"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:instruments-service-cloud-run@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_cloud_scheduler_job" "honest_coverage_daily" {
  project          = var.project_id
  region           = var.region
  name             = "honest-coverage-daily"
  description      = "Daily 00:30 UTC trigger — downloads launch-measure-honest-coverage-vm.sh from GCS and creates measurement VM. Output: gs://{pid}-honest-coverage/{date}/coverage.json"
  schedule         = "30 0 * * *"
  time_zone        = "UTC"
  attempt_deadline = "60s"

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/honest-coverage-daily-launcher:run"

    oauth_token {
      service_account_email = "cloud-scheduler@${var.project_id}.iam.gserviceaccount.com"
    }
  }

  depends_on = [module.honest_coverage_daily_job]
}
