# Code-tarball auto-refresh — Cloud Run Job + Cloud Scheduler (every 30 min)
#
# WHY: VM-deployment tarballs (gs://{code-bucket}/code/*-code.tar.gz) are the RAPID-DEV
# delivery path — VMs fetch them in setup-data-pipeline-vm.sh. They must track the LDR
# integration branch, but a manual `create-code-tarballs.sh` on every ship is a treadmill
# (and it tars the WORKING TREE, so a dirty foreign-WIP clone blocks it). Prod is unaffected:
# prod runs Docker IMAGES off `main` (stable), not these tarballs.
#
# WHAT: a cron fires a Cloud Run Job that runs scripts/vm/refresh_code_tarballs.sh —
#   * SHA-skip: `git ls-remote` each repo's LDR tip vs its uploaded {tarball}.manifest.json
#     commit_sha; only CHANGED repos are rebuilt. Idle runs are cheap (just ls-remote calls).
#   * Clean-by-construction: clones each changed repo FRESH at origin/live-defi-rollout
#     (a dirty host working tree is irrelevant).
#   * Sequential per-repo (clone→build→cleanup) so peak scratch is one repo (LDR is highly
#     active — many repos can change between runs; /tmp is memory-backed in Cloud Run).
#
# Created imperatively 2026-06-17 (ADC admin); this file is the IaC SSOT.
#
# IMAGE (changed 2026-08-01, see code_tarball_refresh_job_silently_failing_since_2026_07_30_2026_08_01.md):
# reuses deployment-service:latest — same maintenance-jobs image tarball_cleanup_scheduler.tf /
# vm_log_archival_scheduler.tf already use, built by cloud-build/deployment-service-jobs-image.cloudbuild.yaml
# (`code-tarball-refresh` added to that config's redeploy-jobs list so future builds keep it in sync).
# The ORIGINAL design used the stock `gcr.io/google.com/cloudsdktool/google-cloud-cli:latest` image with a
# bootstrap that sparse-checked-out ONLY scripts/vm/ from deployment-service@LDR (to avoid a slow full clone) —
# this silently broke `gcs_upload_via_adc.py`'s `from deployment_service... import main`: the sparse checkout
# never included the `deployment_service` PACKAGE itself, so every upload crashed with ModuleNotFoundError
# while the tarball BUILD step (which doesn't need the package) kept succeeding, masking the failure for 2+
# days. `deployment-service:latest` already has the full `deployment_service` package installed (Python
# 3.13.14, matching the package's own `requires-python`) AND git/bash/tar/gcloud (all verified present) — no
# bootstrap/sparse-checkout needed at all; the job just runs the script straight from the baked-in image.
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md.

module "code_tarball_refresh_job" {
  source = "../modules/container-job/gcp"

  name                  = "code-tarball-refresh"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email # secretAccessor(GH_PAT) + storage.objectAdmin(code bucket)

  # deployment-service jobs image — see the IMAGE note above. WORKDIR is /app, so the script
  # path below resolves as scripts/vm/... (mirrors tarball_cleanup_scheduler.tf's convention).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-service:latest"

  cpu             = "4"
  memory          = "16Gi" # memory-backed /tmp: 1 repo's clone+tarball at a time (per-repo bounded)
  timeout_seconds = 3600   # 60 min — LDR is high-churn (~9 repos can change between runs) at ~3-4 min/repo serial
  max_retries     = 0      # SHA-skip + idempotent overwrite → a timed-out run just rebuilds fewer; next tick converges

  command = ["bash"]
  args    = ["scripts/vm/refresh_code_tarballs.sh", "--project", var.project_id]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
  }

  service_name = "code-tarball-refresh"
  environment  = var.environment

  labels = {
    purpose = "code-tarball-refresh"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — every 30 min
# -------------------------------------------------------
# 30-min cadence balances tarball freshness vs rebuild cost. SHA-skip makes a no-change tick
# cheap (ls-remote only); an active tick rebuilds just the changed repos, one at a time.
resource "google_cloud_scheduler_job" "code_tarball_refresh_cron" {
  project     = var.project_id
  region      = var.region
  name        = "${local.env_prefix}-code-tarball-refresh-cron"
  description = "Refresh VM-deployment code tarballs from live-defi-rollout (SHA-skip; rapid-dev path — prod uses images off main)."
  schedule    = "*/30 * * * *"
  time_zone   = "UTC"

  retry_config {
    retry_count          = 0
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.code_tarball_refresh_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  depends_on = [module.code_tarball_refresh_job]
}

output "code_tarball_refresh_job_name" {
  description = "Cloud Run Job name for the code-tarball refresh (ad-hoc fire: `gcloud run jobs execute`)."
  value       = module.code_tarball_refresh_job.name
}

output "code_tarball_refresh_cron_name" {
  description = "Cloud Scheduler cron name for the 30-min code-tarball refresh."
  value       = google_cloud_scheduler_job.code_tarball_refresh_cron.name
}
