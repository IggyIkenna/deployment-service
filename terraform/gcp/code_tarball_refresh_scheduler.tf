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
# The job uses the cloud-sdk image (git + gsutil + gcloud), reads GH_PAT from Secret Manager
# to clone private repos, and bootstraps by cloning deployment-service@LDR for the scripts
# (so the cron always runs the latest refresh logic — no stale bucket copy).
#
# Created imperatively 2026-06-17 (ADC admin); this file is the IaC SSOT.
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md.

locals {
  # cloud-sdk image: has git, gsutil, gcloud, bash, tar. The job clones deployment-service@LDR
  # for the refresh scripts, then runs refresh_code_tarballs.sh.
  # NOTE: avoid the `|` character (e.g. `||`) — the imperative `gcloud run jobs ... --args=^|^`
  # deploy uses `|` as the list delimiter, and `||` fragments the command (incident 2026-06-17:
  # the job silently ran only `command -v git` → no-op exit 0). Use `if/fi` instead.
  # Bootstrap: SPARSE-checkout only scripts/vm/ from deployment-service@LDR (a few KB), NOT the
  # full ~1.9 GiB repo — the full clone was ~14 min of every run's startup. Then run the refresh
  # script (always the latest LDR logic — no stale bucket copy).
  code_tarball_refresh_bootstrap = <<-EOT
    set -e
    if ! command -v git >/dev/null 2>&1; then apt-get -qq update >/dev/null && apt-get -qq install -y git >/dev/null; fi
    PAT=$(gcloud secrets versions access latest --secret=GH_PAT --project="$GCP_PROJECT_ID")
    git clone -q --depth 1 -b live-defi-rollout --filter=blob:none --sparse "https://x-access-token:$PAT@github.com/IggyIkenna/deployment-service.git" /tmp/ds
    git -C /tmp/ds sparse-checkout set scripts/vm
    exec bash /tmp/ds/scripts/vm/refresh_code_tarballs.sh --project "$GCP_PROJECT_ID"
  EOT
}

module "code_tarball_refresh_job" {
  source = "../modules/container-job/gcp"

  name                  = "code-tarball-refresh"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email # secretAccessor(GH_PAT) + storage.objectAdmin(code bucket)

  image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest"

  cpu             = "4"
  memory          = "16Gi" # memory-backed /tmp: sparse bootstrap (KB) + 1 repo's clone+tarball at a time (per-repo bounded)
  timeout_seconds = 3600    # 60 min — LDR is high-churn (~9 repos can change between runs) at ~3-4 min/repo serial
  max_retries     = 0       # SHA-skip + idempotent overwrite → a timed-out run just rebuilds fewer; next tick converges

  command = ["/bin/sh"]
  args    = ["-c", local.code_tarball_refresh_bootstrap]

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
