# Plan Hygiene Sweep — Cloud Run Job + Cloud Scheduler cron (daily at 05:00 UTC)
#
# Plan-of-record:
#   `unified-trading-pm/plans/active/plan_hygiene_automation_2026_05_21.md` Phase 6.
#
# Cron schedule:
#   `0 5 * * *` UTC (daily at 05:00) — offset from orphan-ping-audit passes
#   (02:15/06:15/10:15/14:15/18:15/22:15) so the two jobs don't collide.
#
# What the Job does:
#   1. Clone unified-trading-pm @ live-defi-rollout using a GH_PAT loaded
#      from Secret Manager.
#   2. Run `bash scripts/plan-hygiene/run_hygiene_sweep.sh --ci` (runs all
#      hard + soft checks; exits 1 on hard failures so Cloud Run marks the
#      job FAILED for alerting).
#   3. If any hard check fails, run_hygiene_sweep.sh --ci is responsible for
#      appending failure pings to orchestrator _agent_pings.md files and
#      committing + pushing back to live-defi-rollout so both operators see
#      the notification on next `git fetch`.
#
# Daily sweep: `0 5 * * *` UTC.
# Timeout: 600s (sweep + inventory regen + git push can take up to ~5 min).
#
# Image:
#   `gcr.io/google.com/cloudsdktool/google-cloud-cli:slim` — Google's public
#   Cloud SDK slim image has bash + git + gcloud preinstalled. No private
#   image build needed; the cron only needs a shell + git + the GH_PAT.
#
# Service account:
#   * Scheduler invoker + container runtime: `t1_batch_sa` (already has
#     roles/run.invoker; `roles/secretmanager.secretAccessor` on `GH_PAT`
#     is already granted in `orphan_ping_audit_scheduler.tf` — see comment
#     on `google_secret_manager_secret_iam_member.t1_batch_gh_pat_accessor`
#     in that file. DO NOT re-declare the IAM binding here to avoid
#     Terraform conflicts.
#
# First install (2026-05-21):
#   terraform apply — both resources are new (no out-of-band gcloud create
#   was done for this job, unlike orphan-ping-audit which was pre-created).

# -------------------------------------------------------
# Cloud Run Job — plan hygiene sweep
# -------------------------------------------------------
resource "google_cloud_run_v2_job" "plan_hygiene_sweep" {
  name                = "${local.env_prefix}-plan-hygiene-sweep"
  location            = var.region
  project             = var.project_id
  deletion_protection = false

  template {
    parallelism = 1
    task_count  = 1

    template {
      timeout               = "600s"
      service_account       = google_service_account.t1_batch.email
      max_retries           = 1
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

      containers {
        # Public Google Cloud SDK slim image — has bash + git + gcloud.
        # No private image build needed.
        image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:slim"

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        command = ["bash"]
        args = [
          "-c",
          "git clone --depth=10 --branch=live-defi-rollout https://x-access-token:$${GH_PAT}@github.com/IggyIkenna/unified-trading-pm.git /tmp/pm && bash /tmp/pm/scripts/plan-hygiene/cron_hygiene_sweep_entrypoint.sh",
        ]

        env {
          name  = "PM_BRANCH"
          value = "live-defi-rollout"
        }
        env {
          name  = "PM_REPO_URL"
          value = "https://github.com/IggyIkenna/unified-trading-pm.git"
        }
        env {
          name  = "GIT_COMMITTER_EMAIL"
          value = "hygiene-cron@odum-research.com"
        }
        env {
          name  = "GIT_COMMITTER_NAME"
          value = "hygiene-cron"
        }
        env {
          name  = "GIT_AUTHOR_EMAIL"
          value = "hygiene-cron@odum-research.com"
        }
        env {
          name  = "GIT_AUTHOR_NAME"
          value = "hygiene-cron"
        }
        env {
          name = "GH_PAT"
          value_source {
            secret_key_ref {
              secret  = "GH_PAT"
              version = "latest"
            }
          }
        }
      }
    }
  }

  labels = {
    "managed-by" = "terraform"
    "service"    = "plan-hygiene-sweep"
    "purpose"    = "plan-hygiene"
    "owner"      = "ikenna"
  }

  # GH_PAT secret accessor IAM binding is already declared in
  # orphan_ping_audit_scheduler.tf as
  # google_secret_manager_secret_iam_member.t1_batch_gh_pat_accessor.
  # Both jobs share the same t1_batch SA + GH_PAT secret; no second
  # binding needed.
  depends_on = [
    google_service_account.t1_batch,
  ]
}

# -------------------------------------------------------
# Cloud Scheduler — daily at 05:00 UTC
# -------------------------------------------------------
# Offset from orphan-ping-audit (02:15/06:15/10:15/14:15/18:15/22:15)
# so the two cron jobs don't overlap and each catches issues the other
# might miss.
resource "google_cloud_scheduler_job" "plan_hygiene_sweep_cron" {
  name        = "${local.env_prefix}-plan-hygiene-sweep-cron"
  description = "Daily plan hygiene sweep — run_hygiene_sweep.sh --ci, push failure pings to live-defi-rollout."
  schedule    = "0 5 * * *"
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.plan_hygiene_sweep.name}:run"

    oauth_token {
      service_account_email = google_service_account.t1_batch.email
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
output "plan_hygiene_sweep_job_name" {
  description = "Cloud Run Job name for ad-hoc manual fire (`gcloud run jobs execute`)."
  value       = google_cloud_run_v2_job.plan_hygiene_sweep.name
}

output "plan_hygiene_sweep_cron_name" {
  description = "Cloud Scheduler cron name."
  value       = google_cloud_scheduler_job.plan_hygiene_sweep_cron.name
}
