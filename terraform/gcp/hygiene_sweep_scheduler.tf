# Plan Hygiene Sweep — Cloud Run Job + Cloud Scheduler cron (daily 05:00 UTC)
#
# Plan-of-record:
#   CLAUDE.md § "Plan Hygiene — Frontmatter, Line Caps, Archive Candidates"
#   codex/11-project-management/plan-hygiene.md
#
# What the Job does:
#   1. Clone unified-trading-pm @ live-defi-rollout using a GH_PAT loaded
#      from Secret Manager.
#   2. Run `bash scripts/plan-hygiene/run_hygiene_sweep.sh --ci` — exits 1
#      if any HARD check fails (todo regression or frontmatter violations).
#   3. If failures: append a notification to BOTH orchestrator inboxes
#      (ikenna_orchestrator/_agent_pings.md + harsh_orchestrator/_agent_pings.md)
#      and commit + push back to live-defi-rollout.
#   4. Exit 0 always (failures are surfaced via inbox notifications).
#
# Image
#   `gcr.io/google.com/cloudsdktool/google-cloud-cli:slim` — has bash + git +
#   python3 preinstalled. No private image build needed.
#
# Service account
#   * Scheduler invoker + container runtime: `t1_batch_sa` (already has
#     roles/run.invoker; GH_PAT secret accessor already granted via
#     orphan_ping_audit_scheduler.tf).
#
# Entrypoint:
#   unified-trading-pm/scripts/plan-hygiene/cron_hygiene_sweep_entrypoint.sh

# -------------------------------------------------------
# Cloud Run Job — daily plan hygiene sweep
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
          "git clone --depth=10 --branch=live-defi-rollout https://x-access-token:$${GH_PAT}@github.com/IggyIkenna/unified-trading-pm.git /tmp/unified-trading-pm && bash /tmp/unified-trading-pm/scripts/plan-hygiene/cron_hygiene_sweep_entrypoint.sh",
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
          value = "hygiene-sweep-cron@odum-research.com"
        }
        env {
          name  = "GIT_COMMITTER_NAME"
          value = "hygiene-sweep-cron"
        }
        env {
          name  = "GIT_AUTHOR_EMAIL"
          value = "hygiene-sweep-cron@odum-research.com"
        }
        env {
          name  = "GIT_AUTHOR_NAME"
          value = "hygiene-sweep-cron"
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
    "purpose"    = "plan-hygiene-sweep"
    "owner"      = "ikenna"
  }

  depends_on = [
    google_secret_manager_secret_iam_member.t1_batch_gh_pat_accessor,
  ]
}

# -------------------------------------------------------
# Cloud Scheduler — daily at 05:00 UTC
# -------------------------------------------------------
resource "google_cloud_scheduler_job" "plan_hygiene_sweep_cron" {
  name        = "${local.env_prefix}-plan-hygiene-sweep-cron"
  description = "Run plan-hygiene sweep daily at 05:00 UTC. Appends failure notifications to orchestrator inboxes. CLAUDE.md Plan Hygiene HARD RULE."
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
