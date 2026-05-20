# Orphan-Ping Audit — Cloud Run Job + Cloud Scheduler cron (4h cadence)
#
# Plan-of-record:
#   CLAUDE.md HARD RULE "Every Active Ping Must Reference A Plan Item"
#   (codified 2026-05-20 round 5; cadence tightened round 6).
#
# Cron stack (intentionally redundant — the two passes catch each other's misses):
#   - Local (Ikenna's laptop crontab): `0 */4 * * *` UTC.
#   - Cloud (this file):              `15 2,6,10,14,18,22 * * *` UTC (2h offset).
#
# What the Job does:
#   1. Clone unified-trading-pm @ live-defi-rollout using a GH_PAT loaded
#      from Secret Manager.
#   2. Run `scripts/agents/audit_ping_orphans.sh` (script appends
#      orphan-notification entries to BOTH orchestrator inboxes if orphans
#      are detected).
#   3. If ping-ledger files dirty, commit + push back to live-defi-rollout
#      so both operators' tab worktrees see the notifications on next
#      `git fetch`.
#
# Image
#   `gcr.io/google.com/cloudsdktool/google-cloud-cli:slim` — Google's public
#   Cloud SDK slim image has bash + git + gcloud preinstalled. No private
#   image build needed; the cron only needs a shell + git + the GH_PAT.
#
# Service account
#   * Scheduler invoker + container runtime: `t1_batch_sa` (already has
#     roles/run.invoker; granted `roles/secretmanager.secretAccessor` on
#     `GH_PAT` via `secret_rotation.tf` (or one-shot gcloud during initial
#     install — IaC-reconciled on next terraform apply).
#
# First install (2026-05-20)
#   Resources were created out-of-band via `gcloud run jobs create` +
#   `gcloud scheduler jobs create http` (one-shot manual exec verified the
#   clone+audit+push chain works end-to-end). This file is the IaC
#   reconciler — `terraform plan` should be a no-op against the live
#   resources. Initial commit: unified-trading-pm@4588737a0 (entrypoint
#   script) + deployment-service@<this commit> (terraform).

# Bind GH_PAT secret-accessor to the t1_batch SA (used by the orphan-ping
# audit container runtime; one-shot grant was applied via gcloud during
# initial install — this resource reconciles it as IaC).
resource "google_secret_manager_secret_iam_member" "t1_batch_gh_pat_accessor" {
  secret_id = "GH_PAT"
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.t1_batch.email}"
}

# -------------------------------------------------------
# Cloud Run Job — orphan-ping audit
# -------------------------------------------------------
resource "google_cloud_run_v2_job" "orphan_ping_audit" {
  name                = "${local.env_prefix}-orphan-ping-audit"
  location            = var.region
  project             = var.project_id
  deletion_protection = false

  template {
    parallelism = 1
    task_count  = 1

    template {
      timeout               = "300s"
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
          "git clone --depth=10 --branch=live-defi-rollout https://x-access-token:$${GH_PAT}@github.com/IggyIkenna/unified-trading-pm.git /tmp/pm && bash /tmp/pm/scripts/agents/cron_orphan_ping_audit_entrypoint.sh",
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
          value = "orphan-ping-cron@odum-research.com"
        }
        env {
          name  = "GIT_COMMITTER_NAME"
          value = "orphan-ping-cron"
        }
        env {
          name  = "GIT_AUTHOR_EMAIL"
          value = "orphan-ping-cron@odum-research.com"
        }
        env {
          name  = "GIT_AUTHOR_NAME"
          value = "orphan-ping-cron"
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
    "service"    = "orphan-ping-audit"
    "purpose"    = "orphan-ping-audit"
    "owner"      = "ikenna"
  }

  depends_on = [
    google_secret_manager_secret_iam_member.t1_batch_gh_pat_accessor,
  ]
}

# -------------------------------------------------------
# Cloud Scheduler — 4h cadence offset by 2h from local cron
# -------------------------------------------------------
# Local cron: `0 */4 * * *` (UTC) → fires at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00.
# Cloud cron (this):                fires at 02:15, 06:15, 10:15, 14:15, 18:15, 22:15.
# 2h offset + 15min skew so the two passes don't collide and each catches
# orphans the other might miss between cycles.
resource "google_cloud_scheduler_job" "orphan_ping_audit_cron" {
  name        = "${local.env_prefix}-orphan-ping-audit-cron"
  description = "Run audit_ping_orphans.sh every 4h offset by 2h from local cron. Pushes orphan notifications back to live-defi-rollout. CLAUDE.md HARD RULE."
  schedule    = "15 2,6,10,14,18,22 * * *"
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.orphan_ping_audit.name}:run"

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
output "orphan_ping_audit_job_name" {
  description = "Cloud Run Job name for ad-hoc manual fire (`gcloud run jobs execute`)."
  value       = google_cloud_run_v2_job.orphan_ping_audit.name
}

output "orphan_ping_audit_cron_name" {
  description = "Cloud Scheduler cron name."
  value       = google_cloud_scheduler_job.orphan_ping_audit_cron.name
}
