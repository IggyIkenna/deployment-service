# REACTIVATED — this Cloud Run Job + Cloud Scheduler cron IS the live
# production mechanism (confirmed 2026-07-08: `gcloud scheduler jobs describe
# uts-prod-sports-scheduler-cron` ENABLED, `gcloud run jobs executions list
# --job=uts-prod-sports-scheduler` shows real executions every 5 min). The
# stale "DEFERRED... VM instead" note that used to be here was wrong — no
# `sports-scheduler-*` VM has been running (`gcloud compute instances list`
# returns zero matches); Plans 12/13 unblocked the Cloud Build path at some
# point after 2026-04-22 and this job was reactivated without anyone updating
# this comment. `launch-sports-scheduler-vm.sh` still exists as a documented
# fallback shape but is NOT currently in use.
#
# 2026-07-08: fixed a silent zero-dispatch bug where the container's CMD
# never passed `--backend`/`--cloud-run-*` to `sports-trigger run`, so every
# invocation defaulted to `backend="local"` — which cannot work in this
# image (ships only deployment-service, not instruments-service /
# market-tick-data-service / features-service). See
# unified-trading-pm/plans/active/issues/
# sports_trigger_scheduler_cloud_dispatch_broken_2026_07_08.md. Fix: pass
# `--backend cloud` + the real Cloud Run Job dispatch targets (see
# configs/sports-trigger-tiers.yaml).
#
# Sports Scheduler — Cloud Run Job + Cloud Scheduler Cron
#
# Plan: sports_scheduler_cron_activation_2026_04_21
# Depends on: sports_scheduler_periodic_tier_dispatch (deployment-service d9652cd)
#
# Operational shape (Option B per plan §"Operational shape"):
#   * Cloud Scheduler fires every 5 min on a cron.
#   * Each tick triggers Cloud Run Job `sports-scheduler` which runs
#     `python -m deployment_service sports-trigger run --one-shot --backend cloud ...`.
#   * One cycle = evaluate Tier-1 discovery + Tier-2 reference cadences +
#     fixture-proximate Tier-3/4 triggers, dispatch any that are due, exit 0.
#   * Cadence is preserved across short-lived containers by persisting
#     last_run[tier] to gs://deployment-scripts-<project>/sports_scheduler_state/
#     (written by PeriodicTierState; SSOT for scheduler state).
#
# Why Cloud Run Job, not Cloud Run Service:
#   * Jobs bill per-invocation, not 24/7 idle CPU between polls.
#   * Jobs are the canonical shape for cron-driven short-lived work in GCP.
#   * Matches codex §8 (Cloud Run warm-start ~1s, cheap per-invocation).
#
# Image built by cloudbuild.yaml Step 3b (`--target sports-scheduler`)
# under the same artifact repo as the deployment-dashboard, so IAM and
# vulnerability scanning inherit the existing setup.

# -------------------------------------------------------
# Service account — reuse t1_batch (already has run.invoker)
# -------------------------------------------------------
# The scheduler job needs:
#   * roles/run.invoker — so Cloud Scheduler can trigger the job (inherited
#     via google_project_iam_member.t1_batch_run_invoker in t1_batch_scheduler.tf).
#   * roles/storage.objectAdmin on deployment-scripts-<project> — to read/write
#     sports_scheduler_state/ (handled at the project level via the unified
#     unified-trading-sa in main.tf; the scheduler job runs as that SA).
#   * roles/compute.instanceAdmin.v1 — to launch child VM backfills via
#     `gcloud compute instances create` invoked by per-fixture dispatch.
#
# The job spec below binds to `unified-trading-sa` (existing) which already has
# storage.objectAdmin + pubsub.editor. The compute.instanceAdmin grant lives in
# the broader project IAM — add there if missing.

# -------------------------------------------------------
# Cloud Run Job — sports-scheduler
# -------------------------------------------------------
# 5-minute target wall-time per invocation (one evaluation cycle); timeout set
# to 15 min for headroom on fixture-heavy windows where several child-VM
# launches are dispatched sequentially.
resource "google_cloud_run_v2_job" "sports_scheduler" {
  name     = "${local.env_prefix}-sports-scheduler"
  location = var.region
  project  = var.project_id

  deletion_protection = false

  template {
    # Single task per invocation — no parallel fan-out; the scheduler itself
    # decides what to dispatch.
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.unified_trading.email

      # Image published by cloudbuild.yaml push-sports-scheduler step.
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/deployment-dashboard/sports-scheduler:latest"

        # CMD override — Dockerfile CMD is `["python", "-m", "deployment_service",
        # "sports-trigger", "run", "--one-shot"]`; args here REPLACES that whole
        # CMD (ENTRYPOINT stays `/usr/bin/tini --`), appending the dispatch
        # backend flags so triggers actually reach instruments-service /
        # market-tick-data-service / features-sports-service via the real,
        # already-provisioned Cloud Run Jobs named in
        # configs/sports-trigger-tiers.yaml (see 2026-07-08 fix note above).
        args = [
          "python", "-m", "deployment_service", "sports-trigger", "run", "--one-shot",
          "--backend", "cloud",
          "--cloud-run-project", var.project_id,
          "--cloud-run-region", var.region,
          "--cloud-run-service-account", google_service_account.unified_trading.email,
        ]

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "DEPLOYMENT_ENV"
          value = var.environment
        }
        # State bucket is auto-resolved by resolve_state_bucket(), which reads
        # DeploymentConfig.project_id. Setting explicitly for clarity / override.
        env {
          name  = "SPORTS_SCHEDULER_STATE_BUCKET"
          value = "deployment-scripts-${var.project_id}"
        }
        # Manifest-429 per-VM sharding — every container that calls
        # ManifestWriter writes to its own shard file, the consolidator
        # cron (manifest_consolidator_scheduler.tf) merges them back.
        env {
          name  = "MANIFEST_PER_VM_SHARDS"
          value = "true"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }

      timeout     = "900s" # 15 min — headroom over nominal 5-min wall-time
      max_retries = 1
    }
  }

  labels = merge(local.common_labels, {
    "purpose" = "sports-scheduler",
    "tier"    = "scheduler"
  })
}

# -------------------------------------------------------
# Cloud Scheduler Cron — every 5 min, UTC
# -------------------------------------------------------
# 5-minute cadence matches the scheduler's existing internal poll_interval
# (sports_trigger_scheduler.SportsTriggerScheduler.poll_interval_seconds=300).
# Ensures fixture-proximate triggers (odds T-1h, T-6h, T-24h) fire within
# their tolerance windows (15-30 min — see configs/sports-trigger-tiers.yaml).
resource "google_cloud_scheduler_job" "sports_scheduler_cron" {
  name        = "${local.env_prefix}-sports-scheduler-cron"
  description = "Sports fixture-aware trigger scheduler — evaluates Tier-1/2/3/4 cadences every 5 min"
  schedule    = "*/5 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.sports_scheduler.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # Retry once on transient 5xx — scheduler is idempotent (cadence gates via
  # last_run[tier] in GCS state) so a second fire within 5 min is a no-op.
  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "sports_scheduler_job_name" {
  description = "Cloud Run Job name for the sports scheduler — referenced by Cloud Scheduler"
  value       = google_cloud_run_v2_job.sports_scheduler.name
}

output "sports_scheduler_cron_name" {
  description = "Cloud Scheduler cron job name — drives the 5-min cadence"
  value       = google_cloud_scheduler_job.sports_scheduler_cron.name
}
