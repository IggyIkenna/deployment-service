# Data-Pipeline Fleet Monitors — Cloud Run Jobs + Scheduler crons
#
# Plan: data_pipeline_hardening_self_monitoring_2026_06_22 (Phase 2, Wave 4a)
# Code: deployment_service.data_pipeline_monitors (this repo)
#   - exit_code_fleet_monitor (DP-VM-001/002) — self-delete-proof exit_code +
#     captured cross-check (CLAUDE.md 2026-06-22 rule).
#   - heartbeat_stall_watcher (DP-VM-003/004) — PIPELINE_HEARTBEAT staleness.
#   - meta_watchers (DP-CATALOG-001 / DP-WATCHER-001/002) — catalogue /
#     zombie-watchdog / cron freshness probes.
#
# Topology
#   Three Cloud Run Jobs (one per mode) on the MTDS image (UTL bundled). Each
#   reads the persisted GCS run.log/EXIT_STATUS + per-VM manifest shards + the
#   durable artifact freshness; emits DP_* events that the alerting-service
#   router mirrors to #data-pipeline-alerts (CRITICAL also pages). Mirrors the
#   consolidator-liveness watchdog topology (consolidator_liveness_scheduler.tf):
#   single stateless job per mode, exit-0-always (alerting is event-driven).
#
# Cadence
#   exit-code + heartbeat : */5  (matches the 5-min zombie-watchdog poll — a
#                                 self-deleted OOM VM must be caught within one
#                                 census window).
#   meta                  : */15 (catalogue 24h / watchdog 30m / cron budgets are
#                                 coarse — 15 min is ample, keeps Actions/run cost low).
#
# Retry (P1, 2026-06-23 watch-the-watchers)
#   The scheduler jobs carry retry_count=2 so a transient Cloud Run invocation
#   failure does not silently drop a monitor tick (the JOB itself stays
#   max_retries=0 / exit-0-always — the retry is on the SCHEDULER→Run invocation).
#
# Service account
#   * Scheduler invoker: t1_batch_sa (roles/run.invoker).
#   * Container runtime: unified_trading_sa (storage read on the log + manifest
#     buckets, compute read to list RUNNING VMs, SM accessor on the
#     DATA_PIPELINE_ALERTS_SLACK_WEBHOOK so the runtime can post directly when the
#     alerting-service router path is unavailable).

locals {
  # The monitor entrypoint is `python -m deployment_service.data_pipeline_monitors.cli`
  # — that package lives ONLY in the deployment-service image (the `api` Dockerfile
  # target COPYs `deployment_service/`), NOT the MTDS image. The MTDS image bundles UTL
  # (which is why the consolidator job — `-m unified_trading_library.manifest_consolidator`
  # — reuses it), but `deployment_service.*` is absent there → ModuleNotFoundError.
  # So the monitors run on the deployment-service `deployment-api` image.
  # (Fixed 2026-06-22: the original MTDS-image pick crashed every cron with exit 1.)
  data_pipeline_monitor_image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/deployment-api:latest"
}

# --- SA accessor: DATA_PIPELINE_ALERTS_SLACK_WEBHOOK ---
# Mirrors the *_accessor IAM members (hygiene_sweep_scheduler.tf). Binds the
# unified_trading SA — which runs BOTH these monitor jobs AND the alerting-service
# Cloud Run job (audit03_cron_provisioning.tf alerting_paging_job uses the same
# SA) — so the alerting-service router AND the monitor runtime can read the
# webhook secret created in Phase 0. The webhook is the #data-pipeline-alerts
# carrier. t1_batch (the scheduler invoker) is bound too for parity.
resource "google_secret_manager_secret_iam_member" "unified_trading_data_pipeline_webhook_accessor" {
  secret_id = "DATA_PIPELINE_ALERTS_SLACK_WEBHOOK"
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.unified_trading.email}"
}

resource "google_secret_manager_secret_iam_member" "t1_batch_data_pipeline_webhook_accessor" {
  secret_id = "DATA_PIPELINE_ALERTS_SLACK_WEBHOOK"
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.t1_batch.email}"
}

# --- Cloud Run Jobs (one per mode) ---
module "data_pipeline_exit_code_monitor_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-exit-code-monitor"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.data_pipeline_monitor_image

  # 8Gi/cpu2 (bumped from 2Gi/cpu1 2026-06-23) — the exit-code sweep reads per-VM shards for
  # the whole RUNNING fleet (same heavy load as heartbeat), so it OOM'd at 2Gi AND 4Gi on every
  # */5 run (signal 9) → its last-run sentinel was never written → the out-of-band deadman paged
  # it stale ("dp-exit-code-monitor — sentinel stale: never ran"). Cloud Run requires cpu>=2 at 8Gi.
  # timeout_seconds bumped 300 -> 900 (2026-07-29) — the fleet has grown enough since the memory
  # fix that the OOM was traded for a TIMEOUT: real logs (`gcloud logging read`, job
  # uts-prod-dp-exit-code-monitor) show "Terminating task because it has reached the maximum
  # timeout of 300 seconds" on EVERY execution since at least 2026-07-27T05:00 UTC — this monitor
  # has been silently failing to complete a single sweep for 2+ days, so DP_VM_PREEMPTED never
  # fires and no SPOT-preempted VM (of any kind, not just this session's) has been auto-recovered
  # in that window (zero "DP_VM_PREEMPTED" log lines in 3 days of history) — a genuinely broken,
  # not merely slow, safety net. SSOT:
  # plans/active/issues/cefi_migration_vm_launcher_no_sharding_and_spot_preemption_churn_2026_07_28.md.
  cpu             = "2"
  memory          = "8Gi"
  timeout_seconds = 900
  max_retries     = 0 # exit-0-always; the next cron cycle re-checks
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "deployment_service.data_pipeline_monitors.cli",
    "--mode", "exit-code",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "dp-exit-code-monitor"
  environment  = var.environment

  labels = {
    "purpose" = "dp-exit-code-monitor"
  }
}

module "data_pipeline_heartbeat_watcher_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-heartbeat-watcher"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.data_pipeline_monitor_image

  # 16Gi/cpu4 (bumped from 8Gi/cpu2 2026-08-02, DP-WATCHER-002 escalation agt-4cb519) — the
  # fleet grew past what 8Gi supports: `gcloud run jobs executions list` showed the sweep
  # flapping between OOM ("configured memory limit was reached", exit 0) and success from
  # 2026-08-02T10:04Z onward (continuous OOM 10:18-11:33Z, 15 straight failed */5 runs), the
  # exact same growth-past-the-current-ceiling pattern the meta-watcher hit on 2026-06-24 (also
  # fixed by doubling memory, 8Gi->16Gi/cpu4 there). A failed run never reaches the sentinel
  # write (`_gcs.write_monitor_last_run`), so every OOM'd sweep silently drops a heartbeat
  # tick — the meta sweep's `check_monitor_crons_fired` sees the sentinel go stale past its
  # 2x-cadence (10 min) budget and pages DP_CRON_DID_NOT_FIRE (DP-WATCHER-002). Was 8Gi/cpu2
  # (bumped from 2Gi/cpu1 2026-06-23) — the heartbeat sweep reads the full VM census +
  # per-VM shard rows for the whole RUNNING fleet, so it OOM'd at 2Gi AND 4Gi on every */5 run
  # before that fix too. exit-code stays 8Gi/cpu2 (bumped 2026-06-23, no OOM observed there
  # currently) — re-bump it too if the same flapping pattern recurs there.
  cpu             = "4"
  memory          = "16Gi"
  timeout_seconds = 300
  max_retries     = 0
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "deployment_service.data_pipeline_monitors.cli",
    "--mode", "heartbeat",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "dp-heartbeat-watcher"
  environment  = var.environment

  labels = {
    "purpose" = "dp-heartbeat-watcher"
  }
}

module "data_pipeline_meta_watchers_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-meta-watchers"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.data_pipeline_monitor_image

  # 16Gi/cpu4 (bumped from 8Gi 2026-06-24) — the meta sweep OOM'd at 2/4/8Gi (signal 9) on the
  # UTL import baseline + the execution-history cross-check + catalogue/census reads (the corpus
  # grew after the consolidator merged ~18.6M rows) → its sentinel went stale → deadman paged
  # "dp-meta-monitor stale: 45m". 8Gi was insufficient under the larger index; 16Gi is green.
  cpu             = "4"
  memory          = "16Gi"
  timeout_seconds = 300
  max_retries     = 0
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "deployment_service.data_pipeline_monitors.cli",
    "--mode", "meta",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "dp-meta-watchers"
  environment  = var.environment

  labels = {
    "purpose" = "dp-meta-watchers"
  }
}

# --- Cloud Scheduler crons ---
resource "google_cloud_scheduler_job" "dp_exit_code_monitor_cron" {
  name        = "${local.env_prefix}-dp-exit-code-monitor-cron"
  description = "Exit_code-aware fleet monitor: per-terminated-VM run.log exit_code + captured cross-check; emit DP_VM_EXIT_NONZERO / DP_VM_GONE_NO_CAPTURE."
  schedule    = "*/5 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.data_pipeline_exit_code_monitor_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 2
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

resource "google_cloud_scheduler_job" "dp_heartbeat_watcher_cron" {
  name        = "${local.env_prefix}-dp-heartbeat-watcher-cron"
  description = "Heartbeat-stall watcher: PIPELINE_HEARTBEAT staleness across RUNNING VMs; emit DP_VM_STALL / DP_EVENT_LOOP_STARVED."
  schedule    = "*/5 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.data_pipeline_heartbeat_watcher_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 2
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

resource "google_cloud_scheduler_job" "dp_meta_watchers_cron" {
  name        = "${local.env_prefix}-dp-meta-watchers-cron"
  description = "Meta-watchers: catalogue / zombie-watchdog / cron freshness probes; emit DP_CATALOG_NOT_RUNNING / DP_ZOMBIE_WATCHDOG_DOWN / DP_CRON_DID_NOT_FIRE."
  schedule    = "*/15 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.data_pipeline_meta_watchers_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 2
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# --- Outputs ---
output "dp_exit_code_monitor_job_name" {
  description = "Cloud Run Job name for the exit_code-aware fleet monitor."
  value       = module.data_pipeline_exit_code_monitor_job.name
}

output "dp_heartbeat_watcher_job_name" {
  description = "Cloud Run Job name for the heartbeat-stall watcher."
  value       = module.data_pipeline_heartbeat_watcher_job.name
}

output "dp_meta_watchers_job_name" {
  description = "Cloud Run Job name for the meta-watchers."
  value       = module.data_pipeline_meta_watchers_job.name
}
