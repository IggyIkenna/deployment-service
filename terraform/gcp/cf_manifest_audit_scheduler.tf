# CF Manifest Audit (Cross-AG data-state audit) — Cloud Run Job + daily Scheduler cron
#
# Plan: audit_criteria_automation_2026_06_08.md Phase 2 (Tier-3 continuous-verification cron)
# Pattern source: codex/05-infrastructure/manifest-consolidator-ssot.md
#   + manifest_consolidator_scheduler.tf (Cloud Run Job + Cloud Scheduler shape)
#
# Background
#   The master plan (master_to_live_defi_2026_05_23.md Groups A-G) requires a
#   continuous-verification path for data-state completeness across all 5 asset_groups.
#   This file ships the daily cron that runs the cross-AG CF manifest audit wrapper
#   (being built in unified-trading-pm; see below for the entrypoint contract) once per
#   day at 06:00 UTC — AFTER the manifest consolidator (*/1 * * * *) has run — and
#   ALERTS on any RED result by exiting non-zero so the Cloud Run execution surfaces
#   RED in Cloud Console + any Cloud Monitoring alert that fires on non-zero job exits.
#
# Schedule
#   `0 6 * * *` UTC — daily, after consolidator cycles have settled overnight data.
#   Runs ONCE per day (not per-minute) because CF audit scans all 5 asset_groups ×
#   {market-data-tick, instruments-store} buckets and is IO-intensive (~2-5 min).
#
# Entrypoint contract
#   Command: `python -m unified_trading_library.cf_manifest_audit --all-ags
#   --json-out gs://<audit-bucket>/cf_audit/<date>.json`
#   - Loops all 5 asset_groups × {market-data-tick, instruments-store} = 10 CF checks.
#   - Emits a per-CF GREEN/RED rollup to stdout + a machine-readable JSON summary to GCS.
#   - Exits NON-ZERO if ANY CF is RED (enabling this job's execution to surface as a failure).
#   FIXED 2026-07-10: this job originally shipped as `command = ["cf-manifest-audit-all"]`,
#   a console-script entry point that was never actually packaged/installed anywhere — the
#   script lived as a bare file in unified-trading-pm, never wired into the MTDS image, so
#   every run failed to exec (0 objects written for ~3 weeks, no alert since the exec
#   failure itself never reached Cloud Monitoring's log-based filter). Fixed by moving the
#   audit logic into `unified_trading_library.cf_manifest_audit` (mirrors
#   `manifest_consolidator.py`'s exact deployment shape below) — MTDS already bundles UTL,
#   so no dedicated image/build pipeline is needed.
#
# Alert-on-RED mechanism
#   Cloud Run Jobs emit a `google.cloud.run.job.v1.JobFailed` Pub/Sub notification when
#   the job exits non-zero. A Cloud Monitoring log-based alert (below) filters on
#   resource.type="cloud_run_job" AND resource.labels.job_name matching this job's name
#   AND severity=ERROR (Cloud Run writes a failed-execution log entry at ERROR severity).
#   This mirrors how `CONSOLIDATOR_DOWN` is detected — non-zero exit = observable signal.
#
# Image
#   Defaults to `market-tick-data-service:latest` (UTL bundled as dep — same as the
#   consolidator job; no dedicated image needed). `var.cf_audit_image` remains as an
#   override escape hatch if a future need arises.
#
# Service account
#   * Scheduler invoker: `t1_batch_sa` (already has roles/run.invoker).
#   * Container runtime: `unified_trading_sa` (storage.objectReader on all manifest
#     buckets — reads availability_index.parquet per bucket; objectAdmin on the audit
#     output bucket so the JSON summary can be written).

variable "cf_audit_image" {
  description = "Container image for the cf-manifest-audit job. Defaults to market-tick-data-service (UTL bundled — runs unified_trading_library.cf_manifest_audit). Override only if a dedicated image is ever needed."
  type        = string
  default     = "" # empty = fall through to local default below
}

locals {
  # Audit output bucket: one shared bucket for daily CF audit JSON summaries.
  # Group B naming (flat, no env-suffix) per cloud-providers.yaml convention.
  cf_audit_output_bucket = "cf-manifest-audit-${var.project_id}"

  # Image: prefer the explicit variable; fall back to the MTDS image (UTL included).
  # The comment-contract above documents when to override this.
  cf_audit_image_resolved = var.cf_audit_image != "" ? var.cf_audit_image : "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"
}

# -------------------------------------------------------
# GCS output bucket — JSON audit summaries, 90-day retention
# -------------------------------------------------------
# Each daily run writes `cf_audit/YYYY-MM-DD.json` (the wrapper's --json-out path).
# 90-day retention keeps ~3 months of audit history for trend analysis.
resource "google_storage_bucket" "cf_manifest_audit_output" {
  name          = local.cf_audit_output_bucket
  project       = var.project_id
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true

  labels = {
    purpose = "cf-manifest-audit"
  }
}

# objectAdmin for the runtime SA on the output bucket (write JSON summaries).
resource "google_storage_bucket_iam_member" "cf_manifest_audit_output_runtime" {
  bucket = google_storage_bucket.cf_manifest_audit_output.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.unified_trading.email}"
}

# -------------------------------------------------------
# Cloud Run Job — single job, runs the cross-AG CF audit
# -------------------------------------------------------
# Single job (not per-asset-group) — the wrapper loops all AGs internally.
# max_retries = 0: a non-zero exit IS the alert signal; we do not want a retry
# that masks the failure or double-fires the alert. Next day's cron re-checks.
module "cf_manifest_audit_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-cf-manifest-audit"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  # See `cf_audit_image_resolved` local — defaults to MTDS image (UTL bundled).
  # Override var.cf_audit_image once the PM audit wrapper ships its own release.
  image = local.cf_audit_image_resolved

  # CF audit is IO-bound (GCS metadata reads across 10 buckets) but not CPU-heavy.
  # 2 vCPU / 4Gi is sufficient; the per-bucket CF manifest read is sequential.
  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800 # 30 min upper bound; typical run 2-5 min for 10 buckets

  max_retries = 0 # non-zero exit = RED alert; do not retry (masks the signal)
  parallelism = 1
  task_count  = 1

  # unified_trading_library.cf_manifest_audit — bundled in the MTDS image via UTL,
  # same deployment shape as unified_trading_library.manifest_consolidator below.
  # --all-ags: loop all 5 asset_groups × {market-data-tick, instruments-store}.
  # --json-out: directory prefix — the {YYYY-MM-DD}.json filename is computed at
  # runtime inside the script (Cloud Run Job args are an exec array, no shell, so
  # a literal $(date ...) here would never be evaluated — see module docstring).
  # EXIT NON-ZERO if any CF is RED (enables Cloud Monitoring alert on job failure).
  command = ["python"]
  args = [
    "-m", "unified_trading_library.cf_manifest_audit",
    "--all-ags",
    "--json-out", "gs://${local.cf_audit_output_bucket}/cf_audit",
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "cf-manifest-audit"
  environment  = var.environment

  labels = {
    "purpose" = "cf-manifest-audit"
    "tier"    = "continuous-verification"
    "plan"    = "audit-criteria-automation-2026-06-08"
  }
}

# -------------------------------------------------------
# Cloud Scheduler cron — daily at 06:00 UTC
# -------------------------------------------------------
# 06:00 UTC is after the manifest consolidator (*/1 * * * *) has run overnight
# and the T+1 pipeline has settled (T+1 batch completes by ~05:30 UTC).
# One missed cron cycle = gap in the audit trail, but no data-pipeline impact
# (the audit is read-only; the underlying manifests are untouched).
resource "google_cloud_scheduler_job" "cf_manifest_audit_cron" {
  name        = "${local.env_prefix}-cf-manifest-audit-cron"
  description = "Daily cross-AG CF manifest audit — loops all 5 asset_groups; alerts on any RED CF. Continuous-verification path for master plan Groups A-G. Plan: audit_criteria_automation_2026_06_08.md Phase 2."
  schedule    = "0 6 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.cf_manifest_audit_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries — non-zero exit is the RED alert; the next day's cron re-checks.
  # Mirrors the consolidator pattern (retry_count = 0).
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

# -------------------------------------------------------
# Cloud Monitoring log-based alert — fires on RED (non-zero job exit)
# -------------------------------------------------------
# Cloud Run Jobs write an ERROR-severity log entry when a job execution
# fails (non-zero exit). This alert policy surfaces that signal as a
# Google Cloud Monitoring incident (→ Notification Channels wired at
# workspace level: Slack / PagerDuty / email per the alerting-service pattern).
#
# The filter targets:
#   resource.type="cloud_run_job"
#   resource.labels.job_name = the CF audit job name
#   severity = "ERROR"
# which fires exactly when the job exits non-zero (i.e. any CF is RED).
#
# NOTE: google_monitoring_notification_channel IDs are provisioned outside
# this file (workspace-level alerting setup). Set var.alert_notification_channels
# to wire them in. Defaults to empty (alert exists, no notification until wired).
variable "cf_audit_alert_notification_channels" {
  description = "List of Cloud Monitoring notification channel IDs to wire to the CF audit RED alert. Wired at workspace-level alerting setup. Empty list = alert policy exists but no external notification."
  type        = list(string)
  default     = []
}

resource "google_monitoring_alert_policy" "cf_manifest_audit_red" {
  display_name = "${local.env_prefix} CF Manifest Audit — RED (any asset_group)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  # Alert when a CF audit job execution fails (non-zero exit = any CF is RED).
  conditions {
    display_name = "CF audit job failed (RED CF detected)"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_run_job"
        resource.labels.job_name="${module.cf_manifest_audit_job.name}"
        severity="ERROR"
        textPayload=~"execution failed"
      EOT

      label_extractors = {
        "job_name" = "EXTRACT(resource.labels.job_name)"
      }
    }
  }

  notification_channels = var.cf_audit_alert_notification_channels

  alert_strategy {
    notification_rate_limit {
      # At most one alert per 24h — the cron is daily, so one alert per RED day
      # is sufficient (avoids noisy multi-alert from Cloud Run retries if ever enabled).
      period = "86400s"
    }
    auto_close = "604800s" # 7 days — auto-close stale incidents
  }

  documentation {
    content   = "The daily CF manifest audit detected a RED cross-file (CF) in at least one asset_group. Check the audit JSON output at gs://${local.cf_audit_output_bucket}/cf_audit/ for per-CF details. Plan: audit_criteria_automation_2026_06_08.md Phase 2. Pattern: codex/05-infrastructure/manifest-consolidator-ssot.md."
    mime_type = "text/markdown"
  }

  # google_monitoring_alert_policy uses user_labels (a top-level `labels`
  # argument is not part of the resource schema — caught at first validate).
  user_labels = {
    "purpose" = "cf-manifest-audit-alert"
    "tier"    = "continuous-verification"
  }
}

# -------------------------------------------------------
# Outputs
# -------------------------------------------------------
output "cf_manifest_audit_job_name" {
  description = "Cloud Run Job name for the CF manifest audit (for ad-hoc runs: `gcloud run jobs execute`)."
  value       = module.cf_manifest_audit_job.name
}

output "cf_manifest_audit_cron_name" {
  description = "Cloud Scheduler cron name for the daily CF manifest audit."
  value       = google_cloud_scheduler_job.cf_manifest_audit_cron.name
}

output "cf_manifest_audit_output_bucket" {
  description = "GCS bucket holding daily CF audit JSON summaries (gs://<bucket>/cf_audit/YYYY-MM-DD.json)."
  value       = google_storage_bucket.cf_manifest_audit_output.name
}

output "cf_manifest_audit_alert_policy_name" {
  description = "Cloud Monitoring alert policy name — fires when the CF audit exits non-zero (RED)."
  value       = google_monitoring_alert_policy.cf_manifest_audit_red.name
}
