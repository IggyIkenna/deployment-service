# Data-Pipeline Self-Monitoring Audits — Cloud Run Jobs + daily Schedulers
#
# Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md Wave-4b
#   "Schedule the three daily-audit crons" (INFRA P1).
# Pattern source: cf_manifest_audit_scheduler.tf (Cloud Run Job + Cloud Scheduler
#   shape: SA `unified_trading_sa` runtime, `t1_batch_sa` invoker, max_retries=0 /
#   parallelism=1 / task_count=1, env GCP_PROJECT_ID/DEPLOYMENT_ENV/CLOUD_PROVIDER).
#
# Background
#   Wave 1-4a shipped the 3 e2e self-monitoring audit scripts (in `e2e-testing`)
#   and the alerting-service `DP_*` → `#data-pipeline-alerts` router. This file
#   makes those audits actually RUN daily — the last hop that turns the data
#   pipeline self-monitoring LIVE. Each script reads the per-AG availability index,
#   emits `DP_*` events via UTL `log_event` (the alerting-service router mirrors
#   them to Slack — we never post to Slack directly), and EXITS 0 BY DESIGN (a
#   finding is an alert, not a job failure). So — unlike cf-manifest-audit — there
#   is NO alert-on-nonzero-exit Cloud Monitoring policy here; the alert path is the
#   `DP_*` event → alerting-service, not the job exit code.
#
# The 3 audits + cadence (all UTC, after the 06:00 CF audit / T+1 settle):
#   data_pipeline_daily_digest.py        0 7 * * *   per-AG completion digest (DP_DAILY_DIGEST)
#   manifest_hygiene_daily.py --mode changed   0 8 * * *   daily index-only hygiene (manifest-readable checks)
#   manifest_hygiene_daily.py --mode full      0 8 * * 0   WEEKLY full GCS walk (Sunday — adds phantom + 4-pillar)
#   reprobe_new_empty_confirmed.py       0 9 * * *   re-probe today's new empty_confirmed cells
#
# Each script takes an optional `--asset-group` (all 5 AGs when omitted — we omit
# it so each job covers the whole fleet in one run).
#
# ─────────────────────────────────────────────────────────────────────────────
# ✅ IMAGE GAP CLOSED (2026-06-22) — the e2e-audit runner image now bundles the scripts.
#
#   The e2e audit scripts live in `e2e-testing/scripts/audit/*.py` (plain python
#   files invoked by PATH, alongside their `_dp_common.py` shared module). The MTDS
#   image's `COPY . .` copies ONLY the MTDS repo, so `market-tick-data-service:latest`
#   does NOT contain `/app/e2e-testing/...` — invoking the scripts against it fails at
#   the script PATH.
#
#   FIX: `e2e-testing/Dockerfile` + `e2e-testing/cloudbuild.yaml` now build a dedicated
#   runner image `unified-trading-library/e2e-audit:latest` that FROMs the UTL base
#   (carries UTL `StorageClient`/`log_event`/the DP_* events + UAC + pandas + gcsfs)
#   AND `COPY . /app/e2e-testing`, so it contains `/app/e2e-testing/scripts/audit/*.py`.
#   The cloudbuild runs each script's credential-free `--smoke` (imports + arg-parse)
#   BEFORE push. `var.dp_audit_image` below now DEFAULTS to that image. The terraform
#   (Jobs + Schedulers + SA wiring) was already correct; this default closes the hop.
# ─────────────────────────────────────────────────────────────────────────────

variable "dp_audit_image" {
  description = "Container image for the data-pipeline self-monitoring audit jobs. Defaults to the e2e-audit runner image (UTL base + e2e-testing/scripts/audit/* bundled, built by e2e-testing/cloudbuild.yaml) — this CLOSES the IMAGE GAP described in the header. Override only to pin a specific digest/SHA over :latest."
  type        = string
  # IMAGE GAP CLOSED (2026-06-22): the e2e-audit image (e2e-testing/Dockerfile +
  # cloudbuild.yaml) FROMs the UTL base AND `COPY . /app/e2e-testing`, so it contains
  # /app/e2e-testing/scripts/audit/*.py that the jobs invoke. (region/project are
  # hardcoded here because a terraform variable default cannot interpolate other vars;
  # region=asia-northeast1, project=central-element-323112 are the only deploy target.)
  default = "asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-library/e2e-audit:latest" # IMAGE GAP CLOSED
}

locals {
  # Image: var.dp_audit_image (now defaults to the e2e-audit runner image that bundles
  # the audit scripts — see the IMAGE GAP CLOSED header). The MTDS-image fallback is kept
  # only as a defensive non-empty-guard; it is NOT reached with the new non-empty default.
  dp_audit_image_resolved = var.dp_audit_image != "" ? var.dp_audit_image : "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  # Path to the audit scripts inside the image (once the image bundles e2e-testing).
  # The MTDS image WORKDIRs at /app/market-tick-data-service; the repo root is /app.
  dp_audit_script_dir = "/app/e2e-testing/scripts/audit"

  # Shared runtime env — identical to cf-manifest-audit. The scripts resolve buckets
  # via `resolve_bucket_name` and read the availability index via UTL `StorageClient`.
  dp_audit_env = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  dp_audit_common_labels = {
    "tier" = "continuous-verification"
    "plan" = "data-pipeline-hardening-self-monitoring-2026-06-22"
  }
}

# -------------------------------------------------------
# Job 1 — daily per-AG completion digest (DP_DAILY_DIGEST) @ 07:00 UTC
# -------------------------------------------------------
module "dp_daily_digest_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-daily-digest"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.dp_audit_image_resolved

  # IO-bound (per-AG availability-index reads). 2 vCPU / 4Gi is sufficient.
  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800

  max_retries = 0 # a finding is a DP_* event, not a job failure; do not retry
  parallelism = 1
  task_count  = 1

  command = ["python3", "${local.dp_audit_script_dir}/data_pipeline_daily_digest.py"]
  # No --asset-group → all 5 AGs in one run.
  args = []

  environment_variables = local.dp_audit_env

  service_name = "dp-daily-digest"
  environment  = var.environment

  labels = merge(local.dp_audit_common_labels, { "purpose" = "dp-daily-digest" })
}

resource "google_cloud_scheduler_job" "dp_daily_digest_cron" {
  name        = "${local.env_prefix}-dp-daily-digest-cron"
  description = "Daily per-AG completion digest → #data-pipeline-alerts (DP_DAILY_DIGEST). Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md Wave-4b."
  schedule    = "0 7 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.dp_daily_digest_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
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
# Job 2 — daily manifest hygiene (--mode changed, index-only) @ 08:00 UTC
# -------------------------------------------------------
module "dp_manifest_hygiene_changed_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-manifest-hygiene-changed"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.dp_audit_image_resolved

  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800 # --mode changed is index-only (no corpus walk) → fast

  max_retries = 0
  parallelism = 1
  task_count  = 1

  command = ["python3", "${local.dp_audit_script_dir}/manifest_hygiene_daily.py"]
  # --mode changed: manifest-readable checks only (v9, divergence, path-canonicality);
  # NO full GCS corpus walk (cost-aware — the operator's #1 ask).
  args = ["--mode", "changed"]

  environment_variables = local.dp_audit_env

  service_name = "dp-manifest-hygiene-changed"
  environment  = var.environment

  labels = merge(local.dp_audit_common_labels, { "purpose" = "dp-manifest-hygiene-changed" })
}

resource "google_cloud_scheduler_job" "dp_manifest_hygiene_changed_cron" {
  name        = "${local.env_prefix}-dp-manifest-hygiene-changed-cron"
  description = "Daily index-only manifest hygiene (v9 / divergence / path-canonicality) → DP_* WARNs. Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md Wave-4b."
  schedule    = "0 8 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.dp_manifest_hygiene_changed_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
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
# Job 3 — WEEKLY manifest hygiene (--mode full, GCS walk) @ 08:00 UTC Sunday
# -------------------------------------------------------
# Reuses the SAME script as job 2 but with `--mode full`, which ADDITIONALLY runs
# the GCS-walking checks (phantom rows, 4-pillar shard validation). Weekly (not
# daily) because the full corpus walk is IO-intensive — cost-aware by design.
module "dp_manifest_hygiene_full_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-manifest-hygiene-full"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.dp_audit_image_resolved

  # Full GCS walk → more memory + a longer ceiling than the index-only daily.
  cpu             = "2"
  memory          = "8Gi"
  timeout_seconds = 7200 # 2h upper bound for the weekly full corpus walk

  max_retries = 0
  parallelism = 1
  task_count  = 1

  command = ["python3", "${local.dp_audit_script_dir}/manifest_hygiene_daily.py"]
  # --mode full: index-only checks PLUS phantom + 4-pillar GCS-existence checks.
  args = ["--mode", "full"]

  environment_variables = local.dp_audit_env

  service_name = "dp-manifest-hygiene-full"
  environment  = var.environment

  labels = merge(local.dp_audit_common_labels, { "purpose" = "dp-manifest-hygiene-full" })
}

resource "google_cloud_scheduler_job" "dp_manifest_hygiene_full_cron" {
  name        = "${local.env_prefix}-dp-manifest-hygiene-full-cron"
  description = "WEEKLY (Sunday) full-GCS-walk manifest hygiene — adds phantom + 4-pillar shard checks → DP_* WARNs. Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md Wave-4b."
  schedule    = "0 8 * * 0"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.dp_manifest_hygiene_full_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
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
# Job 4 — re-probe today's new empty_confirmed cells @ 09:00 UTC
# -------------------------------------------------------
module "dp_reprobe_empty_job" {
  source = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-dp-reprobe-empty"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.dp_audit_image_resolved

  cpu             = "2"
  memory          = "4Gi"
  timeout_seconds = 1800

  max_retries = 0
  parallelism = 1
  task_count  = 1

  command = ["python3", "${local.dp_audit_script_dir}/reprobe_new_empty_confirmed.py"]
  # No --day → defaults to today; no --asset-group → all 5 AGs.
  args = []

  environment_variables = local.dp_audit_env

  service_name = "dp-reprobe-empty"
  environment  = var.environment

  labels = merge(local.dp_audit_common_labels, { "purpose" = "dp-reprobe-empty" })
}

resource "google_cloud_scheduler_job" "dp_reprobe_empty_cron" {
  name        = "${local.env_prefix}-dp-reprobe-empty-cron"
  description = "Daily re-probe of today's new empty_confirmed / SOURCE_RETURNED_ZERO cells (DP_EMPTY_REPROBE_DISAGREEMENT). Plan: data_pipeline_hardening_self_monitoring_2026_06_22.md Wave-4b."
  schedule    = "0 9 * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.dp_reprobe_empty_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
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
output "dp_audit_job_names" {
  description = "Cloud Run Job names for the data-pipeline self-monitoring audits (for ad-hoc runs: `gcloud run jobs execute`)."
  value = [
    module.dp_daily_digest_job.name,
    module.dp_manifest_hygiene_changed_job.name,
    module.dp_manifest_hygiene_full_job.name,
    module.dp_reprobe_empty_job.name,
  ]
}

output "dp_audit_cron_names" {
  description = "Cloud Scheduler cron names for the data-pipeline self-monitoring audits."
  value = [
    google_cloud_scheduler_job.dp_daily_digest_cron.name,
    google_cloud_scheduler_job.dp_manifest_hygiene_changed_cron.name,
    google_cloud_scheduler_job.dp_manifest_hygiene_full_cron.name,
    google_cloud_scheduler_job.dp_reprobe_empty_cron.name,
  ]
}
