# mdps_odds_horizon_bucket daily driver — Cloud Scheduler + Cloud Run Job
#
# ROOT CAUSE (sports-scheduling audit, 2026-07-14,
# unified-trading-pm/plans/active/sports_data_sources_canonical_completion_2026_07_13.md
# "SCHEDULED CAPTURE AUDIT" entry): market-data-processing-service's derived
# odds-horizon-bucket reprocessing of raw odds_api ticks
# (scripts/reprocess_sports_odds.py, source=mdps_odds_horizon_bucket in the
# canonical sports manifest) has NEVER had a recurring scheduled driver
# anywhere in the fleet — its only production path has been manual one-off VM
# launches (scripts/vm/launch-mdps-sports-bucket-vm.sh). The pre-existing
# uts-prod-market-data-processing-t1-schedule scheduler (t1_batch_scheduler.tf)
# was investigated as a possible existing driver for this gap and ruled out:
# its description ("candle aggregation (depends on MTDS)") and target Cloud
# Run Job's intended args (general --CEFI/--TRADFI/--DEFI/--SPORTS/--PREDICTION
# candle processing, per the orphaned terraform/services/market-data-
# processing-service/gcp/main.tf workflow.yaml) show it was ALWAYS the
# general MDPS T+1 candle-aggregation job, never sports-odds-specific — that
# job's OWN missing-CRJ bug is fixed separately in audit03_cron_provisioning.tf
# (mdps_t1_recon_job). This file provisions the genuinely-new, dedicated
# odds-horizon-bucket driver the audit actually calls for.
#
# Script fixes shipped same-day and confirmed working in prod via the ongoing
# manual VM backfill fleet (deployment-service@0e7d771 SPOT-provisioning fix):
#   - market-data-processing-service@e8f6709 — raw-input prefix-template fix
#   - market-data-processing-service@7c5c74d — FetchEvidence-on-empty-day fix
#     (previously a 100%-crash UnprovenHonestAbsenceError on any genuinely-
#     empty day in the range)
#   - market-data-processing-service@<pending> (this change) — --start-date/
#     --end-date made optional, defaulting to a rolling [today-2, today] (UTC)
#     window when both omitted. A google_cloud_scheduler_job http_target's
#     args are static (no per-trigger date templating, unlike the orphaned
#     Workflow's compute_date step), so the date window must be computed
#     INSIDE the script, not injected by Terraform.
#
# Window choice: 3-day rolling ([today-2, today]) rather than a single day —
# absorbs bookmakers' late-arriving raw ODDS_API ticks landing a day or two
# after the fixture date (t1_batch_scheduler.tf's header classifies sports odds
# as an "immediate" source, but immediate != guaranteed-same-day for every
# bookmaker feed). Cheap to re-run daily: the script's ManifestWriter pre-flight
# (row_key lookup) skips any day already `captured`/`empty_confirmed` unless
# --force, so only genuinely-new or previously-failed days do real work.
#
# Schedule: 01:15 UTC daily — after the 00:30 MTDS fast-phase job (raw ODDS_API
# ticks land) and the 01:00 general market-data-processing T+1 job, comfortably
# inside the FAST-phase window documented at the top of t1_batch_scheduler.tf.
#
# Service accounts: reuses BOTH existing accounts, no new IAM needed —
#   - Image SA: unified-trading-sa (google_service_account.unified_trading,
#     main.tf) — the SAME identity the general market-data-processing-service
#     t1-recon job (audit03_cron_provisioning.tf) uses; that job already reads
#     the market-data-tick-sports-* and instruments-store-sports-* buckets this
#     script needs (raw ticks + bucketed output + manifest), so no new bucket
#     IAM is required.
#   - Scheduler invoker SA: local.t1_service_account_email (t1_batch,
#     t1_batch_scheduler.tf) — already carries a PROJECT-WIDE roles/run.invoker
#     binding (google_project_iam_member.t1_batch_run_invoker), matching the
#     sports_enrichment_provider_scheduler.tf precedent exactly.
#
# Sizing: reprocess_sports_odds.py's ManifestWriter pre-flight reads the full
# canonical sports manifest (~5-6M rows) before any per-day work — the same
# read cost class that OOM'd the sibling soccer_football_info enrichment job at
# 2cpu/8Gi (sports_enrichment_provider_scheduler.tf comment). Sized to that
# job's PROVEN-safe allocation (8cpu/32Gi) from the start to avoid burning a
# live-test iteration on a foreseeable OOM.

module "mdps_odds_horizon_job" {
  source = "../modules/container-job/gcp"

  name                  = "uts-prod-mdps-odds-horizon-bucket"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = local.mdps_t1_image

  cpu             = "8"
  memory          = "32Gi"
  timeout_seconds = 3600 # 1h — 3-day rolling window with manifest pre-flight skip on already-captured days
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  # No `command` override — the market-data-processing-service image
  # ENTRYPOINT is `python -m market_data_processing_service` (candle CLI), so
  # this job's `command` points `python` at the standalone reprocessor script
  # instead (matches the understat-eu-typing-sweep pattern: command=["python"],
  # args=[<script path>, ...]). No --start-date/--end-date passed — the script
  # self-defaults to the rolling [today-2, today] window (see file header).
  command = ["python"]
  args = [
    "scripts/reprocess_sports_odds.py",
    "--workers", "8",
  ]

  environment_variables = {
    GCP_PROJECT_ID   = var.project_id
    DEPLOYMENT_ENV   = var.environment
    GCS_LOCATION     = var.region
    PYTHONUNBUFFERED = "1"
  }

  service_name = "market-data-processing-service"
  environment  = var.environment

  labels = {
    "purpose"     = "mdps-odds-horizon-bucket"
    "asset_group" = "sports"
    "finding"     = "no-scheduled-driver-2026-07-14"
  }
}

resource "google_cloud_scheduler_job" "mdps_odds_horizon_daily" {
  name        = "${local.env_prefix}-mdps-odds-horizon-bucket-daily"
  description = "MDPS sports odds_horizon_bucket daily reprocessing — rolling 3-day window (no prior scheduled driver; see file header)."
  schedule    = "15 1 * * *" # 01:15 UTC daily
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.mdps_odds_horizon_job.name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    max_retry_duration   = "300s"
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 2
  }
}

output "mdps_odds_horizon_job_name" {
  description = "Cloud Run Job name for the daily MDPS odds_horizon_bucket reprocessor (ad-hoc run: `gcloud run jobs execute <name> --wait`)."
  value       = module.mdps_odds_horizon_job.name
}

output "mdps_odds_horizon_cron_name" {
  description = "Cloud Scheduler cron name for the daily MDPS odds_horizon_bucket reprocessor."
  value       = google_cloud_scheduler_job.mdps_odds_horizon_daily.name
}
