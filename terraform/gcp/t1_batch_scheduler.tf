# T+1 Batch Reconciliation — Cloud Scheduler triggers
#
# Each batch service runs independently on its own T+1 schedule so recon data
# is ready when batch-live-reconciliation-service starts at 06:00 UTC.
#
#
# Schedule design (pipeline DAG order):
#
# Data availability constraints (verified 2026-04-16):
#   - Tardis (CeFi): ~1.5-6h after UTC midnight (safe at 06:00 UTC)
#   - Databento (TradFi): T+1 — yesterday's data available after midnight UTC
#   - Sports (API Football, Odds API): immediate
#   - DeFi (on-chain, The Graph): immediate
#   - Prediction (Polymarket): immediate
#
# Two phases: FAST (immediate sources) + SLOW (delayed sources)
#
# FAST phase (Sports, DeFi, Prediction, TradFi instruments):
#   00:00 — instruments-service DEFI (per-AG; DeFi on-chain immediate)
#   00:10 — instruments-service TRADFI (per-AG; Databento T+1 OK at midnight)
#   00:20 — instruments-service PREDICTION (per-AG; Polymarket immediate)
#   00:30 — market-tick-data-service SPORTS + DEFI + PREDICTION + TRADFI (all available)
#   00:30 — execution-service config snapshot
#   01:00 — lifecycle-catalogue-regen per AG (after all per-AG instrument producers finish)
#   01:30 — market-data-processing-service (for fast-phase tick data)
#   02:00 — features-calendar, features-delta-one (TradFi), features-volatility
#   02:30 — features-onchain, features-sports, features-cross-instrument,
#            features-multi-timeframe, features-commodity
#   03:00 — ml-service
#   04:00 — strategy-service
#
# SLOW phase (CeFi — Tardis delayed):
#   06:00 — instruments-service CEFI (per-AG; Tardis data now available)
#   06:00 — market-tick-data-service CEFI (Tardis data now available)
#   07:00 — market-data-processing-service CEFI
#   07:30 — features-delta-one CEFI, features-onchain CEFI
#   08:00 — ml-inference CEFI
#
#   09:00 — batch-live-reconciliation-service (after ALL phases complete)
#
# NOTE: The old all-AG "instruments" 00:00 job (uts-prod-instruments-service-t1-recon)
# OOM'd at 8cpu/32Gi (signal 9) because sports alone = 5.6M rows in one execution.
# It has been RETIRED and replaced by per-AG jobs (sports has its own sports-fixtures
# jobs; cefi runs at 06:00). The old all-AG job was temporarily scoped to DEFI as an
# interim — the DEFI per-AG entry below (00:00) supersedes it.

# Service account for T+1 batch Cloud Scheduler jobs — must exist before scheduler jobs are created
resource "google_service_account" "t1_batch" {
  account_id   = "${local.env_prefix}-batch-sa"
  display_name = "T+1 Batch Scheduler SA (${var.environment})"
  description  = "Service account used by Cloud Scheduler to trigger T+1 batch Cloud Run Jobs"
  project      = var.project_id
}

resource "google_project_iam_member" "t1_batch_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.t1_batch.email}"
}

locals {
  # Cloud Run Job name convention: {env_prefix}-{service}-t1-recon
  # Cloud Scheduler job name convention: {env_prefix}-{service}-t1-schedule
  t1_service_account_email = google_service_account.t1_batch.email

  t1_batch_services = {
    # Per-AG instruments-service producers (FAST phase: 00:00-00:20, staggered 10min)
    # REPLACES the retired all-AG "instruments" 00:00 job (OOM'd at 8cpu/32Gi — signal 9).
    # Sports has its own sports-fixtures jobs; CeFi runs at 06:00 (Tardis lag).
    "instruments-defi" = {
      schedule    = "0 0 * * *"
      job_name    = "${local.env_prefix}-instruments-service-defi-t1-recon"
      description = "instruments-service DEFI T+1 — DeFi on-chain instrument definitions (immediate source; 4cpu/8Gi)"
    }
    "instruments-tradfi" = {
      schedule    = "10 0 * * *"
      job_name    = "${local.env_prefix}-instruments-service-tradfi-t1-recon"
      description = "instruments-service TRADFI T+1 — TradFi Databento instrument definitions (T+1 available at midnight; 4cpu/8Gi)"
    }
    "instruments-prediction" = {
      schedule    = "20 0 * * *"
      job_name    = "${local.env_prefix}-instruments-service-prediction-t1-recon"
      description = "instruments-service PREDICTION T+1 — Polymarket prediction market instrument definitions (immediate source; 2cpu/4Gi)"
    }
    "instruments-cefi" = {
      schedule    = "0 6 * * *"
      job_name    = "${local.env_prefix}-instruments-service-cefi-t1-recon"
      description = "instruments-service CeFi T+1 — delayed run after Tardis data available (~6h lag)"
    }
    "sports-fixtures-6am" = {
      schedule    = "0 6 * * *"
      job_name    = "${local.env_prefix}-instruments-service-sports-fixtures"
      description = "Sports future fixtures refresh — fetches fixture calendar for next 14 days (6am UTC)"
    }
    "sports-fixtures-noon" = {
      schedule    = "0 12 * * *"
      job_name    = "${local.env_prefix}-instruments-service-sports-fixtures"
      description = "Sports future fixtures refresh — noon UTC"
    }
    "sports-fixtures-6pm" = {
      schedule    = "0 18 * * *"
      job_name    = "${local.env_prefix}-instruments-service-sports-fixtures"
      description = "Sports future fixtures refresh — 6pm UTC"
    }
    "sports-fixtures-midnight" = {
      schedule    = "0 0 * * *"
      job_name    = "${local.env_prefix}-instruments-service-sports-fixtures"
      description = "Sports future fixtures refresh — midnight UTC"
    }
    "market-tick-data-fast" = {
      schedule    = "30 0 * * *"
      job_name    = "${local.env_prefix}-market-tick-data-service-fast-t1-recon"
      description = "MTDS T+1 FAST — Sports odds, DeFi on-chain, Prediction, TradFi (Databento T+1 available at midnight)"
    }
    "market-tick-data-cefi" = {
      schedule    = "0 6 * * *"
      job_name    = "${local.env_prefix}-market-tick-data-service-cefi-t1-recon"
      description = "MTDS T+1 CEFI — Tardis tick data (available ~6h after midnight)"
    }
    "execution-config-snapshot" = {
      schedule    = "30 0 * * *"
      job_name    = "${local.env_prefix}-execution-service-config-snapshot"
      description = "execution-service EOD config snapshot — prerequisite for Stage 3 recon"
    }
    "market-data-processing" = {
      schedule    = "0 1 * * *"
      job_name    = "${local.env_prefix}-market-data-processing-service-t1-recon"
      description = "market-data-processing-service T+1 recon batch — candle aggregation (depends on MTDS)"
    }
    "features-calendar" = {
      schedule    = "30 1 * * *"
      job_name    = "${local.env_prefix}-features-calendar-service-t1-recon"
      description = "features-calendar-service T+1 recon batch — writes to t1-recon/features/calendar/"
    }
    "features-delta-one" = {
      schedule    = "0 2 * * *"
      job_name    = "${local.env_prefix}-features-delta-one-service-t1-recon"
      description = "features-delta-one-service T+1 recon batch — writes to t1-recon/features/delta-one/"
    }
    "features-volatility" = {
      schedule    = "0 2 * * *"
      job_name    = "${local.env_prefix}-features-volatility-service-t1-recon"
      description = "features-volatility-service T+1 recon batch — writes to t1-recon/features/volatility/"
    }
    "features-onchain" = {
      schedule    = "30 2 * * *"
      job_name    = "${local.env_prefix}-features-onchain-service-t1-recon"
      description = "features-onchain-service T+1 recon batch — writes to t1-recon/features/onchain/"
    }
    "features-sports" = {
      schedule    = "30 2 * * *"
      job_name    = "${local.env_prefix}-features-sports-service-t1-recon"
      description = "features-sports-service T+1 recon batch — writes to t1-recon/features/sports/"
    }
    "features-cross-instrument" = {
      schedule    = "30 2 * * *"
      job_name    = "${local.env_prefix}-features-cross-instrument-service-t1-recon"
      description = "features-cross-instrument-service T+1 recon batch — writes to t1-recon/features/cross-instrument/"
    }
    "features-multi-timeframe" = {
      schedule    = "30 2 * * *"
      job_name    = "${local.env_prefix}-features-multi-timeframe-service-t1-recon"
      description = "features-multi-timeframe-service T+1 recon batch — writes to t1-recon/features/multi-timeframe/"
    }
    "features-commodity" = {
      schedule    = "30 2 * * *"
      job_name    = "${local.env_prefix}-features-commodity-service-t1-recon"
      description = "features-commodity-service T+1 recon batch — writes to t1-recon/features/commodity/"
    }
    "ml" = {
      schedule    = "0 3 * * *"
      job_name    = "${local.env_prefix}-ml-service-t1-recon"
      description = "ml-service T+1 recon batch — reads t1-recon/features/, writes to t1-recon/ml/"
    }
    "strategy" = {
      schedule    = "0 4 * * *"
      job_name    = "${local.env_prefix}-strategy-service-t1-recon"
      description = "strategy-service T+1 recon batch — reads t1-recon/ml/, writes to t1-recon/strategy/"
    }
    "batch-live-reconciliation" = {
      schedule    = "0 6 * * *"
      job_name    = "${local.env_prefix}-batch-live-reconciliation-service"
      description = "batch-live-reconciliation-service — T+1 orchestrator; polls GCS for upstream data availability"
    }
  }
}

resource "google_cloud_scheduler_job" "t1_batch_schedule" {
  for_each = local.t1_batch_services

  name        = "${local.env_prefix}-${each.key}-t1-schedule"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${each.value.job_name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  retry_config {
    retry_count          = 1
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }

}
