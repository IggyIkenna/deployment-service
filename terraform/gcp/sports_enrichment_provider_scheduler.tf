# Sports enrichment-provider daily schedulers — footystats / transfermarkt /
# soccer_football_info (SFI)
#
# ROOT CAUSE (audit 2026-07-14, unified-trading-pm/plans/active/
# sports_data_sources_canonical_completion_2026_07_13.md "SCHEDULED CAPTURE
# AUDIT" entry): these 3 sources' enrichment data (footystats MATCHES/ODDS/
# PREDICTIONS, transfermarkt PLAYER_VALUES, soccer_football_info
# SFI_PROGRESSIVE_STATS) had ZERO scheduled driver anywhere in the fleet.
#
#   - The 4x-daily ``uts-prod-sports-fixtures-{midnight,6am,noon,6pm}-t1-
#     schedule`` crons invoke ``uts-prod-instruments-service-sports-fixtures``
#     with baked args ``--sports-provider=API_FOOTBALL`` — this NARROWS
#     ``active_venues`` to API_FOOTBALL only (``_apply_sports_provider_filter``,
#     instruments-service ``process_preflight.py:255``), so the footystats/
#     transfermarkt/SFI enrichment blocks in ``process_enrichment.py`` never
#     run in this invocation (their ``"X" in active_venues_set`` guards are
#     False).
#   - The 5-min ``uts-prod-sports-scheduler-cron`` (``sports_trigger_scheduler``
#     + ``configs/sports-trigger-tiers.yaml``) never names MATCHES / ODDS
#     (footystats) / PLAYER_VALUES / SFI_PROGRESSIVE_STATS as a
#     ``--sports-entity`` in ANY tier — grep of the live YAML confirms zero
#     occurrences.
#   - Verified empirically: ``gcloud logging read`` over the preceding 14 days
#     shows ZERO ``Sports provider filter from CLI: FOOTYSTATS|TRANSFERMARKT|
#     SOCCER_FOOTBALL_INFO`` invocations of the Cloud Run Job — every "fresh"
#     capture seen in the manifest for these 3 sources was from manual one-off
#     backfill/residual-closer scripts run by agents, not sustained automation.
#   - Live-verified the fix direction: manually executing
#     ``uts-prod-instruments-service-sports-fixtures`` with
#     ``--sports-provider=FOOTYSTATS --start-date=2026-07-14 --end-date=2026-07-14``
#     (execution ``uts-prod-instruments-service-sports-fixtures-lx6sw``) hit the
#     ``FOOTYSTATS short-circuit`` path end-to-end and wrote real captured rows
#     into the CANONICAL ``instruments-store-sports-prd-central-element-323112``
#     manifest (same bucket the batch/backfill path uses — confirmed via the
#     execution's own ``ManifestWriter: updated availability index ...`` log
#     line). TRANSFERMARKT / SOCCER_FOOTBALL_INFO share the IDENTICAL
#     ``_sports_provider_short_circuit`` code path (process_preflight.py) —
#     not independently live-tested in this pass to avoid piling more
#     concurrent load onto an already-contended manifest during this same
#     audit session, but code-symmetric with the verified FOOTYSTATS case.
#
# No NEW Cloud Run Job image/deploy pipeline needed — reuses the EXACT same
# ``instruments-service:latest`` image + ``unified-trading-sa`` runtime
# service account as the existing (working) ``uts-prod-instruments-service-
# sports-fixtures`` job (confirmed via ``gcloud run jobs describe``: that SA
# already successfully resolves all 5 sports data-source API keys, incl.
# footystats/transfermarkt/soccer_football_info — no new IAM needed). The
# Scheduler invoker identity reuses ``local.t1_service_account_email``
# (``t1_batch``), which already carries a PROJECT-WIDE ``roles/run.invoker``
# binding (``google_project_iam_member.t1_batch_run_invoker`` in
# t1_batch_scheduler.tf) — no new per-job IAM binding needed either.
#
# Sizing: these are enrichment SHORT-CIRCUITS (skip the URDI/fixture
# orchestrator entirely, per process_preflight.py's ``_ENRICHMENT_PROVIDERS``
# short-circuit) — much lighter than the full API_FOOTBALL fixtures job
# (8cpu/32Gi). Sized like the sibling ``understat-eu-typing-sweep`` job
# (2cpu/8Gi — dominated by the ~5M-row manifest preflight read, not the
# enrichment fetch itself).
#
# Schedule: staggered 5 min apart, in the 00:35-00:45 UTC FAST-phase window
# (after the 00:30 MTDS/execution-config jobs, before 01:00 market-data-
# processing / lifecycle-catalogue-regen) — mirrors the T+1 batch DAG
# documented at the top of t1_batch_scheduler.tf.

locals {
  sports_enrichment_provider_service_account = "unified-trading-sa@${var.project_id}.iam.gserviceaccount.com"
  sports_enrichment_image                    = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  sports_enrichment_providers = {
    "footystats" = {
      provider    = "FOOTYSTATS"
      schedule    = "35 0 * * *"
      description = "FootyStats daily enrichment — MATCHES/PREDICTIONS/ODDS (no prior scheduled driver; see file header)."
    }
    "transfermarkt" = {
      provider    = "TRANSFERMARKT"
      schedule    = "40 0 * * *"
      description = "Transfermarkt daily enrichment — PLAYER_VALUES (no prior scheduled driver; see file header)."
    }
    "soccer-football-info" = {
      provider    = "SOCCER_FOOTBALL_INFO"
      schedule    = "45 0 * * *"
      description = "soccer_football_info daily enrichment — SFI_PROGRESSIVE_STATS (no prior scheduled driver; see file header)."
    }
  }
}

module "sports_enrichment_provider_job" {
  for_each = local.sports_enrichment_providers
  source   = "../modules/container-job/gcp"

  name                  = "uts-prod-sports-enrichment-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = local.sports_enrichment_provider_service_account

  image = local.sports_enrichment_image

  cpu             = "2"
  memory          = "8Gi"
  timeout_seconds = 1800 # 30 min — single-provider short-circuit fetch, not the full orchestrator
  max_retries     = 0
  parallelism     = 1
  task_count      = 1

  # No `command` override — the instruments-service image ENTRYPOINT already
  # handles `python -m instruments_service` (matches the sibling
  # uts-prod-instruments-service-sports-fixtures job: `command: None`).
  args = [
    "--operation=instruments",
    "--mode=batch",
    "--asset-group=SPORTS",
    "--sports-provider=${each.value.provider}",
    "--run-tag=t1-recon",
  ]

  environment_variables = {
    DEPLOYMENT_ENV   = "prod"
    GCP_PROJECT_ID   = var.project_id
    GCS_LOCATION     = var.region
    PYTHONUNBUFFERED = "1"
  }

  service_name = "instruments-service"
  environment  = var.environment

  labels = {
    "purpose"     = "sports-enrichment-${each.key}"
    "asset_group" = "sports"
  }
}

resource "google_cloud_scheduler_job" "sports_enrichment_provider_daily" {
  for_each = local.sports_enrichment_providers

  name        = "${local.env_prefix}-sports-enrichment-${each.key}-daily"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.sports_enrichment_provider_job[each.key].name}:run"

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

output "sports_enrichment_provider_job_names" {
  description = "Cloud Run Job names for the daily sports enrichment-provider drivers (ad-hoc run: `gcloud run jobs execute <name>`)."
  value       = { for k, m in module.sports_enrichment_provider_job : k => m.name }
}

output "sports_enrichment_provider_cron_names" {
  description = "Cloud Scheduler cron names for the daily sports enrichment-provider drivers."
  value       = { for k, j in google_cloud_scheduler_job.sports_enrichment_provider_daily : k => j.name }
}
