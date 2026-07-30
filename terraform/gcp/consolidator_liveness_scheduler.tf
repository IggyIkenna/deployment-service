# Manifest Consolidator Liveness Watchdog — Cloud Run Job + Scheduler cron
#
# Plan: manifest_consolidator_liveness_health_2026_06_01
# Code: unified-trading-library@3732ffaa
#   (`unified_trading_library.monitors.consolidator_liveness`)
#
# Background
#   The manifest consolidator (manifest_consolidator_scheduler.tf) heartbeats
#   every cycle, including no-op cycles — it touches the canonical
#   `_index/availability_index.parquet` mtime + emits MANIFEST_CONSOLIDATED.
#   Nothing watched for the heartbeat's ABSENCE. This watchdog does: per
#   bucket it reads the heartbeat age and emits CONSOLIDATOR_DOWN (ERROR) when
#   a bucket misses > N consolidation cycles (default 5 × 60s = 300s),
#   CONSOLIDATOR_RECOVERED when it returns. The CLI exits non-zero if any
#   bucket is DOWN, so the Cloud Run execution surfaces red as a second signal
#   alongside the alert event.
#
# Topology
#   A SINGLE job (not one-per-bucket like the consolidator) — the watchdog CLI
#   accepts every bucket in one `--buckets a,b,c` arg, and each check is a
#   cheap blob-metadata read. One cron, `*/2 * * * *` (half the consolidator
#   cadence — liveness doesn't need per-minute granularity).
#
# Image
#   Reuses `market-tick-data-service:latest` (UTL bundled as a dep). Requires
#   the image to be rebuilt at >= UTL 3732ffaa so the consolidator_liveness
#   module is present.
#
# Service account
#   * Scheduler invoker: `t1_batch_sa` (roles/run.invoker).
#   * Container runtime: `unified_trading_sa` (storage read on all manifest
#     buckets — needs blob.updated + list_blobs to compute heartbeat age).

locals {
  # Every bucket the consolidator covers (env-tiered + legacy + extended
  # Group B) is also watched for liveness. Dedup since legacy/env-tiered maps
  # can overlap conceptually; distinct() keeps the --buckets arg minimal.
  consolidator_liveness_buckets = distinct(concat(
    values(local.manifest_consolidator_buckets),
    values(local.manifest_consolidator_buckets_extended),
  ))

  # Fast/slow tier split (manifest_consolidator_cadence_cost_audit_2026_07_20.md, operator
  # RULED 2026-07-29 "proceed" — item 2's "matching liveness-watchdog --cycle-sec/
  # --cycles-grace update (fast-tier/slow-tier split)"). The watchdog CLI
  # (unified_trading_library/monitors/consolidator_liveness.py) applies ONE shared
  # cycle_sec/cycles_grace to EVERY bucket passed to a single --buckets invocation — so
  # slowing a bucket's own cron to hourly (manifest_consolidator_schedule above) without a
  # matching watchdog change would false-page CONSOLIDATOR_DOWN ~5min into every hourly
  # gap. Splitting into two Cloud Run Job + Cloud Scheduler pairs instead of one job fixes
  # this: "fast" keeps the unchanged 60s/5-cycle (300s) threshold for buckets still on
  # */1; "slow" uses a threshold sized for the new hourly cadence for the 12 widened
  # categories (same category set as manifest_consolidator_schedule, by construction).
  consolidator_liveness_slow_tier_buckets = [
    for cat in keys(local.manifest_consolidator_schedule) :
    lookup(local.manifest_consolidator_buckets, cat, lookup(local.manifest_consolidator_buckets_extended, cat, null))
  ]
  consolidator_liveness_fast_tier_buckets = [
    for b in local.consolidator_liveness_buckets : b
    if !contains(local.consolidator_liveness_slow_tier_buckets, b)
  ]

  consolidator_liveness_tiers = {
    # Unchanged from the original single-job behavior for every bucket that stayed on
    # */1 (the 4 live market-data buckets + instruments-sports/features-sports).
    fast = {
      buckets      = local.consolidator_liveness_fast_tier_buckets
      cycle_sec    = 60
      cycles_grace = 5 # 300s DOWN threshold — same as before the split
      schedule     = "*/2 * * * *"
    }
    # 3600s cycle x 2 cycles-grace = 7200s (2h) DOWN threshold — comfortably above the
    # new hourly cadence (2x, matching the "TTL/threshold > real cadence with margin"
    # pattern this file's sibling manifest_consolidator_lock_ttl_seconds already uses)
    # while still catching a genuine multi-hour outage promptly, not masking it the way
    # cefi's 86400s staleness budget alone would if this were the only signal.
    slow = {
      buckets      = local.consolidator_liveness_slow_tier_buckets
      cycle_sec    = 3600
      cycles_grace = 2
      # :15 and :45 past the hour — always >=15min after the hourly consolidator cron's
      # :00 fire, so a healthy bucket's heartbeat has had time to land before each check.
      schedule = "15,45 * * * *"
    }
  }
}

module "consolidator_liveness_job" {
  for_each = local.consolidator_liveness_tiers
  source   = "../modules/container-job/gcp"

  name                  = "${local.env_prefix}-consolidator-liveness-watchdog-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.unified_trading.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/market-tick-data-service:latest"

  # Lightweight — only blob-metadata reads + a list-blobs existence check per
  # bucket. No data download, no merge.
  cpu             = "1"
  memory          = "2Gi"
  timeout_seconds = 300
  max_retries     = 0 # exit-1-on-DOWN is the intended signal; next cron cycle re-checks
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "-m", "unified_trading_library.monitors.consolidator_liveness",
    "--buckets", join(",", each.value.buckets),
    "--cycle-sec", tostring(each.value.cycle_sec),
    "--cycles-grace", tostring(each.value.cycles_grace),
  ]

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
  }

  service_name = "consolidator-liveness-watchdog"
  environment  = var.environment

  labels = {
    "purpose" = "consolidator-liveness-watchdog"
    "tier"    = each.key
  }
}

resource "google_cloud_scheduler_job" "consolidator_liveness_cron" {
  for_each = local.consolidator_liveness_tiers

  name        = "${local.env_prefix}-consolidator-liveness-watchdog-${each.key}-cron"
  description = "Watch manifest consolidator heartbeats (${each.key} tier, ${length(each.value.buckets)} bucket(s)); emit CONSOLIDATOR_DOWN on missed cycles."
  schedule    = each.value.schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.consolidator_liveness_job[each.key].name}:run"

    oauth_token {
      service_account_email = local.t1_service_account_email
    }
  }

  # No retries — the next cycle re-checks (the watchdog is stateless per run).
  retry_config {
    retry_count          = 0
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }
}

output "consolidator_liveness_job_name" {
  description = "Cloud Run Job name per tier for the consolidator liveness watchdog (for manual ad-hoc fires)."
  value       = { for tier, mod in module.consolidator_liveness_job : tier => mod.name }
}

output "consolidator_liveness_cron_name" {
  description = "Cloud Scheduler cron name per tier for the consolidator liveness watchdog."
  value       = { for tier, job in google_cloud_scheduler_job.consolidator_liveness_cron : tier => job.name }
}

# -------------------------------------------------------
# Cloud Monitoring log-based alert — pages on this watchdog's OWN exit(1)
# -------------------------------------------------------
# Issue: plans/active/issues/defi_consolidator_cron_left_paused_2026_07_15.md
#   The defi consolidator's triggering Cloud Scheduler cron was left PAUSED
#   for 13.5h (2026-07-14 22:25Z -> 2026-07-15 11:56Z). This watchdog job
#   correctly logged "N bucket(s) DOWN ... Container called exit(1)" every
#   2-min cycle for 12+ consecutive hours, and its app-level path
#   (log_event(CONSOLIDATOR_DOWN) -> lifecycle-events Pub/Sub ->
#   alerting-service's CONSOLIDATOR_DOWN rule -> PagerDuty/Telegram) is coded
#   and DID fire per the incident's own logs -- but nothing paged a human, and
#   there was ALSO zero GCP-native monitoring on this job's own exit code (no
#   `google_monitoring_alert_policy` anywhere in terraform/gcp/ keyed off
#   resource.type="cloud_run_job" for this job). So the ONE thing watching the
#   consolidator (this watchdog) had no independent watcher of its own -- a
#   violation of the "each layer is independent of the one it watches" design
#   in codex/05-infrastructure/deployment-observability.md § "Out-of-band
#   liveness". This alert closes that specific gap: a GCP-native backstop that
#   does not depend on the app's own Pub/Sub/alerting-service pipeline at all,
#   so a break anywhere in THAT pipeline (the still-open, unverified half of
#   this issue's P1 todo) can no longer leave this watchdog's own failures
#   silent.
#
# Channel reuse: the same out-of-band bedrock as the other real (non-toothless)
# alerts in this file's directory --
# `google_monitoring_notification_channel.monitoring_deadman_email`
# (monitoring_deadman_scheduler.tf) -- deliberately NOT a new channel, and
# deliberately NOT the empty-default `var.cf_audit_alert_notification_channels`
# pattern in cf_manifest_audit_scheduler.tf (that pattern ships an alert policy
# that pages no one until a separate workspace-level wiring step happens; this
# alert must actually page from day one).
resource "google_monitoring_alert_policy" "consolidator_liveness_watchdog_failed" {
  display_name = "${local.env_prefix} consolidator-liveness-watchdog — exit(1) (bucket DOWN)"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true

  # ONE condition covering BOTH tiers (fast/slow, see the for_each split above) via an
  # OR'd job_name filter — GCP rejects multiple condition_matched_log conditions on a
  # single policy ("Alert policies with a log matching condition can only have a single
  # condition", confirmed live 2026-07-30), so a dynamic per-tier `conditions` block
  # (the naive split) is NOT viable here; a single condition whose filter ORs every
  # tier's job name is the supported equivalent — still pages on EITHER tier's ERROR,
  # identical semantics to the pre-split single-job policy.
  conditions {
    display_name = "consolidator-liveness-watchdog (any tier) logged ERROR (bucket DOWN or execution failed)"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_run_job"
        resource.labels.job_name=(${join(" OR ", [for mod in module.consolidator_liveness_job : "\"${mod.name}\""])})
        severity="ERROR"
      EOT

      label_extractors = {
        "job_name" = "EXTRACT(resource.labels.job_name)"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.monitoring_deadman_email.id]

  alert_strategy {
    notification_rate_limit {
      # The watchdog cron is */2 min; while a bucket stays DOWN it re-logs
      # ERROR every cycle. Page promptly on the first occurrence, then at
      # most once/hour while the condition persists (not every 2 min).
      period = "3600s"
    }
    auto_close = "604800s" # 7 days — auto-close stale incidents
  }

  documentation {
    content   = "The consolidator liveness watchdog (uts-prod-consolidator-liveness-watchdog) logged an ERROR -- either a manifest bucket's consolidator heartbeat is stale (CONSOLIDATOR_DOWN) or the watchdog's own Cloud Run execution failed. Check `gcloud scheduler jobs describe uts-prod-manifest-consolidator-market-data-{bucket}-cron --location=asia-northeast1` for a PAUSED cron first (the 2026-07-15 root cause -- a paused scheduler never self-recovers) before assuming a transient consolidator error. Then `gcloud run jobs executions list --job=uts-prod-consolidator-liveness-watchdog --region=asia-northeast1 --limit=5` for the watchdog's own recent runs. SSOTs: codex/05-infrastructure/manifest-consolidator-ssot.md, plans/active/issues/defi_consolidator_cron_left_paused_2026_07_15.md."
    mime_type = "text/markdown"
  }

  user_labels = {
    "purpose" = "consolidator-liveness-watchdog-alert"
  }
}
