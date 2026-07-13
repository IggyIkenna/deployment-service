# Understat EU (expected_unattempted) typing sweep — daily Cloud Scheduler trigger
#
# Closes the LAG between the daily forward-poll enum (expected_universe_v2_scheduler.tf,
# 01:30 UTC) and the understat blank-reason expected_unattempted residual it leaves behind.
# `enumerate_expected_universe.py`'s per-day source-rule gate is NOT matchday-aware, so it
# re-seeds a blank-reason expected_unattempted row for every understat XG/XG_SHOTS
# (league, date) cell not yet captured — including genuine off-season/no-fixture dates,
# which should be typed EXPECTED_NO_FIXTURE, not left blank. Until now,
# `instruments-service/scripts/type_understat_eu_no_provider_coverage.py` (the matchday-aware
# typing sweep for exactly this residual) existed in the tree but was NEVER wired into any
# scheduler — it only ever ran ad-hoc, which is the entire cause of the ~2-day lag between a
# date going stale and it finally getting typed (root-caused
# plans/active/understat_local_backfill_completion_2026_07_06.md, 2026-07-13 entry).
#
# Schedule: 03:00 UTC daily — comfortably after the 01:30 UTC forward-poll enum's worst-case
# completion (its own timeout is 3600s = 60 min, i.e. by 02:30 at the latest), so the typing
# sweep always sees that morning's freshly-seeded residual rather than racing it.
#
# Reuses the `expected_universe_v2_enum` service account (expected_universe_v2_scheduler.tf)
# — it already holds `roles/storage.objectAdmin` on `instruments-store-sports-prd-*` (the
# exact bucket + read/write scope this script needs: reads the manifest + the FIXTURES
# parquets it builds its truthset from, writes a per-VM shard), so no new SA/IAM-bucket
# wiring is needed; only a NEW `run.invoker` binding on THIS job (Cloud Run Job invoker perms
# are per-job, not bucket-wide — the same gap that silently no-op'd the sibling enum
# scheduler until 2026-06-22, see that file's IAM comment).
#
# CONSOLIDATOR-SAFE: writes ONLY a per-VM shard (`_index/per_vm/type-understat-eu-daily.parquet`,
# stable name — same shard overwritten + merged each day, last-write-wins), never touches the
# canonical blob directly. `--apply` is passed unconditionally; the script's own base_mask/
# type_mask logic makes a no-op run (0 candidates) a safe, cheap early-exit.
#
# Lifecycle note: `type_understat_eu_no_provider_coverage.py` carries its own
# `# Delete-when: ... == 0 for 7 consecutive daily runs` marker — once that holds, BOTH the
# script and this scheduler should be deleted together (the residual will then need the
# writer-side root-cause fix in `enumerate_expected_universe.py` instead, tracked separately).
#
# References:
#   instruments-service/scripts/type_understat_eu_no_provider_coverage.py
#   deployment-service/terraform/gcp/expected_universe_v2_scheduler.tf  (the SA this reuses)
#   plans/active/understat_local_backfill_completion_2026_07_06.md  (2026-07-13 entry)

resource "google_cloud_run_v2_job_iam_member" "understat_eu_typing_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = module.understat_eu_typing_job.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.expected_universe_v2_enum.email}"
}

module "understat_eu_typing_job" {
  source = "../modules/container-job/gcp"

  name                  = "understat-eu-typing-sweep"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.expected_universe_v2_enum.email

  # Same image as the enum job — the script lives at the same
  # /app/instruments-service/scripts/ path.
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = "2"
  memory          = "8Gi" # full sports manifest read (~5M rows) dominates, not the tiny understat residual itself
  timeout_seconds = 600   # 10 min — small candidate set + a bounded fixture-index build (32-way concurrent reads)
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "/app/instruments-service/scripts/type_understat_eu_no_provider_coverage.py",
    "--apply",
  ]

  environment_variables = {
    GCP_PROJECT_ID         = var.project_id
    PROJECT_ID             = var.project_id
    DEPLOYMENT_ENV_SHORT   = "prd"
    CLOUD_PROVIDER         = "gcp"
    MANIFEST_PER_VM_SHARDS = "true"
    VM_NAME                = "type-understat-eu-daily" # stable shard name; consolidator merges last-writer-wins
  }

  service_name = "understat-eu-typing-sweep"
  environment  = var.environment

  labels = {
    "purpose"     = "understat-eu-typing-sweep"
    "asset_group" = "sports"
  }
}

resource "google_cloud_scheduler_job" "understat_eu_typing_daily" {
  project          = var.project_id
  region           = var.region
  name             = "understat-eu-typing-sweep-daily"
  description      = "Type the daily understat XG/XG_SHOTS expected_unattempted residual (no-fixture / out-of-coverage dates) so it doesn't sit blank-reason for ~2 days waiting on an ad-hoc run"
  schedule         = "0 3 * * *" # 03:00 UTC daily — after the 01:30 forward-poll enum's worst-case 60-min completion
  time_zone        = "UTC"
  attempt_deadline = "600s"

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${module.understat_eu_typing_job.name}:run"

    oauth_token {
      service_account_email = google_service_account.expected_universe_v2_enum.email
    }
  }
}

output "understat_eu_typing_job_name" {
  description = "Cloud Run Job name for the understat EU typing sweep (ad-hoc run: `gcloud run jobs execute`)."
  value       = module.understat_eu_typing_job.name
}

output "understat_eu_typing_cron_name" {
  description = "Cloud Scheduler cron name for the daily understat EU typing sweep."
  value       = google_cloud_scheduler_job.understat_eu_typing_daily.name
}
