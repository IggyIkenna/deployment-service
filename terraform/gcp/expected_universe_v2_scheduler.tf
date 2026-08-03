# Expected-universe v2 enumerator — daily Cloud Scheduler trigger (per asset_group)
#
# Closes the WRITER gap for the manifest 4th state `expected_unattempted` (codex
# `02-data/availability-manifest-and-data-status.md` § F4). The lifecycle catalogue
# is rolled up by the SIBLING `lifecycle_catalogue_scheduler.tf` (01:00 UTC →
# gs://{instruments-store-<ag>}/{DEPLOYMENT_ENV}/catalog.parquet); THIS scheduler is
# the missing second hop — it runs `instruments-service/scripts/enumerate_expected_universe.py
# --enumerator-version v2 --apply-write`, which cross-joins that catalogue ×
# dates × data_types − the existing manifest → seeds `expected_unattempted`
# per-instrument-grain rows so the coverage denominator
#   % = captured / (captured + empty_confirmed + attempted_failed + expected_unattempted)
# reflects the IS-listed could-exist universe instead of under-counting it.
#
# ROOT CAUSE this closes (audit 2026-06-19): the v2 enumerator + its catalogue
# builder existed in code but the apply-write hop was NEVER cron-wired (the
# `launch-expected-universe-v2-vm.sh` runbook was one-shot, `last_executed: NEVER`)
# → 0 `expected_unattempted` rows materialised in EVERY IS + MTDS `_index`
# fleet-wide. This file makes the materialisation recurring + self-sustaining.
#
# PER-VM SHARD ISOLATION (codex `05-infrastructure/manifest-consolidator-ssot.md`):
# the enumerator writes a per-VM shard `_index/per_vm/{VM_NAME}.parquet` (NOT the
# consolidated `_index` directly) — the manifest consolidator merges it
# last-writer-wins per shard. So this job is SAFE to run concurrently with the
# capture-writing backfill VMs (they write DIFFERENT per-VM shards) and re-running
# it overwrites only its own stable per-AG shard. `MANIFEST_PER_VM_SHARDS=true` +
# `VM_NAME=enum-universe-v2-<ag>` are passed (the enumerator hard-requires both for
# `--apply-write`).
#
# WINDOW: `--start-date` is a bounded recent window (120 days back) so the LIVE
# denominator is honest without the full-history per-instrument blow-up (~190M rows
# fleet-wide — the unbounded 2018→today v2 universe; tracked as a gated follow-up,
# NOT materialised by this recurring job). The window is `local.expected_universe_start_date_rolling`
# — `today - 120d` recomputed at every `terraform plan`/`apply` (fixed 2026-08-03,
# plans/active/issues/sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists_2026_07_27.md
# — the prior static "2026-02-20" literal default never advanced without a fresh
# apply, so it silently stopped seeding `expected_unattempted` for any date before
# it). `var.expected_universe_start_date` stays available as an explicit override to
# pin the window if ever needed.
#
# AG-PARAMETRIC: one Cloud Run Job + Scheduler per asset_group (for_each) so a
# per-AG failure/retry is isolated (every AG GREEN independently, data-pipeline
# correctness rule). build/enumerate are already AG-parametric.
#
# Trigger time: 01:30 UTC daily — AFTER the 01:00 UTC lifecycle-catalogue-regen
# (writes the catalog.parquet this reads) and BEFORE the 02:00 UTC downstream
# catalogue-regen consumers.
#
# NOT fire-and-forget: each AG is a bounded Cloud Run Job (STARTED/STOPPED/FAILED
# + ENUMERATOR_COMPLETED event) with scheduler retry_config; post-apply verify each
# job RAN (T+10min: `gcloud run jobs executions list --job
# expected-universe-v2-<ag>` = Succeeded) before the gate is GREEN.
#
# ✅ IDLE-BUCKET CONSOLIDATOR TRAP — FIXED at the UTL SSOT (2026-06-19, issue option 1;
# issue `plans/active/issues/consolidator_idle_bucket_incremental_trap_2026_06_19.md`
# RESOLVED). The `*/1` manifest consolidator's incremental cutoff used to read the
# canonical's FRESHNESS mtime, which the idle `_touch` advances every cycle — so on a
# bucket with NO concurrent capture writes at 01:30, the just-written enumerator shard
# could be `_touch`-advanced past + pruned as "settled" BEFORE it merged (observed
# 2026-06-19: defi/sports merged within 1 cycle; cefi/tradfi idle → never merged,
# needed a manual `consolidate(bucket, force=True)`). The fix splits the markers: the
# incremental cutoff + prune now read a dedicated `consolidator_content_write_at`
# marker stamped ONLY by a real merge (never by the idle `_touch`), so an idle touch
# can no longer move the cutoff past an unmerged shard. This scheduler is therefore
# self-sustaining on idle AGs — NO companion force-consolidate job is needed. SSOT:
# `codex/05-infrastructure/manifest-consolidator-ssot.md` § "Incremental cutoff =
# LAST-CONTENT-WRITE marker".
#
# References:
#   codex/02-data/availability-manifest-and-data-status.md § "expected_unattempted (F4)"
#   instruments-service/scripts/enumerate_expected_universe.py
#   deployment-service/terraform/gcp/lifecycle_catalogue_scheduler.tf  (the catalogue half)

variable "expected_universe_start_date" {
  description = "Explicit override (YYYY-MM-DD) to pin the v2 expected_unattempted enumeration window start. Leave null (default) to use the genuinely rolling today-120d window computed in local.expected_universe_start_date_rolling."
  type        = string
  default     = null
}

locals {
  # Genuinely rolling `today - 120d`, recomputed at every `terraform plan`/`apply` —
  # replaces the prior frozen "2026-02-20" literal default, which never advanced
  # without a fresh apply and so silently stopped seeding `expected_unattempted` for
  # any date before it (120 days = 2880h).
  expected_universe_start_date_rolling = coalesce(
    var.expected_universe_start_date,
    formatdate("YYYY-MM-DD", timeadd(timestamp(), "-2880h")),
  )
}

locals {
  # asset_group → { instruments-store catalogue bucket, MTDS manifest bucket (shard target), per-AG enumerator args }.
  # Buckets are the canonical env-short shapes per resolve_bucket_name / cloud-providers.yaml
  # (matches lifecycle_catalogue_scheduler.tf; prediction = the "pred" short key).
  #
  # cpu/memory default to the shared 2/8Gi tier below (module defaults) — DeFi
  # overrides to 8/32Gi (documented memory-bump stopgap, 2026-08-01,
  # plans/active/issues/defi_v2_expected_universe_enumerator_oom_2026_08_01.md):
  # even after streaming BOTH the write path (Todo 1) and the manifest read
  # path, the enumerator's in-memory present_set/captured_set (built from
  # DeFi's ~29.96M-row consolidated index, ~30M present-set string-tuples) is
  # itself several-GB-to-tens-of-GB in pure Python object overhead, independent
  # of how the manifest is READ — streaming the read only bounds the transient
  # per-batch DataFrame, not the accumulated output sets. Per this craft's
  # EFFICIENCY north-star (stream first, scale hardware second) this is a
  # scoped, DeFi-only stopgap, not a blanket bump — cefi/tradfi/sports/
  # prediction stay on the working 2/8Gi tier.
  expected_universe_v2_asset_groups = {
    cefi       = { catalogue_bucket = "instruments-store-cefi-prd-central-element-323112", manifest_bucket = "market-data-tick-cefi-prd-central-element-323112" }
    defi       = { catalogue_bucket = "instruments-store-defi-prd-central-element-323112", manifest_bucket = "market-data-tick-defi-prd-central-element-323112", cpu = "8", memory = "32Gi" }
    tradfi     = { catalogue_bucket = "instruments-store-tradfi-prd-central-element-323112", manifest_bucket = "market-data-tick-tradfi-prd-central-element-323112" }
    sports     = { catalogue_bucket = "instruments-store-sports-prd-central-element-323112", manifest_bucket = "instruments-store-sports-prd-central-element-323112" }
    prediction = { catalogue_bucket = "instruments-store-pred-prd-central-element-323112", manifest_bucket = "market-data-tick-pred-prd-central-element-323112" }
  }
}

resource "google_service_account" "expected_universe_v2_enum" {
  project      = var.project_id
  account_id   = "expected-universe-v2-enum"
  display_name = "Expected-Universe v2 Enumerator — daily per-AG expected_unattempted seed"
}

# Read the catalogue + READ/WRITE the per-VM shard on the manifest bucket per AG.
resource "google_storage_bucket_iam_member" "expected_universe_v2_catalogue_reader" {
  for_each = { for ag, cfg in local.expected_universe_v2_asset_groups : ag => cfg.catalogue_bucket }
  bucket   = each.value
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.expected_universe_v2_enum.email}"
}

resource "google_storage_bucket_iam_member" "expected_universe_v2_manifest_admin" {
  for_each = { for ag, cfg in local.expected_universe_v2_asset_groups : ag => cfg.manifest_bucket }
  bucket   = each.value
  role     = "roles/storage.objectAdmin" # read manifest + write _index/per_vm/{vm}.parquet shard
  member   = "serviceAccount:${google_service_account.expected_universe_v2_enum.email}"
}

# CRITICAL (added 2026-06-22): the Cloud Scheduler job authenticates as the enum SA
# (oauth_token below) and POSTs the per-AG job's `:run` endpoint — so the enum SA
# MUST hold `roles/run.invoker` ON EACH JOB, or the scheduler trigger is rejected
# with `status code: 7 (PERMISSION_DENIED)` and the job NEVER executes. This was the
# silent gap that left `expected_unattempted=0` fleet-wide (defi/cefi/tradfi/sports
# never ran; only a hand-triggered prediction run existed) despite the scheduler +
# jobs being ENABLED — diagnosed via `gcloud scheduler jobs describe … status.code`.
# Provenance: plans/active/data_completion_to_100_all_ag_2026_06_21.md Progress Log
# 2026-06-22 DEFI lane PHASE A.
resource "google_cloud_run_v2_job_iam_member" "expected_universe_v2_run_invoker" {
  for_each = local.expected_universe_v2_asset_groups

  project  = var.project_id
  location = var.region
  name     = module.expected_universe_v2_job[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.expected_universe_v2_enum.email}"
}

module "expected_universe_v2_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.expected_universe_v2_asset_groups

  name                  = "expected-universe-v2-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.expected_universe_v2_enum.email

  # instruments-service image — enumerate_expected_universe.py lives at
  # /app/instruments-service/scripts/ (same image as lifecycle-catalogue-regen).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  # 222k-instrument cefi catalogue × window × data_types cross-product is RAM-heavy;
  # per-AG override (DeFi only, see the locals block above) for the present_set/
  # captured_set memory-bump stopgap.
  cpu             = try(each.value.cpu, "2")
  memory          = try(each.value.memory, "8Gi")
  timeout_seconds = 3600 # 60 min — catalogue load + cross-join + per-VM shard write
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = [
    "/app/instruments-service/scripts/enumerate_expected_universe.py",
    "--asset-group", each.key,
    "--enumerator-version", "v2",
    "--catalog-path", "gs://${each.value.catalogue_bucket}/${var.environment}/catalog.parquet",
    "--start-date", local.expected_universe_start_date_rolling,
    "--apply-write",
    "--max-writes-per-run", "50000000",
  ]

  environment_variables = {
    GCP_PROJECT_ID         = var.project_id
    PROJECT_ID             = var.project_id
    DEPLOYMENT_ENV         = var.environment
    CLOUD_PROVIDER         = "gcp"
    MANIFEST_PER_VM_SHARDS = "true"
    VM_NAME                = "enum-universe-v2-${each.key}" # stable per-AG shard; consolidator merges last-writer-wins
  }

  service_name = "expected-universe-v2-${each.key}"
  environment  = var.environment

  labels = {
    "purpose"     = "expected-universe-v2"
    "asset_group" = each.key
  }
}

resource "google_cloud_scheduler_job" "expected_universe_v2_daily" {
  for_each = local.expected_universe_v2_asset_groups

  project          = var.project_id
  region           = var.region
  name             = "expected-universe-v2-${each.key}-daily"
  description      = "Seed ${each.key} expected_unattempted rows from the lifecycle catalogue (v2 enumerator --apply-write, bounded recent window) so the coverage denominator counts the could-exist universe"
  schedule         = "30 1 * * *" # 01:30 UTC daily — after 01:00 lifecycle-catalogue-regen, before 02:00 downstream regens
  time_zone        = "UTC"
  attempt_deadline = "1800s"

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/expected-universe-v2-${each.key}:run"

    oauth_token {
      service_account_email = google_service_account.expected_universe_v2_enum.email
    }
  }
}
