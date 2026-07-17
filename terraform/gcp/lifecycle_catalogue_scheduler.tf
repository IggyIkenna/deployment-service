# Instrument LIFECYCLE catalogue roll-up — daily Cloud Scheduler trigger (per asset_group)
#
# G1 foundation (master_data_canonicalisation_migration_catalogue_2026_06_07 §G1.schedule +
# proper_instrument_catalogue_lifecycle_rollup_2026_06_04): the "could-exist universe"
# root. This schedules instruments-service `scripts/build_instrument_catalogue.py` — the
# LIFECYCLE roll-up that rolls the maintained per-date
#   instrument_availability/by_date/day=…/venue=…/instruments.parquet
# snapshots into the cumulative available_from/available_to catalogue
#   gs://{instruments-store-<ag>}/{DEPLOYMENT_ENV}/catalog.parquet
# which `enumerate_expected_universe.py --enumerator-version v2` then cross-joins ×
# dates × data_types − existing manifest → seeds record_expected_unattempted.
#
# DISTINCT FROM the two sibling schedulers (do NOT conflate):
#   * catalogue_regen_scheduler.tf      → enumerate_envelope/availability/strategy_instruments (UAC artefacts)
#   * instrument_catalogue_scheduler.tf → generate_instrument_catalogue.py (UAC drilldown json/md)
# Neither of those runs build_instrument_catalogue.py — this file fills that gap so the v2
# enumerator always reads a FRESH lifecycle catalogue per AG.
#
# AG-PARAMETRIC: one Cloud Run Job + Scheduler per asset_group (for_each), so a per-AG
# failure/retry is isolated (every AG GREEN independently, per the data-pipeline-correctness
# rule). build_instrument_catalogue.py is already AG-parametric (--asset-group choices =
# cefi/defi/tradfi/sports/prediction; bucket via get_write_bucket_name("instruments", ag),
# env-tier). Sports keys its by-date snapshots under sports_reference/by_date so it carries
# the --by-date-prefix override.
#
# Trigger time: 01:00 UTC daily — AFTER the 00:00 UTC instruments-service FAST refresh
# (writes the by-date snapshots this rolls up) and BEFORE the 02:00 UTC
# instrument-catalogue-regen + 04:30 UTC catalogue-regen (downstream consumers of the
# rolled-up catalogue).
#
# NOT fire-and-forget: each AG is a bounded Cloud Run Job (STARTED/STOPPED/FAILED exit
# status + ServiceBootstrap events) with scheduler retry_config; post-apply the operator
# verifies each job RAN (T+10min: `gcloud run jobs executions list --job
# lifecycle-catalogue-regen-<ag>` = Succeeded) before the gate is GREEN.
#
# References:
#   plans/active/master_data_canonicalisation_migration_catalogue_2026_06_07.md §G1.schedule
#   plans/active/proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md
#
# Prediction bucket-name reconciliation (RESOLVED 2026-07-06):
#   cloud-providers.yaml maps `instruments-store-prediction` → `instruments-store-pred-${DEPLOYMENT_ENV_SHORT}-…`
#   (SSOT). This file's per-AG map (line 72 below) uses the canonical `instruments-store-pred-prd-…` short-key
#   where the daily catalog.parquet actually lands (verified via B1 flip 2026-07-06: prediction 103.24MiB in the
#   canonical bucket). The sibling schedulers (instrument_catalogue_scheduler.tf +
#   catalogue_regen_scheduler.tf) were reconciled to the same SSOT bucket alongside this comment update
#   (`is_catalogue_completion_2d_2026_07_06` P2 fix).

locals {
  # asset_group → { bucket = instruments-store bucket, extra_args = per-AG CLI overrides }
  # CORRECTED 2026-06-11 (canonicalisation autonomous run): the prior literals were the
  # LEGACY flat buckets (no env segment) + a nonexistent instruments-store-prediction-…
  # (404 — the live bucket is the "pred" short key). The lifecycle roll-up reads/writes
  # the CANONICAL env-short buckets (where prod/catalog.parquet actually lives, verified
  # via gcloud storage ls 2026-06-11) per resolve_bucket_name / cloud-providers.yaml.
  # `memory`/`cpu` are per-AG (default 4Gi/2). tradfi OOM'd at 4Gi (2026-06-19), 8Gi AND
  # 16Gi (2026-06-23 catch-up, signal-9 after ~12 min) — "configured memory limit was
  # reached" — because it has the largest universe (equities + CME/CBOE futures + EC*
  # event contracts; 11.6k by_date parquets) AND the by-date roll-up *buffered the whole
  # corpus in memory* (the old ThreadPoolExecutor.map eagerly downloaded + queued EVERY
  # frame). DURABLE FIX SHIPPED 2026-06-23 (build_instrument_catalogue.py
  # _bounded_parallel_load): the download is now a memory-BOUNDED sliding window — at most
  # max_workers (16) frames in flight, each yielded into the streaming aggregate fold and
  # dropped before the next — so peak memory is O(max_workers) not O(11.6k). With memory
  # no longer the constraint, tradfi is back to 16Gi/cpu4 (the 32Gi band-aid is removed;
  # 16Gi is generous headroom for 16 in-flight equities frames). The slow leg is now the
  # 11.6k-blob GCS read, so `timeout_seconds` is raised 1800→3600 (+ the scheduler
  # attempt_deadline) to give the read room. Bump any other AG here only if its roll-up OOMs.
  #
  # prediction bumped 4Gi/cpu2 → 16Gi/cpu4 (2026-07-15, DP_CATALOG_NOT_RUNNING follow-up):
  # the DAILY incremental job SIGKILLed (signal 9) at the [BISECT-E] monotonic-guard +
  # promote-write stage on 3 consecutive days (07-13, 07-14, 07-15) — confirmed via live
  # `gcloud logging read`, both retry attempts hit the identical point each day, no
  # traceback/CATALOGUE_ROLLUP_FAILED event (process killed externally = OOM signature).
  # Even though the by_date window itself was empty (0 new rows some days), the merge still
  # holds the full 2,673,230-row previous catalogue in memory for the guard+promote step —
  # functionally the same aggregate size as the WEEKLY --mode full job below, which OOM'd at
  # this SAME 4Gi on its first cycle (2026-07-04, "2.5M-row multi-grain aggregate") and was
  # already bumped to 16Gi/cpu4. Matching the daily job to its own sibling full-job
  # provisioning (rather than guessing) — Cloud Run couples CPU/memory (16Gi needs cpu>=4).
  lifecycle_catalogue_asset_groups = {
    cefi   = { bucket = "instruments-store-cefi-prd-central-element-323112", extra_args = [], memory = "4Gi", cpu = "2" }
    defi   = { bucket = "instruments-store-defi-prd-central-element-323112", extra_args = [], memory = "4Gi", cpu = "2" }
    tradfi = { bucket = "instruments-store-tradfi-prd-central-element-323112", extra_args = [], memory = "16Gi", cpu = "4" }
    sports = { bucket = "instruments-store-sports-prd-central-element-323112", extra_args = ["--by-date-prefix", "sports_reference/by_date"], memory = "4Gi", cpu = "2" }
    # prediction → flat "pred" short key per cloud-providers.yaml (instruments-store-prediction-… does not exist).
    prediction = { bucket = "instruments-store-pred-prd-central-element-323112", extra_args = [], memory = "16Gi", cpu = "4" }
  }
}

resource "google_service_account" "lifecycle_catalogue_regen" {
  project      = var.project_id
  account_id   = "lifecycle-catalogue-regen"
  display_name = "Lifecycle Catalogue Regen — daily per-AG instrument available_from/to roll-up"
}

# Read by-date snapshots + WRITE catalog.parquet on every per-AG instruments-store bucket.
resource "google_storage_bucket_iam_member" "lifecycle_catalogue_instruments_admin" {
  for_each = { for ag, cfg in local.lifecycle_catalogue_asset_groups : ag => cfg.bucket }
  bucket   = each.value
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.lifecycle_catalogue_regen.email}"
}

# CRITICAL (added 2026-06-23): the Cloud Scheduler job authenticates as the
# lifecycle-catalogue-regen SA (oauth_token below) and POSTs the per-AG job's
# `:run` endpoint — so the SA MUST hold `roles/run.invoker` ON EACH JOB, or the
# scheduler trigger is rejected with `status code: 7 (PERMISSION_DENIED)` and the
# job NEVER executes. This is the SAME silent gap that left expected_unattempted=0
# fleet-wide (see expected_universe_v2_scheduler.tf) — the catalogue half had the
# missing grant too, which (with the schedulers paused since 2026-06-14) left the
# prediction (+ all AG) lifecycle catalog.parquet STALE → the v2 enumerator
# cross-joined a stale could-exist universe → dishonest coverage denominators.
# Provenance: prediction catalogue daily-aggregation un-pause 2026-06-23.
resource "google_cloud_run_v2_job_iam_member" "lifecycle_catalogue_regen_run_invoker" {
  for_each = local.lifecycle_catalogue_asset_groups

  project  = var.project_id
  location = var.region
  name     = module.lifecycle_catalogue_regen_job[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.lifecycle_catalogue_regen.email}"
}

module "lifecycle_catalogue_regen_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.lifecycle_catalogue_asset_groups

  name                  = "lifecycle-catalogue-regen-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.lifecycle_catalogue_regen.email

  # instruments-service image — build_instrument_catalogue.py lives at
  # /app/instruments-service/scripts/ (Dockerfile WORKDIR=/app/instruments-service, COPY . .).
  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = each.value.cpu    # per-AG (default 2; tradfi 4 — Cloud Run requires cpu>=4 at 16Gi)
  memory          = each.value.memory # per-AG (default 4Gi; tradfi 16Gi — OOM'd at 4Gi+8Gi)
  timeout_seconds = 3600              # 60 min — vast headroom for the DAILY --mode incremental run (~90s tradfi window read; the old full walk that needed this budget now lives in the weekly lifecycle-catalogue-full-* jobs below)
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = concat(
    [
      "/app/instruments-service/scripts/build_instrument_catalogue.py",
      "--asset-group",
      each.key,
    ],
    each.value.extra_args,
  )

  environment_variables = {
    GCP_PROJECT_ID = var.project_id
    DEPLOYMENT_ENV = var.environment
    CLOUD_PROVIDER = "gcp"
    # Force Python stdout/stderr to line-buffered mode in the Cloud Run container so
    # the bisection markers (print+flush=True in run_rollup) surface immediately in
    # Cloud Logging rather than being swallowed by the default block-buffered mode
    # (the previous "truncated traceback" symptom on job failures).
    PYTHONUNBUFFERED = "1"
  }

  service_name = "lifecycle-catalogue-regen-${each.key}"
  environment  = var.environment

  labels = {
    "purpose"     = "lifecycle-catalogue-regen"
    "asset_group" = each.key
  }
}

resource "google_cloud_scheduler_job" "lifecycle_catalogue_regen_daily" {
  for_each = local.lifecycle_catalogue_asset_groups

  project          = var.project_id
  region           = var.region
  name             = "lifecycle-catalogue-regen-${each.key}-daily"
  description      = "Roll up ${each.key} instrument_availability/by_date snapshots into the available_from/to lifecycle catalogue (catalog.parquet) for the v2 expected-universe enumerator"
  schedule         = "0 1 * * *" # 01:00 UTC daily — after 00:00 IS FAST refresh, before 02:00/04:30 downstream catalogue regens
  time_zone        = "UTC"
  attempt_deadline = "1800s" # scheduler only fires the :run POST (returns immediately); the JOB's own timeout_seconds=3600 bounds the roll-up

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/lifecycle-catalogue-regen-${each.key}:run"

    oauth_token {
      service_account_email = google_service_account.lifecycle_catalogue_regen.email
    }
  }
}

# ── Weekly --mode full self-heal (incremental-rollup plan Phase 3) ────────────
# The DAILY jobs run `--mode incremental` (the script default since
# instruments_catalogue_incremental_rollup_2026_06_29): prev catalog.parquet +
# a self-widening trailing window (~90s tradfi vs 2h17m full). A retroactive
# correction to a by_date file OLDER than the window would be invisible to the
# increment, so a WEEKLY `--mode full` re-aggregation self-heals any drift.
#
# ALL by_date-walking AGs — which is all five. The previous note here read "sports is a
# manifest single-read — always full" and excluded it; that was true ONLY for the
# league-grain manifest projection. Since the 2026-07-09 FTP extension sports also walks
# sports_reference/by_date for fixture/team/player grain (318,889 blobs full-history),
# making it the biggest walker here and the AG most in need of this self-heal. Corrected
# 2026-07-17 — see lifecycle_catalogue_full_extra_args for why sports also needs --since.
#
# timeout: the full tradfi walk measured 2h17m (2026-06-29) and grows with
# history; 21600s (6h) gives ~2.6× headroom. The old "3600 = the Cloud-Run-Jobs
# ceiling" note was WRONG — Cloud Run Jobs task timeout goes to 24h; 3600s was
# only ever this file's chosen budget (validated at terraform apply).
locals {
  # sports INCLUDED since 2026-07-17. The prior `if ag != "sports"` exclusion rested
  # on the header's "sports is a manifest single-read — always full" claim, which was
  # true only while the sports catalogue was league-grain-only (manifest-derived). The
  # 2026-07-09 FTP extension (build_sports_fixture_team_player_catalogue) made sports a
  # by_date WALKER — and the largest one we have: 318,889 fixture/team/injuries blobs
  # over the full 2019→2026 corpus, vs 51,483 for its 400d window (measured 2026-07-17).
  # So sports was the ONE asset group that needed this block's own stated rationale — "a
  # retroactive correction to a by_date file OLDER than the window would be invisible to
  # the increment" — and was the only one excluded from it.
  lifecycle_catalogue_full_asset_groups = local.lifecycle_catalogue_asset_groups
  # Stagger the weekly full rebuilds (Saturday, one hour apart) so the whole-corpus GCS
  # walks don't run concurrently. sports takes the 07:00 slot (after prediction).
  lifecycle_catalogue_full_schedule = {
    cefi       = "0 3 * * 6"
    defi       = "0 4 * * 6"
    tradfi     = "0 5 * * 6"
    prediction = "0 6 * * 6"
    sports     = "0 7 * * 6"
  }
  # Weekly FULL rebuilds hold the whole-history aggregate in memory — the daily
  # jobs no longer do (incremental window), so the full jobs need their OWN
  # memory floor. prediction's 2.5M-row multi-grain aggregate OOM'd at the daily
  # map's 4Gi on the first weekly cycle (2026-07-04, "configured memory limit
  # was reached", execution lifecycle-catalogue-full-prediction-6h4fd); cefi
  # (1h39m) and defi (41m) completed their full walks at 4Gi the same cycle.
  lifecycle_catalogue_full_memory = {
    cefi       = "4Gi"
    defi       = "4Gi"
    tradfi     = "16Gi"
    prediction = "16Gi"
    # sports 4Gi (cefi/defi tier), NOT a 16Gi guess: the full-history walk was measured
    # locally 2026-07-17 at ~148MB RSS held FLAT across all 318,889 blobs — the
    # _bounded_parallel_load sliding window keeps peak memory O(max_workers) frames, and
    # the resulting aggregate (~136k rows) is ~5% of prediction's 2.5M-row aggregate that
    # actually OOM'd at 4Gi. Bump only if it OOMs (this block's own standing rule).
    sports = "4Gi"
  }
  # Cloud Run couples CPU and memory (2 CPU caps memory at 8Gi; 16Gi needs 4) —
  # the 16Gi full jobs carry 4 CPU like the tradfi daily.
  lifecycle_catalogue_full_cpu = {
    cefi       = "2"
    defi       = "2"
    tradfi     = "4"
    prediction = "4"
    sports     = "2"
  }
  # FULL-job-only extra args, on top of each AG's shared daily extra_args.
  #
  # sports MUST carry --since here or this job is a LIE: `--mode full` is a no-op for the
  # sports FTP roll-up, which is exempt from the generic incremental engine and always
  # uses its own hardcoded SPORTS_FTP_WINDOW_DAYS=400 trailing window (run_rollup's sports
  # branch). Without --since, a "full self-heal" would silently re-walk the SAME ~13
  # months and report success — the exact failure mode that left the catalogue holding one
  # season (17,064 fixtures) while the raw corpus ran 2019→2026. --since (added
  # instruments-service@4a795c24) is the only supported way to rebuild the full corpus.
  #
  # Cost: ~318,889 blobs. The 6h timeout_seconds below carries it (in-region reference:
  # the tradfi full walk is 2h17m; ~3h measured from a laptop over transpacific GCS, which
  # is the pessimistic bound). The DAILY job deliberately does NOT get --since — it stays
  # on the cheap 400d window and the frozen-tail merge carries the older rows forward, so
  # the full walk is paid weekly here, never on the 60-min daily budget.
  lifecycle_catalogue_full_extra_args = {
    sports = ["--since", "2019-01-01"]
  }
}

module "lifecycle_catalogue_full_job" {
  source   = "../modules/container-job/gcp"
  for_each = local.lifecycle_catalogue_full_asset_groups

  name                  = "lifecycle-catalogue-full-${each.key}"
  project_id            = var.project_id
  region                = var.region
  service_account_email = google_service_account.lifecycle_catalogue_regen.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/unified-trading-system/instruments-service:latest"

  cpu             = local.lifecycle_catalogue_full_cpu[each.key]
  memory          = local.lifecycle_catalogue_full_memory[each.key]
  timeout_seconds = 21600 # 6h — full tradfi walk is 2h17m and grows; Jobs ceiling is 24h
  max_retries     = 1
  parallelism     = 1
  task_count      = 1

  command = ["python"]
  args = concat(
    [
      "/app/instruments-service/scripts/build_instrument_catalogue.py",
      "--asset-group",
      each.key,
      "--mode",
      "full",
    ],
    each.value.extra_args,
    lookup(local.lifecycle_catalogue_full_extra_args, each.key, []),
  )

  environment_variables = {
    GCP_PROJECT_ID   = var.project_id
    DEPLOYMENT_ENV   = var.environment
    CLOUD_PROVIDER   = "gcp"
    PYTHONUNBUFFERED = "1"
  }

  service_name = "lifecycle-catalogue-full-${each.key}"
  environment  = var.environment

  labels = {
    "purpose"     = "lifecycle-catalogue-full-selfheal"
    "asset_group" = each.key
  }
}

resource "google_cloud_run_v2_job_iam_member" "lifecycle_catalogue_full_run_invoker" {
  for_each = local.lifecycle_catalogue_full_asset_groups

  project  = var.project_id
  location = var.region
  name     = module.lifecycle_catalogue_full_job[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.lifecycle_catalogue_regen.email}"
}

resource "google_cloud_scheduler_job" "lifecycle_catalogue_full_weekly" {
  for_each = local.lifecycle_catalogue_full_asset_groups

  project          = var.project_id
  region           = var.region
  name             = "lifecycle-catalogue-full-${each.key}-weekly"
  description      = "Weekly --mode full self-heal of the ${each.key} lifecycle catalogue (repairs drift from by_date corrections older than the incremental window)"
  schedule         = local.lifecycle_catalogue_full_schedule[each.key]
  time_zone        = "UTC"
  attempt_deadline = "1800s" # fires the :run POST only; the JOB's timeout_seconds=21600 bounds the rebuild

  retry_config {
    retry_count          = 1
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/lifecycle-catalogue-full-${each.key}:run"

    oauth_token {
      service_account_email = google_service_account.lifecycle_catalogue_regen.email
    }
  }
}
