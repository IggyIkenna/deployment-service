# Terraform configuration for the CONSOLIDATED features-service sports sub-package
# (features_service/sports/*), deployed as its own Cloud Run Job + Workflow.
#
# SSOT: plans/active/features_sports_service_consolidation_deploy_2026_07_15.md
# (finishes the deploy-side gap left open by the 2026-05-08 features-sports-service
# -> features-service repo consolidation, plans/archive/features_repo_consolidation_2026_05_08.plan.md).
#
# Reuses features-service's own cloudbuild.yaml image (_SERVICE_NAME: features-service,
# _REGISTRY_REPO: unified-trading-system) — NOT a new per-family image/registry.
# This is a NEW, distinctly-named job/workflow set (features-service-sports-*). It was stood up
# to coexist with the legacy `features-sports-service-job` during the cutover; the legacy job +
# its daily/backfill workflows + paused daily-trigger scheduler were RETIRED 2026-07-15 (plan
# todo 7: `tofu state rm` + `gcloud delete`, all 404-confirmed, legacy dir
# terraform/services/features-sports-service/ removed) once this job was proven healthy
# end-to-end (manual exec qsqs4 + scheduled fire 6tm9w both SUCCEEDED) — no double-fire window.
#
# Evidence this is safe to build (real, not inferred):
#   - `docker run --rm --entrypoint python <features-service:latest digest c204c49d...> -c
#     "import unified_api_contracts.internal.schemas.rbac"` -> exit 0 (2026-07-15). The
#     fleet-wide UTL/UAC version-skew bug that broke the OLD image
#     (plans/active/issues/features_sports_service_cloud_run_job_broken_image_2026_07_15.md)
#     does NOT reproduce against the current features-service image.
#   - features_service/sports/cli/_providers.py + _fetch_runner.py (consolidated code, read
#     2026-07-15): "All data fetching is done by instruments-service which writes to GCS. FSS
#     reads from GCS only — no direct adapter instantiation." The old job's 4
#     secret_environment_variables (BETFAIR_APP_KEY / ODDS_API_KEY / ODDSJAM_API_KEY /
#     OPTICODDS_API_KEY) were for the PRE-consolidation direct-fetch architecture and are no
#     longer read anywhere in features_service/sports/* — intentionally NOT carried over here
#     (secret_environment_variables = {}, mirrors the features-calendar-service precedent).
#   - features-service's Dockerfile CMD is `["uvicorn", "features_service.api.main:app", ...]`
#     (the API-serving default for the consolidated multi-family image) — UNLIKE the legacy
#     per-family images, this Cloud Run JOB must override `command`/`args` at the terraform
#     level or every execution would boot the API server instead of running the sports CLI
#     to completion. See `command`/`args` on the `daily_job` module below.

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # DEPLOYMENT_ENV_SHORT convention (workspace bucket-name SSOT, `configs/cloud-providers.yaml`
  # comment header: dev | stg | prd) — this module only ever deploys `environment = "prod"`
  # (single terraform.tfvars, no dev/staging twin), mirrors the convention of the
  # now-retired legacy features-sports-service module.
  bucket_env_short_map = { dev = "dev", staging = "stg", prod = "prd" }
  bucket_env_short     = local.bucket_env_short_map[var.environment]

  # CLI surface (features_service/sports/cli/main.py + parser.py, read 2026-07-15): the
  # consolidated top-level dispatcher (features_service/cli/main.py) requires
  # `--feature-family sports` before any family flag; everything after that is forwarded
  # verbatim to features_service.sports.cli.main.main() unchanged from the legacy job's
  # contract (--operation/--mode/--asset-group per codex/06-coding-standards/cli-convention.md).
  dispatch_prefix = ["--feature-family", "sports"]

  # Daily fixture-features workflow — window unchanged from the legacy job (T-1 through T+7:
  # backlog catch-up for post-match enrichments + forward-horizon pre-match features for the
  # next seven days of fixtures). SSOT: plans/active/features_sports_pipeline_deployment_2026_04_21.md
  # Phase 1 CLI contract.
  workflow_yaml = <<-YAML
main:
  params: [args]
  steps:
    - init:
        assign:
          - project_id: "${var.project_id}"
          - region: "${var.region}"
          - job_name: "${var.job_name}"

    # Compute yesterday (T-1) and seven days ahead (T+7).
    - compute_dates:
        assign:
          - current_time: $${sys.now()}
          - yesterday_seconds: $${int(current_time) - 86400}
          - plus_seven_seconds: $${int(current_time) + (86400 * 7)}
          - yesterday_time: $${time.format(yesterday_seconds)}
          - plus_seven_time: $${time.format(plus_seven_seconds)}
          - start_date: $${text.substring(yesterday_time, 0, 10)}
          - end_date: $${text.substring(plus_seven_time, 0, 10)}

    # Run sports feature generation — compute fixture_features for the window.
    - run_features:
        call: http.post
        args:
          url: $${"https://" + region + "-run.googleapis.com/v2/projects/" + project_id + "/locations/" + region + "/jobs/" + job_name + ":run"}
          auth:
            type: OAuth2
          body:
            overrides:
              containerOverrides:
                - args:
                    - "--feature-family"
                    - "sports"
                    - "--operation"
                    - "compute"
                    - "--mode"
                    - "batch"
                    - "--asset-group"
                    - "SPORTS"
                    - "--tables"
                    - "fixture_features"
                    - "--start-date"
                    - $${start_date}
                    - "--end-date"
                    - $${end_date}
        result: features_response

    - get_execution:
        assign:
          - execution_name: $${features_response.body.metadata.name}

    - wait_features:
        call: http.get
        args:
          url: $${"https://" + region + "-run.googleapis.com/v2/" + execution_name}
          auth:
            type: OAuth2
        result: features_status

    - check_features:
        switch:
          - condition: $${"completionTime" in features_status.body}
            next: return_success
          - condition: $${map.get(features_status.body, "failedCount") != null and map.get(features_status.body, "failedCount") > 0}
            raise: "features-service-sports failed"
        next: features_wait_loop

    - features_wait_loop:
        call: sys.sleep
        args:
          seconds: 60
        next: wait_features

    - return_success:
        return:
          status: "completed"
          start_date: $${start_date}
          end_date: $${end_date}
          execution: $${execution_name}
          message: "features-service-sports completed"
YAML

  # Backfill workflow for historical data
  backfill_workflow_yaml = <<-YAML
main:
  params: [args]
  steps:
    - init:
        assign:
          - project_id: "${var.project_id}"
          - region: "${var.region}"
          - job_name: "${var.job_name}"
          - start_date: $${args.start_date}
          - end_date: $${args.end_date}
          - tables: $${default(map.get(args, "tables"), "fixture_features")}

    - build_args:
        assign:
          - base_args: ["--feature-family", "sports", "--operation", "compute", "--mode", "batch", "--asset-group", "SPORTS", "--start-date", $${start_date}, "--end-date", $${end_date}, "--tables", $${tables}]

    - run_backfill:
        call: http.post
        args:
          url: $${"https://" + region + "-run.googleapis.com/v2/projects/" + project_id + "/locations/" + region + "/jobs/" + job_name + ":run"}
          auth:
            type: OAuth2
          body:
            overrides:
              containerOverrides:
                - args: $${base_args}
        result: backfill_response

    - get_execution:
        assign:
          - execution_name: $${backfill_response.body.metadata.name}

    - wait_backfill:
        call: http.get
        args:
          url: $${"https://" + region + "-run.googleapis.com/v2/" + execution_name}
          auth:
            type: OAuth2
        result: backfill_status

    - check_backfill:
        switch:
          - condition: $${"completionTime" in backfill_status.body}
            next: return_success
          - condition: $${map.get(backfill_status.body, "failedCount") != null and map.get(backfill_status.body, "failedCount") > 0}
            raise: "backfill failed"
        next: backfill_wait_loop

    - backfill_wait_loop:
        call: sys.sleep
        args:
          seconds: 60
        next: wait_backfill

    - return_success:
        return:
          status: "completed"
          start_date: $${start_date}
          end_date: $${end_date}
          tables: $${tables}
          execution: $${execution_name}
          message: "features-service-sports backfill completed"
YAML
}

# Cloud Run Job for the consolidated features-service sports sub-package
module "daily_job" {
  source = "../../../modules/container-job/gcp"

  name       = var.job_name
  project_id = var.project_id
  region     = var.region

  image                 = var.docker_image
  service_account_email = var.service_account_email

  cpu             = var.cpu
  memory          = var.memory
  timeout_seconds = var.timeout_seconds
  max_retries     = var.max_retries

  # REQUIRED override — features-service's Dockerfile CMD defaults to `uvicorn
  # features_service.api.main:app ...` (the API-serving entrypoint shared by every
  # consolidated family). A Cloud Run JOB must run the CLI to a terminal exit code, not boot
  # a long-running server, so `command` pins the module invocation and `args` supplies a sane
  # default (every real invocation overrides `args` via the Workflow's containerOverrides
  # above — this default only fires on a bare `gcloud run jobs execute` with no overrides).
  #
  # The default carries an explicit `--date`/`--tables` (plan todo P2, 2026-07-15): `--mode
  # batch` requires a date (features_service/sports/cli/parser.py::_resolve_batch_dates raises
  # "Batch mode requires --date or both --start-date and --end-date"), so without one a bare
  # execute crashes on arg-validation before doing any work. 2026-07-14 is a fixed, immutable
  # historical fixture date (proven non-empty: 14 rows across leagues 129/2/848/874, plan
  # todo 5 evidence), so a bare smoke-execute is deterministic + cheap + honestly non-empty.
  command = ["python", "-m", "features_service"]
  args = concat(local.dispatch_prefix, [
    "--operation", "compute",
    "--mode", "batch",
    "--asset-group", "SPORTS",
    "--date", "2026-07-14",
    "--tables", "fixture_features",
  ])

  environment_variables = {
    ENVIRONMENT            = var.environment
    GCP_PROJECT_ID         = var.project_id
    GCS_REGION             = var.region
    GCS_LOCATION           = var.gcs_location
    GCS_FUSE_MOUNT_PATH    = "/mnt/gcs"
    UCS_SKIP_GCSFUSE_CHECK = "1"
    PYTHONUNBUFFERED       = "1"
  }

  # Canonical env-tiered bucket (bucket_estate_consolidation_to_sub100_2026_07_13.md,
  # features-sports Cutover phase, 2026-07-15) — the SAME bucket the legacy job was already
  # repointed to; the app never reads this FUSE mount path directly
  # (UCS_SKIP_GCSFUSE_CHECK=1 skips the mount health check), this is a from-birth infra
  # convention, not a load-bearing runtime path.
  gcs_volumes = [
    { name = "features-sports", bucket = "features-sports-${local.bucket_env_short}-${var.project_id}", read_only = false },
  ]

  # Intentionally empty — the consolidated features_service/sports/* no longer performs
  # direct provider fetches (features_service/sports/cli/_fetch_runner.py: "All data fetching
  # is done by instruments-service which writes to GCS. FSS reads from GCS only.") — the
  # legacy job's BETFAIR_APP_KEY / ODDS_API_KEY / ODDSJAM_API_KEY / OPTICODDS_API_KEY secrets
  # are not read anywhere in this package anymore. Mirrors the features-calendar-service
  # precedent (also secret_environment_variables = {}).
  secret_environment_variables = {}

  service_name = "features-service-sports"
  environment  = var.environment

  labels = {
    "app"     = "features-service-sports"
    "version" = "v3"
  }
}

# Daily T+1..T+7 Workflow
module "daily_workflow" {
  source = "../../../modules/workflow/gcp"

  name                  = var.workflow_name
  project_id            = var.project_id
  region                = var.region
  description           = "Daily T-1..T+7 workflow for the consolidated features-service sports sub-package"
  service_account_email = var.service_account_email

  workflow_source = local.workflow_yaml

  # NOTE (resolved 2026-07-15): this scheduler is now ENABLED. During cutover it was created
  # then immediately paused until (a) a manual execution reached a genuine SUCCEEDED terminal
  # state (exec qsqs4) and (b) the legacy features-sports-service-daily-trigger was retired
  # (plan todo 7, 404-confirmed) — both conditions are met, so there is no double-fire risk and
  # `gcloud scheduler jobs resume ${var.workflow_name}-trigger` was run (real scheduled fire
  # 6tm9w SUCCEEDED). Keep this scheduler ENABLED.
  schedule                        = var.schedule
  time_zone                       = var.time_zone
  scheduler_service_account_email = var.scheduler_service_account_email

  workflow_args = {
    trigger = "scheduled"
  }

  labels = {
    "app"     = "features-service-sports"
    "type"    = "daily"
    "version" = "v3"
  }
}

# Backfill Workflow (manual trigger only)
module "backfill_workflow" {
  source = "../../../modules/workflow/gcp"

  name                  = var.backfill_workflow_name
  project_id            = var.project_id
  region                = var.region
  description           = "Historical backfill workflow for the consolidated features-service sports sub-package"
  service_account_email = var.service_account_email

  workflow_source = local.backfill_workflow_yaml

  # No schedule - manual trigger only
  schedule  = null
  time_zone = var.time_zone

  labels = {
    "app"     = "features-service-sports"
    "type"    = "backfill"
    "version" = "v3"
  }
}
