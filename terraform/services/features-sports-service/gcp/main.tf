# Terraform configuration for features-sports-service
# Generates sports betting features (odds, line movement, market efficiency)
# Creates Cloud Run Job + Workflow for daily T+1 operations

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
  # (single terraform.tfvars, no dev/staging twin), so the map is defensive rather than
  # load-bearing today.
  bucket_env_short_map = { dev = "dev", staging = "stg", prod = "prd" }
  bucket_env_short     = local.bucket_env_short_map[var.environment]

  # Daily fixture-features workflow
  #
  # Window: yesterday (T-1) through T+7 — covers backlog catch-up for the
  # previous day (post-match enrichments that land hours after kickoff) AND
  # forward-horizon pre-match features for the next seven days of fixtures.
  # SSOT: plans/active/features_sports_pipeline_deployment_2026_04_21.md
  # Phase 1 CLI contract. Tier-3 `features_pre_match` at T-1h in
  # configs/sports-trigger-tiers.yaml still fires per-fixture on top of this
  # daily catch-up.
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
            raise: "features-sports failed"
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
          message: "features-sports-service completed"
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
          - base_args: ["--operation", "compute", "--mode", "batch", "--asset-group", "SPORTS", "--start-date", $${start_date}, "--end-date", $${end_date}, "--tables", $${tables}]

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
          message: "features-sports-service backfill completed"
YAML
}

# Cloud Run Job for features-sports-service
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

  environment_variables = {
    ENVIRONMENT            = var.environment
    GCP_PROJECT_ID         = var.project_id
    GCS_REGION             = var.region
    GCS_LOCATION           = var.gcs_location
    GCS_FUSE_MOUNT_PATH    = "/mnt/gcs"
    UCS_SKIP_GCSFUSE_CHECK = "1"
    PYTHONUNBUFFERED       = "1"
  }

  # Cutover 2026-07-15 (bucket_estate_consolidation_to_sub100_2026_07_13.md, features-sports
  # Cutover phase): repointed from the bare flat `features-sports-${var.project_id}` to the
  # canonical env-tiered bucket the application code actually reads/writes via
  # `resolve_bucket(kind="features-sports", asset_group="sports")` ->
  # `cloud-providers.yaml`'s `features-sports-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}`. The
  # app never consumes this FUSE mount path directly (no `/mnt/gcs` reads in
  # features-service; `UCS_SKIP_GCSFUSE_CHECK=1` already skips the FUSE-mount health check
  # below) — this mount is a from-birth infra convention, not a load-bearing runtime path.
  gcs_volumes = [
    { name = "features-sports", bucket = "features-sports-${local.bucket_env_short}-${var.project_id}", read_only = false },
  ]

  secret_environment_variables = {
    BETFAIR_APP_KEY = {
      secret_name = "betfair-app-key"
      version     = "latest"
    }
    ODDS_API_KEY = {
      secret_name = "odds-api-key"
      version     = "latest"
    }
    ODDSJAM_API_KEY = {
      secret_name = "oddsjam-api-key"
      version     = "latest"
    }
    OPTICODDS_API_KEY = {
      secret_name = "opticodds-api-key"
      version     = "latest"
    }
  }

  service_name = "features-sports-service"
  environment  = var.environment

  labels = {
    "app"     = "features-sports-service"
    "version" = "v2"
  }
}

# Daily T+1 Workflow
module "daily_workflow" {
  source = "../../../modules/workflow/gcp"

  name                  = var.workflow_name
  project_id            = var.project_id
  region                = var.region
  description           = "Daily T+1 workflow for features-sports-service"
  service_account_email = var.service_account_email

  workflow_source = local.workflow_yaml

  # Schedule at 11:00 AM UTC (parallel with other feature services)
  schedule                        = var.schedule
  time_zone                       = var.time_zone
  scheduler_service_account_email = var.scheduler_service_account_email

  workflow_args = {
    trigger = "scheduled"
  }

  labels = {
    "app"     = "features-sports-service"
    "type"    = "daily"
    "version" = "v2"
  }
}

# Backfill Workflow (manual trigger)
module "backfill_workflow" {
  source = "../../../modules/workflow/gcp"

  name                  = var.backfill_workflow_name
  project_id            = var.project_id
  region                = var.region
  description           = "Historical backfill workflow for features-sports-service"
  service_account_email = var.service_account_email

  workflow_source = local.backfill_workflow_yaml

  # No schedule - manual trigger only
  schedule  = null
  time_zone = var.time_zone

  labels = {
    "app"     = "features-sports-service"
    "type"    = "backfill"
    "version" = "v2"
  }
}
