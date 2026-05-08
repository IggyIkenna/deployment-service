# Terraform variables for features-sports-service
#
# SSOT: plans/active/features_sports_pipeline_deployment_2026_04_21.md (Phase 2).
# Cloud Run job + daily Workflow + Scheduler at 07:00 UTC + backfill Workflow.
# Image published by cloudbuild.yaml in features-sports-service repo to shared
# AR `unified-trading-system/features-sports-service:latest`.
#
# central-element-323112 placeholders substituted at deploy time via
# scripts/substitute-project-id.sh — matches features-onchain-service pattern.

project_id   = "central-element-323112"
region       = "asia-northeast1"
gcs_location = "asia-northeast1"
environment  = "prod"

job_name     = "features-sports-service-job"
docker_image = "asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/features-sports-service:latest"

service_account_email           = "features-sports-sa@central-element-323112.iam.gserviceaccount.com"
scheduler_service_account_email = "features-sports-sa@central-element-323112.iam.gserviceaccount.com"

cpu             = "2"
memory          = "8Gi"
timeout_seconds = 86400
max_retries     = 3

workflow_name          = "features-sports-service-daily"
backfill_workflow_name = "features-sports-service-backfill"

# Schedule at 07:00 UTC (one hour after Tier-1 sports discovery at 06:00 UTC so
# rolling-window FIXTURES are fresh before fixture_features compute).
schedule  = "0 7 * * *"
time_zone = "UTC"
