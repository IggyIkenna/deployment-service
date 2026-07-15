# Terraform variables for the consolidated features-service sports sub-package
#
# SSOT: plans/active/features_sports_service_consolidation_deploy_2026_07_15.md.
# Cloud Run job + daily Workflow + Scheduler at 07:00 UTC + backfill Workflow — finishes the
# deploy-side gap left by plans/archive/features_repo_consolidation_2026_05_08.plan.md.
# Image published by features-service's OWN cloudbuild.yaml (_SERVICE_NAME: features-service,
# _REGISTRY_REPO: unified-trading-system) — NOT a dedicated per-family image/registry.
#
# central-element-323112 placeholders substituted at deploy time via
# scripts/substitute-project-id.sh — matches the legacy features-sports-service pattern.

project_id   = "central-element-323112"
region       = "asia-northeast1"
gcs_location = "asia-northeast1"
environment  = "prod"

job_name = "features-service-sports-job"
# Pinned to an explicit digest (not `:latest`) so the Cloud Run job runs a KNOWN, verified image:
# Cloud Run resolves a tag→digest at job create/update time, so a bare `:latest` silently keeps
# whatever digest was current at the LAST apply — which is exactly how the job ran the stale broken
# c204c49d image (execution features-service-sports-job-kk4dv, 2026-07-15, false CONSOLIDATOR_DOWN).
# This digest = features-service 0.66.0 / commit afbe1ef, built 2026-07-15T19:38Z. VERIFIED in-image
# (docker run) to contain BOTH fixes: (1) unified_api_contracts.internal resolves cleanly, and
# (2) UTL c47273c1 lock-aware assert_consolidator_healthy (consolidator_cycle_in_flight short-circuit).
# On a future features-service rollout, re-pin this to the new verified digest (see plan todo note).
docker_image = "asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading-system/features-service@sha256:b7fc3d7f7b92fe37edfae592b8c62244ecc46d5598dd4e08571508de08fb3117"

# Reuses the SAME service account as the legacy job (confirmed still active 2026-07-15) —
# no new IAM surface needed; the SA's only GCS/Secret Manager grants it actually needs
# (features-sports-prd bucket read/write) are unchanged by this consolidation.
service_account_email           = "features-sports-sa@central-element-323112.iam.gserviceaccount.com"
scheduler_service_account_email = "features-sports-sa@central-element-323112.iam.gserviceaccount.com"

cpu             = "2"
memory          = "8Gi"
timeout_seconds = 86400
max_retries     = 3

workflow_name          = "features-service-sports-daily"
backfill_workflow_name = "features-service-sports-backfill"

# Schedule at 07:00 UTC (one hour after Tier-1 sports discovery at 06:00 UTC so
# rolling-window FIXTURES are fresh before fixture_features compute) — SAME schedule as the
# legacy job. Deploy phase must pause the resulting scheduler job immediately after apply
# (see main.tf note) until the new job is verified healthy and the legacy job is retired.
schedule  = "0 7 * * *"
time_zone = "UTC"
