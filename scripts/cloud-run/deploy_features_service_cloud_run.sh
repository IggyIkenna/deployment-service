#!/usr/bin/env bash
# Deploy features-service to Cloud Run (asia-northeast1).
#
# OPERATOR-SIDE ONLY — do NOT run this script from an agent.
# The actual `gcloud run deploy` invocation is intentionally left to the operator
# (it requires production GCP credentials + image already built + pushed).
#
# ── Prerequisites ──────────────────────────────────────────────────────────────
# 1. Image is built and pushed:
#      gcloud builds submit /path/to/features-service \
#        --config=features-service/cloudbuild.yaml \
#        --substitutions=_PROJECT_ID=${PROJECT_ID} \
#        --timeout=1800 \
#        --project=${PROJECT_ID}
#    OR local docker buildx:
#      docker buildx build --platform linux/amd64 \
#        --build-arg PROJECT_ID=${PROJECT_ID} \
#        -t asia-northeast1-docker.pkg.dev/${PROJECT_ID}/features-service/features-service:latest \
#        --push features-service/
#
# 2. Artifact Registry repo exists:
#      gcloud artifacts repositories create features-service \
#        --repository-format=docker --location=asia-northeast1 \
#        --project=${PROJECT_ID}
#
# 3. Service account features-prod@... exists with correct IAM bindings
#    (SSOT: deployment-service/configs/gcp_service_accounts.yaml#features-prod).
#
# 4. Secret Manager secret mdps-redis-url-prod exists:
#      gcloud secrets create mdps-redis-url-prod \
#        --replication-policy=user-managed \
#        --locations=asia-northeast1 \
#        --project=${PROJECT_ID}
#      echo -n "redis://<host>:<port>" | \
#        gcloud secrets versions add mdps-redis-url-prod --data-file=- \
#        --project=${PROJECT_ID}
#    Grant the features-prod SA access:
#      gcloud secrets add-iam-policy-binding mdps-redis-url-prod \
#        --member="serviceAccount:features-prod@${PROJECT_ID}.iam.gserviceaccount.com" \
#        --role="roles/secretmanager.secretAccessor" \
#        --project=${PROJECT_ID}
#
# ── Usage ──────────────────────────────────────────────────────────────────────
# bash scripts/cloud-run/deploy_features_service_cloud_run.sh [--dry-run]
#
# --dry-run : print the gcloud command without executing it
#
# ── Expected output ────────────────────────────────────────────────────────────
# Service URL: https://features-service-<hash>-an.a.run.app
# Traffic: 100% → latest revision
# Status: Ready
#
# ── Smoke-test commands ────────────────────────────────────────────────────────
# After deploy:
#   SERVICE_URL=$(gcloud run services describe features-service \
#     --region=asia-northeast1 --project=${PROJECT_ID} \
#     --format="value(status.url)")
#
#   # 1. Health check
#   curl -s "${SERVICE_URL}/health" | python3 -m json.tool
#   # Expected: {"status": "ok", "service": "features-service", "version": "...",
#   #            "data_freshness": {"families": {...}, "loaded_families": [...], "healthy": true}}
#
#   # 2. Liveness
#   curl -s "${SERVICE_URL}/liveness"
#   # Expected: {"status": "ok"}
#
#   # 3. Readiness
#   curl -s "${SERVICE_URL}/readiness"
#   # Expected: {"status": "ok"}
#
# ── 24-hour soak readiness criterion ──────────────────────────────────────────
# The soak passes when ALL of the following hold for 24 consecutive hours:
#   a) /health returns "healthy": true (no family probe raises)
#   b) Feature parquets appear in the features bucket every N seconds per active
#      feature_group (verified via manifest rows with capture_status=captured)
#   c) No FAILED events in the alerting-service event stream for features-service
#   d) No manifest gaps (all expected rows are captured or empty_confirmed with reason)
#
# ── SSOT pointers ──────────────────────────────────────────────────────────────
# - Cloud Run spec:  deployment-service/configs/cloud-run/features-service.yaml
# - Service account: deployment-service/configs/gcp_service_accounts.yaml#features-prod
# - Plan Phase E:    unified-trading-pm/plans/active/phase5_features_streaming_carry_staked_basis_mvp_2026_05_19.md
# - Region policy:   CLAUDE.md ("all GCS data is in asia-northeast1")

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
REGION="asia-northeast1"
SERVICE="features-service"
IMAGE="asia-northeast1-docker.pkg.dev/${PROJECT_ID}/features-service/features-service:latest"
SA="features-prod@${PROJECT_ID}.iam.gserviceaccount.com"

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *)
      echo "ERROR: unrecognised argument: $arg" >&2
      echo "       Usage: bash scripts/cloud-run/deploy_features_service_cloud_run.sh [--dry-run]" >&2
      exit 2
      ;;
  esac
done

echo "=== Deploy target: features-service → ${REGION} (${PROJECT_ID}) ==="
echo "  Image:           ${IMAGE}"
echo "  Service account: ${SA}"
echo ""

# Runtime env vars (non-secret).
# DEPLOYMENT_ENV controls writegate behaviour for bucket name resolution.
RUNTIME_ENV=(
  "DEPLOYMENT_ENV=prod"
  "CLOUD_PROVIDER=gcp"
  "GCP_PROJECT_ID=${PROJECT_ID}"
  "GCS_REGION=asia-northeast1"
  "GCS_LOCATION=asia-northeast1"
  "UCS_SKIP_GCSFUSE_CHECK=1"
  "FEATURES_ASSET_GROUPS=cefi,defi,tradfi,sports,prediction"
)
RUNTIME_ENV_STR=$(IFS=,; echo "${RUNTIME_ENV[*]}")

# Secrets mounted as env vars.
# mdps-redis-url-prod must exist in Secret Manager (see prerequisites above).
RUNTIME_SECRETS="MDPS_REDIS_URL=mdps-redis-url-prod:latest"

DEPLOY_CMD=(
  gcloud run deploy "${SERVICE}"
    --image "${IMAGE}"
    --region "${REGION}"
    --project "${PROJECT_ID}"
    --platform managed
    --no-allow-unauthenticated
    --port 8080
    --service-account "${SA}"
    --set-env-vars "${RUNTIME_ENV_STR}"
    --update-secrets "${RUNTIME_SECRETS}"
    --memory 4Gi
    --cpu 2
    --min-instances 1
    --max-instances 3
    --concurrency 80
    --timeout 120
    --execution-environment gen2
)

if [[ "${DRY_RUN}" == true ]]; then
  echo "=== DRY RUN — would execute: ==="
  printf '  %s \\\n' "${DEPLOY_CMD[@]}"
  echo ""
  echo "=== DRY RUN complete — no changes made ==="
  exit 0
fi

echo "=== Deploying ${SERVICE} to Cloud Run ==="
"${DEPLOY_CMD[@]}"

echo ""
echo "=== Routing 100% traffic to latest revision ==="
gcloud run services update-traffic "${SERVICE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --to-latest

# Fetch and display the deployed URL.
SERVICE_URL=$(gcloud run services describe "${SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(status.url)")

echo ""
echo "=== ${SERVICE} deployed ==="
echo "  URL: ${SERVICE_URL}"
echo ""
echo "=== Smoke test ==="
echo "  curl -s '${SERVICE_URL}/health' | python3 -m json.tool"
echo "  curl -s '${SERVICE_URL}/liveness'"
echo "  curl -s '${SERVICE_URL}/readiness'"
echo ""
echo "=== T+10min verification (post-launch gate per SSOT) ==="
echo "  gcloud run services describe ${SERVICE} \\"
echo "    --region=${REGION} --project=${PROJECT_ID} \\"
echo "    --format='value(status.conditions[0].status)'"
echo "  # Expected: True"
echo ""
echo "=== 24h soak criterion ==="
echo "  /health → healthy:true   for 24 consecutive hours"
echo "  Feature parquets in bucket every N seconds per active feature_group"
echo "  No FAILED events in alerting-service for ${SERVICE}"
echo "  No manifest gaps"
