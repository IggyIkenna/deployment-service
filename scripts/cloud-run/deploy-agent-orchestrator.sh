#!/usr/bin/env bash
# Build and deploy agent-orchestrator to Cloud Run (europe-west4).
#
# Mirrors deploy-ui.sh shape; right-sized for an API-only service:
#   - Single region (europe-west4 only; no multi-region fan-out — orchestrator
#     is operator tooling, no global-traffic concern)
#   - No BUILD_ENV_FILE machinery — the Vite dashboard is served by Firebase
#     Hosting (Phase 2 of the deploy plan), so this image is API-only and
#     needs no build-time env bake. Runtime env via --set-env-vars and
#     --update-secrets.
#
# TWO INDEPENDENT ENVIRONMENTS — NO SILENT DEFAULT.
# You must pass `--env=prod` or `--env=uat`. The script exits 2 otherwise.
#
#   | env  | Cloud Run service           | Image tag    | Domain                                            |
#   |------|------------------------------|--------------|---------------------------------------------------|
#   | prod | agent-orchestrator           | :production  | https://agent-orchestrator.odum-research.com      |
#   | uat  | agent-orchestrator-staging   | :uat         | https://agent-orchestrator.staging.odum-research… |
#
# Usage:
#   bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=uat              # local docker build → UAT
#   bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=prod             # local docker build → PROD
#   bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=uat  --cloud     # Cloud Build → UAT
#   bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=prod --cloud     # Cloud Build → PROD
#
# Build speed: cloudbuild-agent-orchestrator.yaml pulls the prior image tag
# and passes --cache-from. Code-only deploys reuse the deps layer.

set -euo pipefail

PROJECT_ID="central-element-323112"
REGION="europe-west4"
SERVICE_PROD="agent-orchestrator"
SERVICE_UAT="agent-orchestrator-staging"
IMAGE_REPO="agent-orchestrator"
IMAGE="europe-west4-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/${IMAGE_REPO}"

USE_CLOUD_BUILD=false
DEPLOY_ENV=""
for arg in "$@"; do
  case "$arg" in
    --cloud) USE_CLOUD_BUILD=true ;;
    --env=*) DEPLOY_ENV="${arg#*=}" ;;
    *)
      echo "ERROR: unrecognised argument: $arg" >&2
      echo "       Usage: bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=prod|uat [--cloud]" >&2
      exit 2
      ;;
  esac
done

case "${DEPLOY_ENV}" in
  prod)
    SERVICE="${SERVICE_PROD}"
    IMAGE_TAG="production"
    PUBLIC_URL="https://agent-orchestrator.odum-research.com"
    ;;
  uat)
    SERVICE="${SERVICE_UAT}"
    IMAGE_TAG="uat"
    PUBLIC_URL="https://agent-orchestrator.staging.odum-research.com"
    ;;
  "")
    echo "ERROR: --env=prod|uat is required. Staging and prod are independent deploys; there is no default." >&2
    echo "       Usage: bash scripts/cloud-run/deploy-agent-orchestrator.sh --env=prod|uat [--cloud]" >&2
    exit 2
    ;;
  *)
    echo "ERROR: unknown --env value: ${DEPLOY_ENV}. Must be one of: prod, uat." >&2
    exit 2
    ;;
esac

echo "=== Deploy target: $(echo "${DEPLOY_ENV}" | tr '[:lower:]' '[:upper:]') (${SERVICE}) — ${PUBLIC_URL} ==="

IMAGE_REF="${IMAGE}:${IMAGE_TAG}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Source repo lives at workspace_root/agent-orchestrator/ (sibling of deployment-service).
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../agent-orchestrator" && pwd)"

if [[ ! -f "${REPO_ROOT}/Dockerfile" ]]; then
  echo "ERROR: Dockerfile not found at ${REPO_ROOT}/Dockerfile" >&2
  exit 1
fi

if [[ "${USE_CLOUD_BUILD}" == true ]]; then
  echo "=== Cloud Build (→ ${IMAGE_REF}) ==="
  gcloud builds submit "${REPO_ROOT}" \
    --config="${SCRIPT_DIR}/cloudbuild-agent-orchestrator.yaml" \
    --substitutions=_IMAGE="${IMAGE_REF}",_PROJECT_ID="${PROJECT_ID}" \
    --timeout=1200 \
    --project="${PROJECT_ID}"
else
  echo "=== Local Docker Build (linux/amd64 → ${IMAGE_REF}) ==="
  gcloud auth configure-docker europe-west4-docker.pkg.dev --quiet 2>/dev/null || true
  docker buildx build \
    --platform linux/amd64 \
    --build-arg "PROJECT_ID=${PROJECT_ID}" \
    -t "${IMAGE_REF}" \
    --load \
    "${REPO_ROOT}"
  echo "=== Pushing image ==="
  docker push "${IMAGE_REF}"
fi

# Runtime env for the Cloud Run service.
# Phase 3 (strict-auth flip): JWT secret + users.json mounted from GCP Secret Manager.
# - ORCHESTRATOR_JWT_SECRET (env var)   ← from Secret Manager
# - /secrets/users.json (file mount)    ← from Secret Manager (ORCHESTRATOR_USERS_JSON points at it)
# - ORCHESTRATOR_ALLOW_ANONYMOUS=false  ← strict mode (all Cloud Run traffic carries
#   X-Forwarded-For from GFE so the ALLOW_ANONYMOUS branch is never hit anyway, but
#   we set it defensively per the P3 plan).
RUNTIME_ENV="ORCHESTRATOR_MODE=live,ORCHESTRATOR_ALLOW_ANONYMOUS=false,ORCHESTRATOR_USERS_JSON=/secrets/users.json"
RUNTIME_SECRETS="ORCHESTRATOR_JWT_SECRET=ORCHESTRATOR_JWT_SECRET:latest,/secrets/users.json=ORCHESTRATOR_USERS_JSON:latest"

echo "=== Deploying to Cloud Run (${SERVICE} @ ${REGION}) ==="
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE_REF}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --platform managed \
  --allow-unauthenticated \
  --port=8080 \
  --set-env-vars "${RUNTIME_ENV}" \
  --update-secrets "${RUNTIME_SECRETS}" \
  --memory=1Gi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=3

echo "=== Routing 100% traffic to latest ==="
gcloud run services update-traffic "${SERVICE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --to-latest

echo "=== Cleaning up old revisions (keep latest) ==="
LATEST=$(gcloud run revisions list \
  --service="${SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(name)" \
  --sort-by="~metadata.creationTimestamp" \
  --limit=1)

gcloud run revisions list \
  --service="${SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(name)" \
  --sort-by="~metadata.creationTimestamp" | tail -n +2 | while read -r rev; do
    if [ -n "$rev" ]; then
      echo "  Deleting old revision: $rev"
      gcloud run revisions delete "$rev" --region="${REGION}" --project="${PROJECT_ID}" --quiet 2>/dev/null || true
    fi
  done

# Fetch the Cloud Run-assigned URL for the just-deployed service.
SERVICE_URL=$(gcloud run services describe "${SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(status.url)")

echo
echo "=== Cloud Run ${SERVICE} updated — active revision: ${LATEST} ==="
echo "  Service URL (raw):    ${SERVICE_URL}"
echo "  Custom domain (P2+):  ${PUBLIC_URL}"
echo
echo "  Smoke test:"
echo "    curl ${SERVICE_URL}/healthz"
echo "    curl ${SERVICE_URL}/health"
echo "    curl ${SERVICE_URL}/readiness"
