#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Deploy a DeFi Cloud Run service (execution-service, strategy-service) using the
# declarative YAML spec in deployment-service/configs/cloud-run/<service>.yaml.
#
# OPERATOR-SIDE ONLY — do NOT run from an agent without explicit operator instruction.
# Requires production GCP credentials and the service image already built + pushed to
# Artifact Registry (asia-northeast1-docker.pkg.dev/central-element-323112/unified-trading/).
#
# Usage:
#   bash scripts/cloud-run/deploy.sh --service execution-service [--dry-run]
#   bash scripts/cloud-run/deploy.sh --service strategy-service  [--dry-run]
#
# Options:
#   --service <name>  Required. One of: execution-service, strategy-service.
#   --dry-run         Print the gcloud command without executing it.
#
# Deployment uses `gcloud run services replace` with the declarative YAML spec so all
# service configuration (image, SA, resources, probes, min/max-scale) is expressed in
# the YAML — not in this script. The YAML is the SSOT.
#
# Smoke-test after deploy:
#   SERVICE_URL=$(gcloud run services describe <service> \
#     --region=asia-northeast1 --project=central-element-323112 \
#     --format="value(status.url)")
#   curl -s "${SERVICE_URL}/health"    | python3 -m json.tool
#   curl -s "${SERVICE_URL}/liveness"
#   curl -s "${SERVICE_URL}/readiness"
#
# SSOT pointers:
#   execution-service spec: deployment-service/configs/cloud-run/execution-service.yaml
#   strategy-service spec:  deployment-service/configs/cloud-run/strategy-service.yaml
#   Migration plan: unified-trading-pm/plans/active/defi_compute_gcp_migration_2026_08_08.md

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
REGION="${REGION:-asia-northeast1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIGS_DIR="${SCRIPT_DIR}/../../configs/cloud-run"

SERVICE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --service requires an argument" >&2
        exit 2
      fi
      SERVICE="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      sed -n '/^# Usage/,/^# SSOT/p' "$0" | sed 's/^# //' >&2
      exit 0
      ;;
    *)
      echo "ERROR: unrecognised argument: $1" >&2
      echo "       Usage: bash scripts/cloud-run/deploy.sh --service <name> [--dry-run]" >&2
      exit 2
      ;;
  esac
done

VALID_SERVICES=(execution-service strategy-service)

if [[ -z "$SERVICE" ]]; then
  echo "ERROR: --service is required (one of: ${VALID_SERVICES[*]})" >&2
  exit 2
fi

VALID=false
for s in "${VALID_SERVICES[@]}"; do
  [[ "$s" == "$SERVICE" ]] && VALID=true && break
done
if [[ "$VALID" == false ]]; then
  echo "ERROR: unknown service '$SERVICE' — must be one of: ${VALID_SERVICES[*]}" >&2
  exit 2
fi

YAML_PATH="${CONFIGS_DIR}/${SERVICE}.yaml"
if [[ ! -f "$YAML_PATH" ]]; then
  echo "ERROR: YAML spec not found: $YAML_PATH" >&2
  exit 1
fi

echo "=== Deploy target: ${SERVICE} → ${REGION} (${PROJECT_ID}) ==="
echo "  Spec: ${YAML_PATH}"
echo ""

DEPLOY_CMD=(
  gcloud run services replace "${YAML_PATH}"
  --region "${REGION}"
  --project "${PROJECT_ID}"
  --platform managed
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

SERVICE_URL=$(gcloud run services describe "${SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(status.url)")

echo ""
echo "=== ${SERVICE} deployed ==="
echo "  URL: ${SERVICE_URL}"
echo ""
echo "=== Smoke test ==="
echo "  curl -s '${SERVICE_URL}/health'    | python3 -m json.tool"
echo "  curl -s '${SERVICE_URL}/liveness'"
echo "  curl -s '${SERVICE_URL}/readiness'"
echo ""
echo "=== Verify Ready ==="
echo "  gcloud run services describe ${SERVICE} \\"
echo "    --region=${REGION} --project=${PROJECT_ID} \\"
echo "    --format='value(status.conditions[0].status)'"
echo "  # Expected: True"
