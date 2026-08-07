#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# deploy-traffic-pin-bridge.sh — Build and deploy the traffic-pin-to-slack-bridge
# Cloud Run service that forwards Cloud Monitoring traffic-pin alerts to Slack #ci-failures.
#
# Architecture:
#   canary-deploy.sh rollback
#     → Cloud Logging (cloud-run-traffic-pin-alert, CRITICAL)
#       → Cloud Monitoring log-based alert policy
#         → Pub/Sub topic (cloud-monitoring-traffic-pin-alerts)
#           → THIS SERVICE push subscription
#             → Slack #ci-failures (webhook from cloud-monitoring-slack-ci-failures-webhook GSM secret)
#
# Prerequisites:
#   1. Pub/Sub topic cloud-monitoring-traffic-pin-alerts must exist (created by setup-traffic-pin-alert.sh)
#   2. GSM secret cloud-monitoring-slack-ci-failures-webhook must be populated with the
#      Slack #ci-failures incoming webhook URL before end-to-end alerts will page.
#      (Service starts and health-checks pass even without the secret, but /push returns 500)
#
# Usage:
#   bash scripts/cloud-run/deploy-traffic-pin-bridge.sh [--project PROJECT] [--region REGION]
#
# Resources managed (idempotent):
#   - Artifact Registry image: unified-trading-system/traffic-pin-slack-bridge:latest
#   - Cloud Run service: traffic-pin-slack-bridge
#   - Pub/Sub push subscription: traffic-pin-slack-bridge-push

set -euo pipefail

PROJECT="${GCLOUD_PROJECT:-central-element-323112}"
REGION="${GCLOUD_REGION:-asia-northeast1}"
SERVICE_NAME="traffic-pin-slack-bridge"
SUBSCRIPTION="traffic-pin-slack-bridge-push"
TOPIC="cloud-monitoring-traffic-pin-alerts"
SA="unified-trading-sa@${PROJECT}.iam.gserviceaccount.com"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/unified-trading-system/${SERVICE_NAME}:latest"

while [ $# -gt 0 ]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --region)  REGION="$2";  shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log() { echo "[deploy-traffic-pin-bridge] $(date +%H:%M:%S) $*"; }

# ── Step 1: Build container image ─────────────────────────────────────────────

log "Building container image via Cloud Build ..."
gcloud builds submit "${SCRIPT_DIR}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --config="${SCRIPT_DIR}/cloudbuild-traffic-pin-bridge.yaml" \
  --substitutions="_IMAGE=${IMAGE}" \
  --timeout=300 --quiet
log "Image built: ${IMAGE}"

# ── Step 2: Deploy Cloud Run service ──────────────────────────────────────────

log "Deploying Cloud Run service ${SERVICE_NAME} ..."
gcloud run deploy "${SERVICE_NAME}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --image="${IMAGE}" \
  --service-account="${SA}" \
  --allow-unauthenticated \
  --memory=256Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=3 \
  --timeout=30 \
  --set-env-vars="GCP_PROJECT=${PROJECT}" \
  --port=8080 \
  --quiet

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --format="value(status.url)" 2>/dev/null)
log "Service deployed: ${SERVICE_URL}"

# ── Step 3: Create push subscription (idempotent) ─────────────────────────────

PUSH_ENDPOINT="${SERVICE_URL}/push"
EXISTING_SUB=$(gcloud pubsub subscriptions describe "${SUBSCRIPTION}" \
  --project="${PROJECT}" --format="value(name)" 2>/dev/null || echo "")

if [ -n "$EXISTING_SUB" ]; then
  log "Push subscription already exists: ${EXISTING_SUB}"
  gcloud pubsub subscriptions modify-push-config "${SUBSCRIPTION}" \
    --project="${PROJECT}" \
    --push-endpoint="${PUSH_ENDPOINT}" --quiet
  log "Push endpoint updated to ${PUSH_ENDPOINT}"
else
  gcloud pubsub subscriptions create "${SUBSCRIPTION}" \
    --project="${PROJECT}" \
    --topic="${TOPIC}" \
    --push-endpoint="${PUSH_ENDPOINT}" \
    --ack-deadline=60 \
    --message-retention-duration=1h \
    --quiet
  log "Push subscription created: ${SUBSCRIPTION} → ${PUSH_ENDPOINT}"
fi

# ── Done ──────────────────────────────────────────────────────────────────────

log ""
log "=== DEPLOY COMPLETE ==="
log "Service:      ${SERVICE_URL}"
log "Push sub:     ${SUBSCRIPTION} → ${PUSH_ENDPOINT}"
log ""
log "NEXT: populate the GSM secret with the Slack #ci-failures webhook URL:"
log "  printf '<webhook-url>' | gcloud secrets versions add \\"
log "    cloud-monitoring-slack-ci-failures-webhook --data-file=- --project=${PROJECT}"
log "Then run the canary: trigger canary-deploy.sh rollback on a UAT service and"
log "confirm the Slack message arrives in #ci-failures."
