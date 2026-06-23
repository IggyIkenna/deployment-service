#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Push market-tick-data-service:latest to ECR and spot-check manifest consolidation.
#
# Prerequisites (run from a machine with these):
#   - Docker
#   - AWS credentials with ecr:GetAuthorizationToken + ecr:PutImage on the target repo
#   - GCP SA key (for pulling the unified-trading-library base image from GAR)
#
# Usage:
#   GCP_SA_KEY=$(cat /path/to/sa-key.json) \
#   AWS_PROFILE=default \
#   bash deployment-service/scripts/push_market_tick_data_service_to_ecr.sh
#
# Plan: unified-trading-pm/plans/active/aws_manifest_consolidator_scope_2026_05_21.md P0.7

set -euo pipefail

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-427895769566}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"
SERVICE="market-tick-data-service"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${SERVICE}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"

echo "=== 1. ECR login ==="
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "=== 2. GAR login (base image) ==="
if [ -n "${GCP_SA_KEY:-}" ]; then
  echo "${GCP_SA_KEY}" | docker login -u _json_key --password-stdin \
    https://asia-northeast1-docker.pkg.dev
else
  echo "WARNING: GCP_SA_KEY not set — pull of base image may fail if not cached locally"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/../../market-tick-data-service" 2>/dev/null \
  || cd "${WORKSPACE_ROOT:-/home/ubuntu/unified-trading-system-repos}/.tabs/1/market-tick-data-service" && pwd)"

echo "=== 3. Build Docker image from ${SERVICE_DIR} ==="
docker build \
  --platform linux/amd64 \
  --build-arg PROJECT_ID="${GCP_PROJECT_ID}" \
  -t "${ECR_URI}:latest" \
  "${SERVICE_DIR}"

echo "=== 4. Push to ECR ==="
docker push "${ECR_URI}:latest"
echo "Pushed: ${ECR_URI}:latest"

echo "=== 5. Spot-check: wait for first successful consolidation run (<= 90s) ==="
SPOT_BUCKET="unified-trading-market-data-defi-${AWS_ACCOUNT_ID}"
INDEX_KEY="_index/availability_index.parquet"
DEADLINE=$(($(date +%s) + 90))

while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  MTIME=$(aws s3api head-object \
    --bucket "${SPOT_BUCKET}" \
    --key "${INDEX_KEY}" \
    --query 'LastModified' \
    --output text 2>/dev/null || echo "")
  if [ -n "${MTIME}" ]; then
    MTIME_EPOCH=$(date -d "${MTIME}" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "${MTIME%+*}" +%s 2>/dev/null || echo 0)
    AGE=$(( $(date +%s) - MTIME_EPOCH ))
    if [ "${AGE}" -lt 90 ]; then
      echo "✅ Consolidation verified: ${SPOT_BUCKET}/${INDEX_KEY} updated ${AGE}s ago"
      exit 0
    fi
  fi
  echo "  Waiting for consolidation run… (${MTIME:-not yet present})"
  sleep 10
done

echo "WARNING: 90s elapsed without fresh consolidation mtime."
echo "  Check EventBridge rule: aws events list-rules --name-prefix uts-prod-consolidator --region ${AWS_REGION}"
echo "  Check Batch jobs: aws batch list-jobs --job-queue uts-prod-manifest-consolidator --region ${AWS_REGION}"
exit 1
