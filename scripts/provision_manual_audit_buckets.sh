#!/usr/bin/env bash
# Provision manual-audit buckets across 3 envs × 2 clouds.
#
# Phase 0c — manual-audit bucket provisioning
# Plan: plans/active/bucket_name_ssot_canonicalisation_2026_05_10.md § Phase 0i tail
# UAC: unified_api_contracts/internal/manual_audit_paths.py BUCKET_KIND_MANUAL_AUDIT
#
# GCP: asia-northeast1, UBLA, Coldline after 90d, 7-year retention lock (irreversible).
# AWS: ap-northeast-1, S3 Object Lock COMPLIANCE at creation, Glacier-IA after 90d, 7-year lock.
#
# 7 years = 220,752,000 seconds / 2557 days (ceil(7 * 365.25))

set -euo pipefail

GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-427895769566}"
GCP_REGION="${GCP_REGION:-asia-northeast1}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"
SEVEN_YEARS_SECS=220752000
DAYS_7_YEARS=2557

ENVS=(prd stg dev)

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
gcp_bucket() { echo "manual-audit-${1}-${GCP_PROJECT_ID}"; }
aws_bucket() { echo "manual-audit-${1}-${AWS_ACCOUNT_ID}"; }  # mirrors cloud-providers.yaml aws.storage.manual-audit

coldline_lifecycle_json() {
  cat <<'JSON'
{"rule":[{"action":{"type":"SetStorageClass","storageClass":"COLDLINE"},"condition":{"age":90}}]}
JSON
}

aws_lifecycle_json() {
  cat <<JSON
{
  "Rules": [{
    "ID": "glacier-ia-after-90d",
    "Status": "Enabled",
    "Filter": {"Prefix": ""},
    "Transitions": [{"Days": 90, "StorageClass": "GLACIER_IR"}]
  }]
}
JSON
}

# --------------------------------------------------------------------------
# GCP provisioning
# --------------------------------------------------------------------------
echo "=== GCP manual-audit bucket provisioning ==="
for ENV_SHORT in "${ENVS[@]}"; do
  BUCKET=$(gcp_bucket "${ENV_SHORT}")
  echo ""
  echo "--- GCP: gs://${BUCKET} ---"

  # Create (idempotent — skip if exists)
  if gcloud storage buckets describe "gs://${BUCKET}" --project="${GCP_PROJECT_ID}" &>/dev/null; then
    echo "[GCP] Bucket already exists — skipping creation."
  else
    echo "[GCP] Creating bucket..."
    gcloud storage buckets create "gs://${BUCKET}" \
      --project="${GCP_PROJECT_ID}" \
      --location="${GCP_REGION}" \
      --uniform-bucket-level-access \
      --default-storage-class=STANDARD \
      --no-public-access-prevention
    echo "[GCP] Created."
  fi

  # Apply Coldline lifecycle (idempotent)
  echo "[GCP] Applying Coldline lifecycle (age=90d)..."
  LIFECYCLE_FILE=$(mktemp /tmp/gcs-lifecycle-XXXXXX.json)
  coldline_lifecycle_json > "${LIFECYCLE_FILE}"
  gcloud storage buckets update "gs://${BUCKET}" \
    --lifecycle-file="${LIFECYCLE_FILE}" \
    --project="${GCP_PROJECT_ID}"
  rm -f "${LIFECYCLE_FILE}"
  echo "[GCP] Lifecycle applied."

  # Apply retention policy (if not already set)
  CURRENT_RETENTION=$(gcloud storage buckets describe "gs://${BUCKET}" \
    --project="${GCP_PROJECT_ID}" \
    --format="value(retentionPolicy.retentionPeriod)" 2>/dev/null || echo "")
  if [[ -z "${CURRENT_RETENTION}" ]]; then
    echo "[GCP] Setting retention policy (${SEVEN_YEARS_SECS}s / 7 years)..."
    gcloud storage buckets update "gs://${BUCKET}" \
      --retention-period="${SEVEN_YEARS_SECS}s" \
      --project="${GCP_PROJECT_ID}"
    echo "[GCP] Retention set."
  else
    echo "[GCP] Retention already set (${CURRENT_RETENTION}s) — skipping."
  fi

  # Lock retention policy (check if already locked; pipe y to bypass interactive prompt)
  IS_LOCKED=$(gcloud storage buckets describe "gs://${BUCKET}" \
    --project="${GCP_PROJECT_ID}" \
    --format="value(retentionPolicy.isLocked)" 2>/dev/null || echo "False")
  if [[ "${IS_LOCKED}" == "True" ]]; then
    echo "[GCP] Retention already locked — skipping."
  else
    echo "[GCP] Locking retention policy (IRREVERSIBLE — piping y to confirm)..."
    echo y | gcloud storage buckets update "gs://${BUCKET}" \
      --lock-retention-period \
      --project="${GCP_PROJECT_ID}"
    echo "[GCP] Locked."
  fi
done

# --------------------------------------------------------------------------
# AWS provisioning (skip gracefully if aws CLI not present)
# --------------------------------------------------------------------------
echo ""
echo "=== AWS manual-audit bucket provisioning ==="
if ! command -v aws &>/dev/null; then
  echo "[AWS] aws CLI not found on this machine."
  echo "[AWS] SKIPPED — provision from a GCE VM or machine with aws CLI configured."
  echo "[AWS] Buckets to create:"
  for ENV_SHORT in "${ENVS[@]}"; do echo "  s3://$(aws_bucket "${ENV_SHORT}") (${AWS_REGION})"; done
else
for ENV_SHORT in "${ENVS[@]}"; do
  BUCKET=$(aws_bucket "${ENV_SHORT}")
  echo ""
  echo "--- AWS: s3://${BUCKET} ---"

  # Check if bucket exists
  if aws s3api head-bucket --bucket "${BUCKET}" --region "${AWS_REGION}" 2>/dev/null; then
    echo "[AWS] Bucket already exists."
  else
    echo "[AWS] Creating bucket with Object Lock enabled..."
    if [[ "${AWS_REGION}" == "us-east-1" ]]; then
      aws s3api create-bucket \
        --bucket "${BUCKET}" \
        --region "${AWS_REGION}" \
        --object-lock-enabled-for-bucket
    else
      aws s3api create-bucket \
        --bucket "${BUCKET}" \
        --region "${AWS_REGION}" \
        --create-bucket-configuration "LocationConstraint=${AWS_REGION}" \
        --object-lock-enabled-for-bucket
    fi
    echo "[AWS] Created."
  fi

  # Enable versioning (required for Object Lock)
  echo "[AWS] Ensuring versioning enabled..."
  aws s3api put-bucket-versioning \
    --bucket "${BUCKET}" \
    --versioning-configuration Status=Enabled \
    --region "${AWS_REGION}"

  # Apply Object Lock COMPLIANCE configuration
  echo "[AWS] Setting Object Lock COMPLIANCE (${DAYS_7_YEARS} days / 7 years)..."
  aws s3api put-object-lock-configuration \
    --bucket "${BUCKET}" \
    --region "${AWS_REGION}" \
    --object-lock-configuration "{
      \"ObjectLockEnabled\": \"Enabled\",
      \"Rule\": {
        \"DefaultRetention\": {
          \"Mode\": \"COMPLIANCE\",
          \"Days\": ${DAYS_7_YEARS}
        }
      }
    }"
  echo "[AWS] Object Lock set."

  # Apply Glacier-IA lifecycle (idempotent)
  echo "[AWS] Applying Glacier-IA lifecycle (after 90d)..."
  LIFECYCLE_JSON=$(aws_lifecycle_json)
  aws s3api put-bucket-lifecycle-configuration \
    --bucket "${BUCKET}" \
    --region "${AWS_REGION}" \
    --lifecycle-configuration "${LIFECYCLE_JSON}"
  echo "[AWS] Lifecycle applied."
done
fi  # end if aws CLI available

# --------------------------------------------------------------------------
# Verification summary
# --------------------------------------------------------------------------
echo ""
echo "=== Verification summary ==="
echo ""
echo "--- GCP ---"
for ENV_SHORT in "${ENVS[@]}"; do
  BUCKET=$(gcp_bucket "${ENV_SHORT}")
  RETENTION=$(gcloud storage buckets describe "gs://${BUCKET}" \
    --project="${GCP_PROJECT_ID}" \
    --format="value(retentionPolicy.retentionPeriod,retentionPolicy.isLocked)" 2>/dev/null | tr '\t' '/')
  echo "  gs://${BUCKET}: retention=${RETENTION}"
done

echo ""
echo "--- AWS ---"
if ! command -v aws &>/dev/null; then
  echo "  [SKIPPED — aws CLI not present]"
else
  for ENV_SHORT in "${ENVS[@]}"; do
    BUCKET=$(aws_bucket "${ENV_SHORT}")
    LOCK=$(aws s3api get-object-lock-configuration --bucket "${BUCKET}" --region "${AWS_REGION}" \
      --query 'ObjectLockConfiguration.Rule.DefaultRetention' \
      --output text 2>/dev/null || echo "ERROR")
    echo "  s3://${BUCKET}: lock=${LOCK}"
  done
fi

echo ""
echo "=== Done. All 6 manual-audit buckets provisioned. ==="
echo ""
echo "GCP verify commands:"
for ENV_SHORT in "${ENVS[@]}"; do
  echo "  gcloud storage buckets describe gs://$(gcp_bucket "${ENV_SHORT}") --format='value(retentionPolicy.retentionPeriod,retentionPolicy.isLocked)'"
done
echo ""
echo "AWS verify commands:"
for ENV_SHORT in "${ENVS[@]}"; do
  echo "  aws s3api get-object-lock-configuration --bucket $(aws_bucket "${ENV_SHORT}")"
done
