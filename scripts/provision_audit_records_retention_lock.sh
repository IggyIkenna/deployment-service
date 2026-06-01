#!/usr/bin/env bash
# Provision audit-records buckets with immutable Object Retention Lock.
#
# PB-2 (audit_records_pb_1_2_3_pre_cutover_2026_05_13.md Phase 3)
#
# Pre-conditions:
#   - Buckets must already exist (run setup-buckets.py first)
#   - GCP: gcloud authenticated with Storage Admin role
#   - AWS: aws CLI configured with S3:PutObjectLockConfiguration permission
#
# 7 years = 220,752,000 seconds (7 * 365.25 * 24 * 3600)
# GCP: Bucket-level Default Object Retention (retentionPolicy)
# AWS: S3 Object Lock COMPLIANCE mode (whole-bucket)

set -euo pipefail

SEVEN_YEARS_SECS=220752000

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
case "${DEPLOYMENT_ENV}" in
  prod|production)     DEPLOYMENT_ENV_SHORT="prd" ;;
  staging)             DEPLOYMENT_ENV_SHORT="stg" ;;
  development|dev)     DEPLOYMENT_ENV_SHORT="dev" ;;
  *)                   DEPLOYMENT_ENV_SHORT="${DEPLOYMENT_ENV}" ;;
esac

GCP_PROJECT_ID="${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID must be set}"

GCP_BUCKET="trading-audit-records-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
AWS_BUCKET="trading-audit-records-${DEPLOYMENT_ENV_SHORT}-${AWS_ACCOUNT_ID}"  # mirrors cloud-providers.yaml aws.storage.audit-records

echo "=== PB-2: Provisioning audit-records retention lock ==="
echo "  GCP bucket: gs://${GCP_BUCKET}"
echo "  AWS bucket: s3://${AWS_BUCKET}"
echo "  Retention: ${SEVEN_YEARS_SECS}s (7 years)"
echo ""

# --- GCP: enable default object retention (bucket-level) ---
echo "[GCP] Enabling default object retention policy (locked=true)..."
gcloud storage buckets update "gs://${GCP_BUCKET}" \
  --default-storage-class=STANDARD \
  --retention-period="${SEVEN_YEARS_SECS}s" \
  --project="${GCP_PROJECT_ID}" \
  --no-uniform-bucket-level-access 2>/dev/null || \
gcloud storage buckets update "gs://${GCP_BUCKET}" \
  --retention-period="${SEVEN_YEARS_SECS}s" \
  --project="${GCP_PROJECT_ID}"
echo "[GCP] Locking retention policy (irreversible — caution in non-prod)..."
gcloud storage buckets update "gs://${GCP_BUCKET}" \
  --lock-retention-period \
  --project="${GCP_PROJECT_ID}"
echo "[GCP] Done. Verify with:"
echo "  gcloud storage buckets describe gs://${GCP_BUCKET} --format='value(retentionPolicy.retentionPeriod,retentionPolicy.isLocked)'"

# Enable Object Versioning (separate from retention lock — creates immutable version history)
echo "[GCP] Enabling Object Versioning on audit bucket..."
gcloud storage buckets update "gs://${GCP_BUCKET}" \
  --versioning
echo "[GCP] Object Versioning: ENABLED"
echo "  Verify with: gcloud storage buckets describe gs://${GCP_BUCKET} --format='value(versioning)'"

echo ""

# --- AWS: enable S3 Object Lock (must be enabled at bucket creation; otherwise recreate) ---
echo "[AWS] Enabling S3 Object Lock COMPLIANCE mode (7 years)..."
DAYS_7_YEARS=2557  # ceil(7 * 365.25)
aws s3api put-object-lock-configuration \
  --bucket "${AWS_BUCKET}" \
  --object-lock-configuration "{
    \"ObjectLockEnabled\": \"Enabled\",
    \"Rule\": {
      \"DefaultRetention\": {
        \"Mode\": \"COMPLIANCE\",
        \"Days\": ${DAYS_7_YEARS}
      }
    }
  }"
echo "[AWS] Done. Verify with:"
echo "  aws s3api get-object-lock-configuration --bucket ${AWS_BUCKET}"

echo ""
echo "=== PB-2 complete ==="
echo "Retention-lock verification commands printed above."
echo "Expected GCP: retentionPeriod=${SEVEN_YEARS_SECS} isLocked=True"
echo "Expected AWS: Mode=COMPLIANCE Days=${DAYS_7_YEARS}"
