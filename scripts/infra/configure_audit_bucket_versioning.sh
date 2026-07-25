#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Configure GCS Object Versioning on the audit-records bucket.
#
# Phase 3.A (wallet_treasury_post_cutover_custody_signing_2026_06_01.md Phase 3)
# PB-1/PB-3 compliance: immutable audit trail via GCS object versioning.
#
# Object versioning keeps every version of an object — even after overwrites or
# deletes — making the audit bucket tamper-evident. Combined with the retention
# lock (provision_audit_records_retention_lock.sh), objects cannot be deleted
# before the 7-year window, and versioning ensures previous versions are
# recoverable.
#
# Pre-conditions:
#   - Bucket must already exist (run setup-buckets.py first)
#   - GCP: gcloud authenticated with Storage Admin role
#     (roles/storage.admin on the bucket or project)
#   - DEPLOYMENT_ENV, GCP_PROJECT_ID must be set
#
# Versioning reference:
#   https://cloud.google.com/storage/docs/object-versioning
#
# Usage:
#   export DEPLOYMENT_ENV=prod GCP_PROJECT_ID=central-element-323112
#   bash configure_audit_bucket_versioning.sh
#
# Idempotent: safe to re-run; versioning is already-on is a no-op.

set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
case "${DEPLOYMENT_ENV}" in
  prod|production)     DEPLOYMENT_ENV_SHORT="prd" ;;
  staging)             DEPLOYMENT_ENV_SHORT="stg" ;;
  development|dev)     DEPLOYMENT_ENV_SHORT="dev" ;;
  *)                   DEPLOYMENT_ENV_SHORT="${DEPLOYMENT_ENV}" ;;
esac

GCP_PROJECT_ID="${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"

GCP_BUCKET="trading-audit-records-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"

echo "=== Phase 3.A: Configuring GCS Object Versioning on audit bucket ==="
echo "  Bucket: gs://${GCP_BUCKET}"
echo "  Action: enable versioning + set 7-year retention lock"
echo ""

# --- Step 1: Enable object versioning ---
# `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
# account (a short-lived WIF token in an interactive AO slot can't refresh
# unattended), while `gcloud storage` resolves via ADC, which stays valid. See
# plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
echo "[1/3] Enabling object versioning..."
gcloud storage buckets update "gs://${GCP_BUCKET}" --versioning
echo "      Versioning enabled."

# Verify
VERSIONING_STATUS=$(gcloud storage buckets describe "gs://${GCP_BUCKET}" --format='value(versioning_enabled)' 2>&1)
if [[ "${VERSIONING_STATUS}" == "True" ]]; then
  echo "      Verified: versioning is ON."
else
  echo "ERROR: versioning did not enable. Output: ${VERSIONING_STATUS}" >&2
  exit 1
fi

echo ""

# --- Step 2: Enable retention policy (7-year lock) ---
# This mirrors provision_audit_records_retention_lock.sh but is safe to call
# independently (retention policy is idempotent if already set to same period).
SEVEN_YEARS_SECS=220752000
echo "[2/3] Setting bucket retention policy (${SEVEN_YEARS_SECS}s = 7 years)..."
gcloud storage buckets update "gs://${GCP_BUCKET}" \
  --retention-period="${SEVEN_YEARS_SECS}s" \
  --project="${GCP_PROJECT_ID}"
echo "      Retention policy set."

echo ""

# --- Step 3: Lock retention policy (irreversible in prod) ---
# Skip locking in non-prod to keep tests recoverable.
if [[ "${DEPLOYMENT_ENV_SHORT}" == "prd" ]]; then
  echo "[3/3] Locking retention policy (IRREVERSIBLE — prod only)..."
  gcloud storage buckets update "gs://${GCP_BUCKET}" \
    --lock-retention-period \
    --project="${GCP_PROJECT_ID}"
  echo "      Retention policy locked."
else
  echo "[3/3] Skipping retention lock (non-prod env: ${DEPLOYMENT_ENV_SHORT})."
fi

echo ""
echo "=== Phase 3.A complete ==="
echo "Verify with:"
echo "  gsutil versioning get gs://${GCP_BUCKET}"
echo "  gcloud storage buckets describe gs://${GCP_BUCKET} \\"
echo "    --format='value(retentionPolicy.retentionPeriod,retentionPolicy.isLocked)'"
echo ""
echo "Expected:"
echo "  gs://${GCP_BUCKET}: Enabled"
echo "  retentionPolicy.retentionPeriod=${SEVEN_YEARS_SECS}"
echo "  retentionPolicy.isLocked=True  (prod only)"
