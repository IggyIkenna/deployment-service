#!/usr/bin/env bash
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
# Enable GCP Data Access audit logging for storage.googleapis.com (Cloud Storage).
#
# Filed by tradfi_manifest_row_loss_regression_2026_07_12.md — that investigation had
# to root-cause a 1.02M-row manifest write via Cloud Scheduler/Cloud Run config +
# object custom metadata because GCS Data Access audit logs were confirmed OFF
# project-wide (`cloudaudit.googleapis.com/data_access` had zero storage.googleapis.com
# entries). This script closes that observability gap for future incidents.
#
# Scope note: GCP IAM audit configs (`auditConfigs` on the project IAM policy) are
# keyed by service + log type and apply at the policy level (project/folder/org) —
# there is no native per-bucket scope. So "project-wide" is the only granularity this
# mechanism offers; a narrower "just the market-data buckets" scope is not achievable
# via this API.
#
# Log-type scope: DATA_WRITE only (not DATA_READ, not ADMIN_READ). DATA_READ emits one
# entry per object read/get — this project's pipelines do millions of GCS object reads
# per day across 5 asset groups' backfills/consolidators, so DATA_READ would be high
# volume/cost (see the AWS CloudTrail S3-data-events watch-item in
# codex/05-infrastructure/aws-cloudtrail-cost-optimization-2026-06-20.md: "with millions
# of parquet objects in the data/mirror buckets this can grow fast"). DATA_WRITE volume
# is bounded by actual write throughput (manifest merges, backfill uploads), which is
# far lower than read throughput, and is exactly what this investigation needed
# ("who wrote this object and when").
#
# Pre-conditions:
#   - gcloud authenticated with `resourcemanager.projects.setIamPolicy` (Owner /
#     roles/resourcemanager.projectIamAdmin) on GCP_PROJECT_ID
#   - jq installed
#
# Usage:
#   export GCP_PROJECT_ID=central-element-323112
#   bash enable_gcs_data_access_audit_logging.sh
#
# Idempotent: safe to re-run. If a storage.googleapis.com auditConfig with DATA_WRITE
# already present, this is a no-op (existing config + etag are reused as-is).

set -euo pipefail

GCP_PROJECT_ID="${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
SERVICE="storage.googleapis.com"
LOG_TYPE="DATA_WRITE"

echo "=== Enabling GCS Data Access audit logging (${LOG_TYPE}) on ${GCP_PROJECT_ID} ==="

POLICY_FILE="$(mktemp)"
trap 'rm -f "${POLICY_FILE}"' EXIT

echo "[1/3] Fetching current project IAM policy..."
gcloud projects get-iam-policy "${GCP_PROJECT_ID}" --format=json > "${POLICY_FILE}"

ALREADY_SET=$(jq --arg svc "${SERVICE}" --arg lt "${LOG_TYPE}" \
  '[.auditConfigs[]? | select(.service == $svc) | .auditLogConfigs[]? | select(.logType == $lt)] | length > 0' \
  "${POLICY_FILE}")

if [[ "${ALREADY_SET}" == "true" ]]; then
  echo "[2/3] ${SERVICE} auditConfig already has ${LOG_TYPE} enabled — no-op."
  echo "=== Done (already enabled) ==="
  exit 0
fi

echo "[2/3] Merging ${LOG_TYPE} auditConfig for ${SERVICE} into the policy..."
NEW_POLICY_FILE="$(mktemp)"
trap 'rm -f "${POLICY_FILE}" "${NEW_POLICY_FILE}"' EXIT

jq --arg svc "${SERVICE}" --arg lt "${LOG_TYPE}" '
  (.auditConfigs // []) as $existing
  | (
      if ($existing | any(.service == $svc)) then
        $existing | map(
          if .service == $svc then
            .auditLogConfigs = ((.auditLogConfigs // []) + [{"logType": $lt}])
          else . end
        )
      else
        $existing + [{"service": $svc, "auditLogConfigs": [{"logType": $lt}]}]
      end
    ) as $merged
  | .auditConfigs = $merged
' "${POLICY_FILE}" > "${NEW_POLICY_FILE}"

echo "[3/3] Applying updated IAM policy (uses the fetched etag — fails safely on concurrent edits)..."
gcloud projects set-iam-policy "${GCP_PROJECT_ID}" "${NEW_POLICY_FILE}"

echo ""
echo "=== Done ==="
echo "Verify with:"
echo "  gcloud projects get-iam-policy ${GCP_PROJECT_ID} --format=json | jq '.auditConfigs'"
echo "  gcloud logging read 'protoPayload.serviceName=\"${SERVICE}\" AND logName:\"data_access\"' --project=${GCP_PROJECT_ID} --limit=5"
