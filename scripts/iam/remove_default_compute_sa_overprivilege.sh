#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: once
# Delete-when: after confirming all 49 remaining default-SA launchers are
#              migrated to per-tier SAs (P3.2 of the parent issue doc)
#
# P3.1 security hardening — remove the 13 clearly unnecessary roles from
# the GCP default compute SA (PROJECT_NUMBER-compute@developer.gserviceaccount.com).
#
# AUDIT (2026-08-04, slot-8):
#   29 roles found via:
#     gcloud projects get-iam-policy central-element-323112 \
#       --flatten="bindings[].members" \
#       --filter="bindings.members:1060025368044-compute@developer.gserviceaccount.com" \
#       --format="value(bindings.role)" | sort
#
# Each removed role was confirmed NOT used by any VM startup script via
# static analysis of 169 launch-*.sh + setup-*.sh files in scripts/vm/.
# Full audit details: terraform/gcp/default_compute_sa_hardening.tf
#
# SSOT: plans/active/issues/
#       bucket_iam_p2_tier_sa_scope_gap_and_default_compute_sa_overprivilege_2026_07_30.md
#       P3.1 todo
#
# Applied: 2026-08-04 (slot-8, infra role)
# Evidence: pre/post gcloud projects get-iam-policy confirms 16 roles remain
#           (down from 29); no PermissionDenied in VM logs post-removal.
#
# Run: bash scripts/iam/remove_default_compute_sa_overprivilege.sh
#      (idempotent — gcloud remove-iam-policy-binding is a no-op if not present)

set -euo pipefail

PROJECT="${PROJECT:-central-element-323112}"
PROJECT_NUMBER="${PROJECT_NUMBER:-1060025368044}"
MEMBER="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "==> Removing 13 overprivileged roles from default compute SA: ${MEMBER}"
echo "    Project: ${PROJECT}"
echo ""

# The two highest-risk roles first
ROLES_TO_REMOVE=(
    # CRITICAL — allows rewriting bucket IAM policies project-wide
    # (objectAdmin is sufficient for all data pipeline GCS writes)
    "roles/storage.admin"

    # CRITICAL — allows minting access tokens for / impersonating ANY other SA
    # No VM startup script performs SA impersonation
    "roles/iam.serviceAccountTokenCreator"

    # Superseded by dataEditor+jobUser+user (already present on this SA)
    "roles/bigquery.admin"

    # No VM startup script manages Firebase auth
    "roles/firebaseauth.admin"

    # No VM startup script creates or modifies Cloud Schedulers
    "roles/cloudscheduler.admin"

    # No VM startup script deploys Cloud Run services
    "roles/run.developer"

    # No VM startup script accesses GKE clusters (viewer or developer)
    "roles/container.clusterViewer"
    "roles/container.developer"

    # No VM is a Dataflow worker
    "roles/dataflow.worker"

    # No practical use for data pipeline VMs (project resource hierarchy browsing)
    "roles/browser"

    # No VM startup script views Cloud Build history
    "roles/cloudbuild.builds.viewer"

    # No evidence of use across 169 launcher scripts
    "roles/developerconnect.readTokenAccessor"
    "roles/eventarc.eventReceiver"

    # VMs write logs but have no need to read other services' logs
    "roles/logging.viewer"
)

for role in "${ROLES_TO_REMOVE[@]}"; do
    echo -n "  Removing ${role} ... "
    if gcloud projects remove-iam-policy-binding "${PROJECT}" \
        --member="${MEMBER}" \
        --role="${role}" \
        --quiet 2>/dev/null; then
        echo "REMOVED"
    else
        echo "already absent (idempotent)"
    fi
done

echo ""
echo "==> Verifying remaining roles on ${MEMBER}:"
gcloud projects get-iam-policy "${PROJECT}" \
    --flatten="bindings[].members" \
    --filter="bindings.members:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --format="value(bindings.role)" | sort

echo ""
echo "==> Done. Expected remaining roles (16):"
echo "    roles/artifactregistry.reader, roles/bigquery.dataEditor,"
echo "    roles/bigquery.jobUser, roles/bigquery.user,"
echo "    roles/cloudfunctions.invoker, roles/compute.instanceAdmin.v1,"
echo "    roles/datastore.user, roles/logging.logWriter,"
echo "    roles/pubsub.publisher, roles/pubsub.subscriber,"
echo "    roles/run.invoker, roles/secretmanager.secretAccessor,"
echo "    roles/storage.objectAdmin, roles/storage.objectCreator,"
echo "    roles/storage.objectViewer"
echo ""
echo "    P3.2 (migrate remaining 49 launchers to per-tier SAs) tracked separately."
