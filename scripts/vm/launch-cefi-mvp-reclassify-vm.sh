#!/usr/bin/env bash
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after cefi_universe_capture_rule_2026_06_23.md Phase C --apply verified
# Launch a GCE VM to run the CeFi MVP-universe manifest reclassification
# (Phase C of cefi_universe_capture_rule_2026_06_23.md) in-region, off the
# operator's local connection (HARD RULE: manifest-index rewrites never run
# from a local machine, see codex/05-infrastructure/vm-launcher-runbook.md
# § heavy-I/O rule).
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Mirrors launch-cefi-migration-vm.sh's VM_TASK=canonical-migration wiring.
# Pre-condition: run `bash create-code-tarballs.sh --asset-group CEFI` first
# so the tarball carries the CURRENT unified-api-contracts MVP_SCOPE_CONFIG_VERSION
# (the reclassification script evaluates every row against whatever predicate
# ships in the tarball — a stale tarball would silently reclassify against a
# stale rule).
#
# Runs (dry-run default unless --apply is passed):
#   market-tick-data-service/scripts/reclassify_cefi_manifest_mvp_universe_2026_06_23.py
#
# Usage:
#   bash launch-cefi-mvp-reclassify-vm.sh              # dry-run (default, writes nothing)
#   bash launch-cefi-mvp-reclassify-vm.sh --apply       # gated real run (snapshot + write)
#   bash launch-cefi-mvp-reclassify-vm.sh --dry-run     # print launch plan only, no VM created
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-8}"
VM_NAME="mtds-migrate-cefi-mvp-reclassify"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
APPLY=false
PLAN_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)   APPLY=true; shift ;;
    --dry-run) PLAN_ONLY=true; shift ;;
    --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone)    ZONE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
SCRIPT_CMD="python scripts/reclassify_cefi_manifest_mvp_universe_2026_06_23.py"
if $APPLY; then
  SCRIPT_CMD="${SCRIPT_CMD} --apply"
fi

echo "============================================================"
echo "CeFi MVP-Universe Manifest Reclassification VM (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VM:       ${VM_NAME}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Mode:     $([ "$APPLY" = true ] && echo APPLY || echo DRY-RUN)"
echo "  Command:  ${SCRIPT_CMD}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if $PLAN_ONLY; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=canonical-migration"
  echo "  VM_MIGRATION_CMD=${SCRIPT_CMD}"
  exit 0
fi

echo "Launching VM..."
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=canonical-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${SCRIPT_CMD}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

lc_verify_tarball_freshness "$CODE_BUCKET" \
    market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT_ID}")" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=cefi-mvp-reclassify,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/canonical-migration.log"
echo "  SSH:          gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
echo "============================================================"
