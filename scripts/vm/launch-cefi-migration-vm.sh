#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch GCE VM to migrate CeFi tick data instrument_type partitions.
# Runs the migration script from within the same region as GCS (fast).
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/common/launch_cefi_migration_vm.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-us-central1-a}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-8}"
VM_NAME="mtds-migrate-cefi-itype"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Parse --env (Phase 0f env-tier targeting per bucket-naming SSOT).
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

GCS_BUCKET="gs://market-data-tick-cefi-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/migration"

echo "============================================================"
echo "CeFi Instrument Type Migration VM"
echo "  Project: ${PROJECT_ID}"
echo "  Zone:    ${ZONE}"
echo "  Machine: ${MACHINE_TYPE}"
echo "  VM:      ${VM_NAME}"
echo "============================================================"

# Package just MTDS (only need the migration script)
TARBALL_PATH="/tmp/cefi_migration_codebase.tar.gz"
STAGING_DIR=$(mktemp -d)
echo "Packaging market-tick-data-service..."
mkdir -p "${STAGING_DIR}/market-tick-data-service"
rsync -a \
  --include='unified_api_contracts/registry/data/' \
  --include='unified_api_contracts/registry/data/**' \
  --include='unified_api_contracts/canonical/domain/sports/data/' \
  --include='unified_api_contracts/canonical/domain/sports/data/**' \
  --filter=':- .gitignore' \
  --exclude='.git' --exclude='.venv*' --exclude='__pycache__' \
  --exclude='*.egg-info' --exclude='node_modules' \
  "${WORKSPACE_ROOT}/market-tick-data-service/" "${STAGING_DIR}/market-tick-data-service/"
(cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
echo "  Tarball: $(du -h "${TARBALL_PATH}" | cut -f1)"
rm -rf "${STAGING_DIR}"

# Upload
echo "Uploading to GCS..."
gsutil -q cp "${TARBALL_PATH}" "${GCS_STAGING}/cefi_migration_codebase.tar.gz"
rm -f "${TARBALL_PATH}"

# Create VM
STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << 'STARTUP_EOF'
#!/bin/bash
set -euo pipefail
export HOME=/root
export PATH="/root/.local/bin:$PATH"
export PYTHONUNBUFFERED=1

exec > >(tee /var/log/cefi-migration.log) 2>&1

WORK_DIR=/tmp/cefi_migration

# Stream logs to GCS every 30s
LOG_GCS_PATH="PLACEHOLDER_GCS_STAGING/logs/cefi-migration.log"
(
  while true; do
    sleep 30
    gsutil -q cp /var/log/cefi-migration.log "${LOG_GCS_PATH}" 2>/dev/null || true
  done
) &
LOG_PID=$!
trap "kill ${LOG_PID} 2>/dev/null; gsutil -q cp /var/log/cefi-migration.log ${LOG_GCS_PATH}" EXIT

echo "=== CeFi Migration VM ==="
date

# Install Python + uv
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev curl 2>/dev/null || {
  apt-get install -yqq software-properties-common
  add-apt-repository -y ppa:deadsnakes/ppa
  apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
}
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"

# Download and unpack
mkdir -p ${WORK_DIR}
gsutil -q cp PLACEHOLDER_GCS_STAGING/cefi_migration_codebase.tar.gz ${WORK_DIR}/codebase.tar.gz
tar xzf ${WORK_DIR}/codebase.tar.gz -C ${WORK_DIR}
rm ${WORK_DIR}/codebase.tar.gz

# Create venv and install deps
python3.13 -m venv ${WORK_DIR}/.venv
source ${WORK_DIR}/.venv/bin/activate

# GCS wheel cache — skip compilation for C extensions
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://deployment-scripts-central-element-323112/wheels/py313-linux-x86_64"
mkdir -p "$WHEEL_CACHE"
gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
uv pip install --find-links "$WHEEL_CACHE" pyarrow gcsfs pandas google-cloud-storage

# Run migration (writes manifest entries every 100 files)
echo ""
echo "=== Starting Migration ==="
date
python3 ${WORK_DIR}/market-tick-data-service/scripts/migrate_cefi_instrument_types.py \
  --workers 16 --mixed-workers 1 --skip-cleanup

echo ""
echo "=== Migration Complete ==="
echo "Manifest entries written for both pre-existing and newly-migrated files."
date
shutdown -h now
STARTUP_EOF

# Replace placeholder with actual GCS path
sed -i.bak "s|PLACEHOLDER_GCS_STAGING|${GCS_STAGING}|g" "$STARTUP_FILE"

echo "Launching VM..."
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

# O-1 β remediation 2026-05-12: observability invariants for ManifestWriter
# concurrency safety (`MANIFEST_PER_VM_SHARDS=true` + per-VM `VM_NAME`) +
# canonical lifecycle metadata (`VM_SHUTDOWN_ON_COMPLETION=true` — heredoc
# ends in `shutdown -h now`).
METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --metadata="${METADATA}" \
  --metadata-from-file=startup-script="${STARTUP_FILE}" \
  --labels=purpose=cefi-migration,env="${DEPLOYMENT_ENV}"

rm -f "$STARTUP_FILE" "${STARTUP_FILE}.bak"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor: gsutil cat ${GCS_STAGING}/logs/cefi-migration.log"
echo "  SSH:     gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/cefi-migration.log"
echo "============================================================"
