#!/usr/bin/env bash
# Launch a GCE VM running gcs_migration_bundle_2026_05_08.py --apply for one
# year-slice of one asset_group bucket. One VM per (asset_group, year) —
# no two VMs ever touch the same parquet (prefix-slice isolation).
#
# Per-VM shard isolation (MANIFEST_PER_VM_SHARDS=true + unique VM_NAME) is
# MANDATORY per workspace concurrency rule and QG STEP 5.66.
#
# VM_PREFIX_TO_BUCKET: gcs-migration-bundle-{ag}- registered in
# vm_zombie_watchdog.py (2026-05-19 slot 1).
#
# Plan: plans/active/gcs_migration_bundle_pipeline_mode_2026_05_08.md Phase 3.
#
# Usage:
#   bash launch-gcs-migration-bundle-vm.sh --asset-group cefi --year 2024
#   bash launch-gcs-migration-bundle-vm.sh --asset-group cefi --year 2024 --dry-run
#   bash launch-gcs-migration-bundle-vm.sh --asset-group defi --year 2023 --workers 16
#
# Fleet invocation (all CeFi years 2020-2026):
#   for Y in 2020 2021 2022 2023 2024 2025 2026; do
#     bash launch-gcs-migration-bundle-vm.sh --asset-group cefi --year $Y
#   done
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-8}"
WORKERS="${WORKERS:-8}"
APPLY=true          # default apply; override with --dry-run
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"

ASSET_GROUP=""
YEAR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --year)        YEAR="$2"; shift 2 ;;
    --workers)     WORKERS="$2"; shift 2 ;;
    --dry-run)     APPLY=false; shift ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${ASSET_GROUP}" ]]; then
  echo "ERROR: --asset-group required (cefi|defi|tradfi|sports|prediction)" >&2; exit 1
fi
if [[ -z "${YEAR}" ]]; then
  echo "ERROR: --year required (e.g. 2024)" >&2; exit 1
fi

case "${ASSET_GROUP}" in
  cefi|defi|tradfi|sports|prediction) ;;
  *) echo "ERROR: --asset-group must be one of cefi|defi|tradfi|sports|prediction (got: ${ASSET_GROUP})" >&2; exit 1 ;;
esac

case "${DEPLOYMENT_ENV}" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: ${DEPLOYMENT_ENV})" >&2; exit 1 ;;
esac

BUCKET="market-data-tick-${ASSET_GROUP}-${PROJECT_ID}"
PREFIX_SLICE="raw_tick_data/by_date/day=${YEAR}-"
RUN_TS=$(date -u +%Y%m%d-%H%M%S)
VM_NAME="gcs-migration-bundle-${ASSET_GROUP}-${YEAR}-${RUN_TS}"
STAGING_GCS="gs://deployment-scripts-${PROJECT_ID}/migration-bundle/staging/${VM_NAME}"
LOG_GCS="gs://deployment-scripts-${PROJECT_ID}/vm-logs/${VM_NAME}/run.log"

echo "============================================================"
echo "GCS Migration Bundle VM"
echo "  Asset group:  ${ASSET_GROUP}"
echo "  Year:         ${YEAR}"
echo "  Bucket:       ${BUCKET}"
echo "  Prefix slice: ${PREFIX_SLICE}"
echo "  Apply mode:   ${APPLY}"
echo "  VM name:      ${VM_NAME}"
echo "  Workers:      ${WORKERS}"
echo "  Zone:         ${ZONE}"
echo "============================================================"

# Upload the migration script to GCS staging
MIGRATION_SCRIPT="${WORKSPACE_ROOT}/unified-trading-pm/scripts/migration/gcs_migration_bundle_2026_05_08.py"
if [[ ! -f "${MIGRATION_SCRIPT}" ]]; then
  echo "ERROR: migration script not found: ${MIGRATION_SCRIPT}" >&2; exit 1
fi

echo "Uploading migration script to ${STAGING_GCS}..."
gcloud storage cp "${MIGRATION_SCRIPT}" "${STAGING_GCS}/gcs_migration_bundle_2026_05_08.py"

APPLY_FLAG=""
if [[ "${APPLY}" == "true" ]]; then
  APPLY_FLAG="--apply"
fi

STARTUP_FILE=$(mktemp)
cat > "${STARTUP_FILE}" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/root
export PATH="/root/.local/bin:\$PATH"
export PYTHONUNBUFFERED=1
export VM_NAME="${VM_NAME}"
export MANIFEST_PER_VM_SHARDS=true

exec > >(tee /var/log/gcs-migration-bundle.log) 2>&1

LOG_GCS="${LOG_GCS}"

(
  while true; do
    sleep 30
    gcloud storage cp /var/log/gcs-migration-bundle.log "\${LOG_GCS}" 2>/dev/null || true
  done
) &
LOG_PID=\$!
trap "kill \${LOG_PID} 2>/dev/null; gcloud storage cp /var/log/gcs-migration-bundle.log \${LOG_GCS} 2>/dev/null || true" EXIT

echo "=== GCS Migration Bundle VM: ${ASSET_GROUP} / ${YEAR} ==="
echo "VM: ${VM_NAME}"
echo "Bucket: ${BUCKET}"
echo "Prefix slice: ${PREFIX_SLICE}"
echo "Apply: ${APPLY}"
date

WORK_DIR=/tmp/migration-bundle

# Install Python + uv
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev curl 2>/dev/null || {
  apt-get install -yqq software-properties-common
  add-apt-repository -y ppa:deadsnakes/ppa
  apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
}
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

mkdir -p "\${WORK_DIR}"
python3.13 -m venv "\${WORK_DIR}/.venv"
source "\${WORK_DIR}/.venv/bin/activate"

# GCS wheel cache — skip compilation for heavy C extensions
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://deployment-scripts-${PROJECT_ID}/wheels/py313-linux-x86_64"
mkdir -p "\${WHEEL_CACHE}"
gsutil -m -q cp "\${WHEEL_GCS}/*.whl" "\${WHEEL_CACHE}/" 2>/dev/null || true
uv pip install --find-links "\${WHEEL_CACHE}" pyarrow gcsfs google-cloud-storage

# Install UAC
echo "=== Installing UAC ==="
gcloud storage cp "gs://deployment-scripts-${PROJECT_ID}/code/unified-api-contracts-code.tar.gz" "\${WORK_DIR}/uac.tar.gz"
mkdir -p "\${WORK_DIR}/unified-api-contracts"
tar xzf "\${WORK_DIR}/uac.tar.gz" -C "\${WORK_DIR}/unified-api-contracts"
uv pip install --find-links "\${WHEEL_CACHE}" -e "\${WORK_DIR}/unified-api-contracts"

# Install UTL
echo "=== Installing UTL ==="
gcloud storage cp "gs://deployment-scripts-${PROJECT_ID}/code/unified-trading-library-code.tar.gz" "\${WORK_DIR}/utl.tar.gz"
mkdir -p "\${WORK_DIR}/unified-trading-library"
tar xzf "\${WORK_DIR}/utl.tar.gz" -C "\${WORK_DIR}/unified-trading-library"
uv pip install --find-links "\${WHEEL_CACHE}" -e "\${WORK_DIR}/unified-trading-library"

# Download migration script
gcloud storage cp "${STAGING_GCS}/gcs_migration_bundle_2026_05_08.py" "\${WORK_DIR}/gcs_migration_bundle_2026_05_08.py"

echo ""
echo "=== Starting Migration ==="
date
MIGRATION_EXIT=0
python3 "\${WORK_DIR}/gcs_migration_bundle_2026_05_08.py" \\
  --bucket "${BUCKET}" \\
  --prefix-slice "${PREFIX_SLICE}" \\
  --workers "${WORKERS}" \\
  --rescan-asset-group "${ASSET_GROUP}" \\
  --skip-rescan \\
  ${APPLY_FLAG} || MIGRATION_EXIT=\$?

echo ""
if [ "\${MIGRATION_EXIT}" -eq 0 ]; then
  echo "=== Migration Complete: ${ASSET_GROUP} / ${YEAR} ==="
else
  echo "=== Migration FAILED: ${ASSET_GROUP} / ${YEAR} exit_code=\${MIGRATION_EXIT} ==="
fi
date
gcloud storage cp /var/log/gcs-migration-bundle.log "\${LOG_GCS}" 2>/dev/null || true
shutdown -h now
STARTUP_EOF

echo "Launching VM..."
# Delete stale VM if exists
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"

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
  --labels="purpose=gcs-migration-bundle,asset-group=${ASSET_GROUP},year=${YEAR},env=${DEPLOYMENT_ENV}"

rm -f "${STARTUP_FILE}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor logs: gsutil cat ${LOG_GCS} | tail -20"
echo "  SSH:          gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/gcs-migration-bundle.log"
echo "  Status:       gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "============================================================"
