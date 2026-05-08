#!/usr/bin/env bash
# Launch GCE VM for MTDS category-specific backfill (CeFi, DeFi, TradFi, etc.)
#
# Generalised from launch_mtds_backfill_vm.sh (sports-specific).
# Stages codebase tarball to the category-specific GCS bucket, launches VM.
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/common/launch_mtds_category_backfill_vm.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Also fills the `_SERVICE_LAUNCHER_SCRIPTS["market-tick-data-service"]` entry
# in `deployment-api/deployment_api/services/deploy_missing.py` (was missing
# on disk; Deploy-Missing UI button silently broke for MTDS).
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Usage:
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --start 2024-04-05 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group DEFI --start 2024-04-05 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group TRADFI --start 2026-03-29 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --dry-run
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
ASSET_GROUP=""
START_DATE=""
END_DATE=""
TIER=""
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-7}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
VM_NAME_OVERRIDE=""
VENUES=""
DATA_TYPES=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --tier) TIER="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --venues) VENUES="$2"; shift 2 ;;
    --data-types) DATA_TYPES="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$ASSET_GROUP" ]]; then
  echo "ERROR: --asset-group is required (CEFI, DEFI, TRADFI, SPORTS, PREDICTION)"
  exit 1
fi

CATEGORY_LOWER=$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')

# Resolve GCS bucket for the category
GCS_BUCKET="gs://market-data-tick-${CATEGORY_LOWER}-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/mtds_backfill"
TARBALL_NAME="mtds_backfill_codebase_${CATEGORY_LOWER}.tar.gz"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-${CATEGORY_LOWER}-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "MTDS Category Backfill VM Launcher"
echo "  Category:  ${ASSET_GROUP}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Tier:      ${TIER:-N/A}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Venues:    ${VENUES:-all}"
echo "  DataTypes: ${DATA_TYPES:-all}"
echo "  Bucket:    ${GCS_BUCKET}"
echo "  VM:        ${VM_NAME}"
echo "  Workspace: ${WORKSPACE_ROOT}"
echo "============================================================"

# ---------- Step 1: Package codebase ----------
echo ""
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "unified-cloud-interface"
  "unified-config-interface"
  "unified-trading-library"
  "unified-features-interface"
  "unified-reference-data-interface"
  "market-tick-data-service"
)

TARBALL_PATH="/tmp/${TARBALL_NAME}"

if ! $DRY_RUN; then
  echo "  Creating tarball from workspace: ${WORKSPACE_ROOT}"
  STAGING_DIR=$(mktemp -d)
  for repo in "${REPOS[@]}"; do
    REPO_PATH="${WORKSPACE_ROOT}/${repo}"
    if [[ ! -d "$REPO_PATH" ]]; then
      echo "  WARNING: ${repo} not found at ${REPO_PATH}, skipping"
      continue
    fi
    echo "  Syncing ${repo}..."
    mkdir -p "${STAGING_DIR}/${repo}"
    rsync -a \
      --filter=':- .gitignore' \
      --exclude='.git' \
      --exclude='.venv*' \
      --exclude='__pycache__' \
      --exclude='*.egg-info' \
      --exclude='node_modules' \
      --exclude='.mypy_cache' \
      --exclude='.pytest_cache' \
      "${REPO_PATH}/" "${STAGING_DIR}/${repo}/"
  done

  echo "  Compressing..."
  (cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
  TARBALL_SIZE=$(du -h "${TARBALL_PATH}" | cut -f1)
  echo "  Tarball: ${TARBALL_PATH} (${TARBALL_SIZE})"
  rm -rf "${STAGING_DIR}"
fi

# ---------- Step 2: Upload to GCS ----------
echo ""
echo "=== Step 2: Uploading to GCS ==="
GCS_TARBALL="${GCS_STAGING}/${TARBALL_NAME}"
GCS_SCRIPT="${GCS_STAGING}/vm_mtds_backfill.sh"

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Uploading backfill script..."
  gsutil -q cp "${SCRIPT_DIR}/vm_mtds_backfill.sh" "${GCS_SCRIPT}"
  echo "  Done."
  rm -f "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball + script to ${GCS_STAGING}/"
fi

# ---------- Step 3: Launch VM ----------
echo ""
echo "=== Step 3: Launching VM ==="

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

TIER_VM_FLAG=""
if [[ -n "$TIER" ]]; then
  TIER_VM_FLAG="--tier ${TIER}"
fi

VENUES_VM_FLAG=""
if [[ -n "$VENUES" ]]; then
  VENUES_VM_FLAG="--venues \"${VENUES}\""
fi

DATA_TYPES_VM_FLAG=""
if [[ -n "$DATA_TYPES" ]]; then
  DATA_TYPES_VM_FLAG="--data-types \"${DATA_TYPES}\""
fi

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/mtds_backfill
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/mtds-backfill.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"

# --- Streaming log upload (every 60s) ---
LOG_GCS_PATH="${GCS_STAGING}/logs/${VM_NAME}.log"
(
  while true; do
    sleep 60
    gsutil -q cp /var/log/mtds-backfill.log "\${LOG_GCS_PATH}" 2>/dev/null || true
  done
) &
LOG_STREAMER_PID=\$!
trap "kill \${LOG_STREAMER_PID} 2>/dev/null; gsutil -q cp /var/log/mtds-backfill.log \${LOG_GCS_PATH}" EXIT

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Category: ${ASSET_GROUP}"
echo "  Range:    ${START_DATE} → ${END_DATE}"
echo "  Tier:     ${TIER:-N/A}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase + script from GCS
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_mtds_backfill.sh
chmod +x \${WORK_DIR}/vm_mtds_backfill.sh

# Unpack tarball
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run backfill
bash \${WORK_DIR}/vm_mtds_backfill.sh \\
  --asset-group ${ASSET_GROUP} \\
  ${TIER_VM_FLAG} \\
  --start ${START_DATE} \\
  --end ${END_DATE} \\
  --chunk-size ${CHUNK_SIZE} \\
  ${FORCE_FLAG} \\
  ${VENUES_VM_FLAG} \\
  ${DATA_TYPES_VM_FLAG} \\
  --work-dir \${WORK_DIR}

echo "Backfill complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

echo "--- ${VM_NAME}: ${ASSET_GROUP} ${START_DATE} → ${END_DATE} ---"

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  rm "$STARTUP_FILE"
else
  echo "  Creating VM..."
  # Delete existing VM if present (from previous run)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --metadata-from-file=startup-script="${STARTUP_FILE}"

  rm "$STARTUP_FILE"
  echo "  VM created: ${VM_NAME}"
  echo ""
  echo "  Monitor: gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/mtds-backfill.log"
  echo "  Logs:    gsutil cat ${GCS_STAGING}/logs/${VM_NAME}.log"
fi

echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
