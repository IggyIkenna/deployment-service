#!/usr/bin/env bash
# Launch 1 GCE VM for Drift S3 historical data backfill
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/defi/launch_solana_drift_vm.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Solana DeFi (Drift Protocol) — relevant for May-23 carry_staked_basis
# archetype Solana-leg observability.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Downloads Drift's public S3 historical data (orderbook, trades, funding)
# and writes to GCS as parquet under solana-defi-{project_id} bucket.
#
# Usage:
#   bash launch_solana_drift_vm.sh                          # Launch VM (SOL-PERP default)
#   bash launch_solana_drift_vm.sh --dry-run                # Print plan only
#   bash launch_solana_drift_vm.sh --market BTC-PERP        # Specific market
#   bash launch_solana_drift_vm.sh --start 2024-01-01       # Custom start date
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"

# Default: 180 days back (Drift has deep history)
START_DATE="$(date -v-180d +%Y-%m-%d 2>/dev/null || date -d '180 days ago' +%Y-%m-%d)"
END_DATE="$(date +%Y-%m-%d)"
DRIFT_MARKET="SOL-PERP"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --market) DRIFT_MARKET="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

GCS_STAGING="gs://market-data-tick-defi-${PROJECT_ID}/_vm_staging/solana_drift"
TARBALL_NAME="solana_drift_codebase.tar.gz"
VM_NAME="mtds-solana-drift-backfill"

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "market-tick-data-service"
)

# ---------- Step 1: Package codebase ----------
echo "=== Step 1: Packaging codebase ==="

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

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Done."
  rm "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball to ${GCS_STAGING}/"
fi

# ---------- Step 3: Launch VM ----------
echo ""
echo "=== Step 3: Launching VM ==="
echo "  VM:     ${VM_NAME}"
echo "  Range:  ${START_DATE} → ${END_DATE}"
echo "  Market: ${DRIFT_MARKET}"

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/solana_drift
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/solana-drift.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Operation: collect-solana-defi (Drift S3 backfill)"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Market:    ${DRIFT_MARKET}"
date

# Stream logs to GCS every 60s
(while true; do
  sleep 60
  gsutil -q cp /var/log/solana-drift.log \
    ${GCS_STAGING}/logs/${VM_NAME}_\$(date +%Y%m%d).log 2>/dev/null || true
done) &
LOG_PID=\$!

# Install Python 3.13
apt-get update -qq && apt-get install -yqq \
  curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download and unpack codebase
mkdir -p \${WORK_DIR}
echo "Downloading codebase..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
echo "Unpacking..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz

# Create venv and install
echo "Installing Python packages..."
python3.13 -m venv \${WORK_DIR}/.venv
source \${WORK_DIR}/.venv/bin/activate

cd \${WORK_DIR}

# GCS wheel cache — skip compilation for C extensions
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://deployment-scripts-${PROJECT_ID}/wheels/py313-linux-x86_64"
mkdir -p "\$WHEEL_CACHE"
gsutil -m -q cp "\$WHEEL_GCS/*.whl" "\$WHEEL_CACHE/" 2>/dev/null || true

# --no-sources: ignore [tool.uv.sources] path overrides
uv pip install --find-links "\$WHEEL_CACHE" --no-sources \
  -e unified-api-contracts \
  -e unified-trading-library \
  -e market-tick-data-service

# Run Drift S3 backfill — proper service CLI invocation
echo ""
echo "=== Starting Drift S3 backfill ==="
echo "  Range:  ${START_DATE} → ${END_DATE}"
echo "  Market: ${DRIFT_MARKET}"
date

cd market-tick-data-service
python3 -m market_tick_data_service \
  --operation collect-solana-defi \
  --mode batch \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --solana-protocols drift \
  --solana-drift-backfill \
  --solana-drift-market "${DRIFT_MARKET}" \
  2>&1

echo ""
echo "=== Drift S3 backfill complete ==="
date

# Final log upload
kill \$LOG_PID 2>/dev/null || true
gsutil -q cp /var/log/solana-drift.log \
  ${GCS_STAGING}/logs/${VM_NAME}_final.log

echo "Shutting down..."
shutdown -h now
STARTUP_EOF

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  echo ""
  echo "  Startup script:"
  cat "$STARTUP_FILE"
  rm "$STARTUP_FILE"
else
  # Delete existing VM if terminated
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  echo "  Creating VM..."
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --metadata-from-file=startup-script="${STARTUP_FILE}" \
    --boot-disk-size=50GB \
    --boot-disk-type=pd-ssd
  echo "  VM ${VM_NAME} created."
  rm "$STARTUP_FILE"
fi

echo ""
echo "============================================================"
echo "VM launched: ${VM_NAME}"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='name=${VM_NAME}' --project=${PROJECT_ID}"
echo ""
echo "SSH + tail log:"
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/solana-drift.log"
echo ""
echo "Stream logs from GCS:"
echo "  gsutil cat ${GCS_STAGING}/logs/${VM_NAME}_final.log | tail -100"
echo ""
echo "Check results:"
echo "  gsutil ls gs://solana-defi-${PROJECT_ID}/solana_defi/drift/ | head -20"
echo "============================================================"
