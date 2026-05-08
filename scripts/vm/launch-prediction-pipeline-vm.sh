#!/usr/bin/env bash
# Launch GCE VM for full PREDICTION pipeline (post tick-data):
#   1. MDPS: tick data → OHLCV candles (all 389 days)
#   2. features-cross-instrument: tick-based features (remaining ~101 days)
#   3. features-delta-one: candle-based features (all 389 days)
#
# Prereqs: tick data must be in GCS (run tick data backfill first).
#
# Usage:
#   bash launch_prediction_pipeline_vm.sh
#   bash launch_prediction_pipeline_vm.sh --dry-run
#   bash launch_prediction_pipeline_vm.sh --start 2025-12-26 --end 2026-04-05 --skip-mdps
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2025-03-13}"
END_DATE="${END_DATE:-2026-04-05}"
FORCE=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
VM_NAME_OVERRIDE=""
SKIP_MDPS=false
SKIP_CROSS_INSTRUMENT=false
SKIP_DELTA_ONE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --skip-mdps) SKIP_MDPS=true; shift ;;
    --skip-cross-instrument) SKIP_CROSS_INSTRUMENT=true; shift ;;
    --skip-delta-one) SKIP_DELTA_ONE=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GCS_BUCKET="gs://market-data-tick-prediction-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/prediction_pipeline"
TARBALL_NAME="prediction_pipeline_codebase.tar.gz"
VM_NAME="${VM_NAME_OVERRIDE:-prediction-pipeline-1}"

TOTAL_DAYS=$(python3 -c "
from datetime import datetime
d1 = datetime.strptime('${START_DATE}', '%Y-%m-%d')
d2 = datetime.strptime('${END_DATE}', '%Y-%m-%d')
print((d2-d1).days + 1)
" 2>/dev/null || echo "?")

echo "============================================================"
echo "Prediction Pipeline VM Launcher"
echo "  Project:    ${PROJECT_ID}"
echo "  Zone:       ${ZONE}"
echo "  Machine:    ${MACHINE_TYPE}"
echo "  Range:      ${START_DATE} → ${END_DATE} (${TOTAL_DAYS} days)"
echo "  Force:      ${FORCE}"
echo "  Skip MDPS:  ${SKIP_MDPS}"
echo "  Skip cross: ${SKIP_CROSS_INSTRUMENT}"
echo "  Skip delta: ${SKIP_DELTA_ONE}"
echo "  Workspace:  ${WORKSPACE_ROOT}"
echo "============================================================"

# ---------- Step 1: Package codebase ----------
echo ""
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "market-tick-data-service"
  "market-data-processing-service"
  "features-cross-instrument-service"
  "features-delta-one-service"
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

  # Include runtime-topology.yaml
  TOPOLOGY_SRC="${WORKSPACE_ROOT}/unified-trading-pm/configs/runtime-topology.yaml"
  if [[ -f "${TOPOLOGY_SRC}" ]]; then
    mkdir -p "${STAGING_DIR}/unified-trading-pm/configs"
    cp "${TOPOLOGY_SRC}" "${STAGING_DIR}/unified-trading-pm/configs/"
    echo "  Copied runtime-topology.yaml"
  fi

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

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << 'STARTUP_EOF'
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/prediction_pipeline
export HOME=/root
export PATH="/root/.local/bin:$PATH"
export WORKSPACE_ROOT=/tmp/prediction_pipeline

exec > >(tee /var/log/prediction-pipeline.log) 2>&1

STARTUP_EOF

cat >> "$STARTUP_FILE" << STARTUP_EOF
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Range: ${START_DATE} → ${END_DATE}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase from GCS
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz

# Create venv + install ALL services
echo "Setting up Python venv..."
python3.13 -m venv \${WORK_DIR}/.venv
source \${WORK_DIR}/.venv/bin/activate

# GCS wheel cache — skip compilation for C extensions
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://deployment-scripts-${PROJECT_ID}/wheels/py313-linux-x86_64"
mkdir -p "\$WHEEL_CACHE"
gsutil -m -q cp "\$WHEEL_GCS/*.whl" "\$WHEEL_CACHE/" 2>/dev/null || true

# --no-sources: ignore [tool.uv.sources] path overrides
INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library \\
           market-tick-data-service market-data-processing-service \\
           features-cross-instrument-service features-delta-one-service; do
  if [[ -d "\${WORK_DIR}/\${pkg}" ]]; then
    INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
  fi
done
uv pip install --find-links "\$WHEEL_CACHE" \${INSTALL_ARGS}
uv pip install --find-links "\$WHEEL_CACHE" pandas pyarrow google-cloud-secret-manager google-cloud-storage aiohttp polars

echo "Verifying imports..."
python3 -c "
from market_data_processing_service.cli.main import run_cli; print('  MDPS: OK')
from features_cross_instrument_service.cli.main import main; print('  cross-instrument: OK')
from features_delta_one_service.cli.main import main; print('  delta-one: OK')
"

# Generate date list
DATES=\$(python3 -c "
from datetime import datetime, timedelta
start = datetime.strptime('${START_DATE}', '%Y-%m-%d')
end = datetime.strptime('${END_DATE}', '%Y-%m-%d')
current = start
while current <= end:
    print(current.strftime('%Y-%m-%d'))
    current += timedelta(days=1)
")
TOTAL_DATES=\$(echo "\$DATES" | wc -l | tr -d ' ')

TICK_BUCKET="market-data-tick-prediction-${PROJECT_ID}"

# ===================================================================
# STAGE 1: MDPS — tick data → OHLCV candles
# ===================================================================
SKIP_MDPS=${SKIP_MDPS}
if [[ "\${SKIP_MDPS}" != "true" ]]; then
  echo ""
  echo "============================================================"
  echo "STAGE 1: MDPS candle processing (\${TOTAL_DATES} days)"
  echo "============================================================"

  DATE_NUM=0
  echo "\$DATES" | while read -r PROC_DATE; do
    DATE_NUM=\$((DATE_NUM + 1))
    echo ""
    echo "--- MDPS [\${DATE_NUM}/\${TOTAL_DATES}] \${PROC_DATE} ---"

    set +e
    CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID="${PROJECT_ID}" \\
      MDPS_ASSET_GROUP=PREDICTION \\
      PROTOCOL_DATA_SOURCE_BUCKET_PREDICTION="\${TICK_BUCKET}" \\
      PROTOCOL_DATA_SINK_BUCKET_PREDICTION="\${TICK_BUCKET}" \\
      SKIP_DEPENDENCY_CHECK=true \\
      "\${WORK_DIR}/.venv/bin/market-data-processing" \\
        --operation process \\
        --mode batch \\
        --start-date "\${PROC_DATE}" \\
        --end-date "\${PROC_DATE}" \\
        ${FORCE_FLAG} \\
      2>&1
    MDPS_EXIT=\${PIPESTATUS[0]:-\$?}
    set -e

    if [[ \$MDPS_EXIT -ne 0 ]]; then
      echo "WARNING: MDPS date \${PROC_DATE} exited with code \${MDPS_EXIT}"
    fi
  done
  echo "STAGE 1 (MDPS) complete."
else
  echo "STAGE 1 (MDPS): SKIPPED"
fi

# ===================================================================
# STAGE 2: features-cross-instrument — tick-based features
# ===================================================================
SKIP_CROSS=${SKIP_CROSS_INSTRUMENT}
if [[ "\${SKIP_CROSS}" != "true" ]]; then
  echo ""
  echo "============================================================"
  echo "STAGE 2: Cross-instrument features (\${TOTAL_DATES} days)"
  echo "============================================================"

  DATE_NUM=0
  echo "\$DATES" | while read -r PROC_DATE; do
    DATE_NUM=\$((DATE_NUM + 1))
    echo ""
    echo "--- Cross-instrument [\${DATE_NUM}/\${TOTAL_DATES}] \${PROC_DATE} ---"

    set +e
    CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID="${PROJECT_ID}" \\
      "\${WORK_DIR}/.venv/bin/features-cross-instrument" \\
        --operation compute \\
        --mode batch \\
        --asset-group PREDICTION \\
        --date "\${PROC_DATE}" \\
        ${FORCE_FLAG} \\
        --log-level INFO \\
      2>&1
    FEAT_EXIT=\${PIPESTATUS[0]:-\$?}
    set -e

    if [[ \$FEAT_EXIT -ne 0 ]]; then
      echo "WARNING: cross-instrument features date \${PROC_DATE} exited with code \${FEAT_EXIT}"
    fi
  done
  echo "STAGE 2 (cross-instrument) complete."
else
  echo "STAGE 2 (cross-instrument): SKIPPED"
fi

# ===================================================================
# STAGE 3: features-delta-one — OHLCV candle-based features
# ===================================================================
SKIP_DELTA=${SKIP_DELTA_ONE}
if [[ "\${SKIP_DELTA}" != "true" ]]; then
  echo ""
  echo "============================================================"
  echo "STAGE 3: Delta-one features (\${TOTAL_DATES} days)"
  echo "============================================================"

  DATE_NUM=0
  echo "\$DATES" | while read -r PROC_DATE; do
    DATE_NUM=\$((DATE_NUM + 1))
    echo ""
    echo "--- Delta-one [\${DATE_NUM}/\${TOTAL_DATES}] \${PROC_DATE} ---"

    set +e
    CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID="${PROJECT_ID}" \\
      PROTOCOL_DATA_SOURCE_BUCKET_PREDICTION="\${TICK_BUCKET}" \\
      "\${WORK_DIR}/.venv/bin/features-delta-one" \\
        --operation compute \\
        --mode batch \\
        --asset-group PREDICTION \\
        --start-date "\${PROC_DATE}" \\
        --end-date "\${PROC_DATE}" \\
        --timeframe 1m \\
        ${FORCE_FLAG} \\
      2>&1
    DELTA_EXIT=\${PIPESTATUS[0]:-\$?}
    set -e

    if [[ \$DELTA_EXIT -ne 0 ]]; then
      echo "WARNING: delta-one features date \${PROC_DATE} exited with code \${DELTA_EXIT}"
    fi
  done
  echo "STAGE 3 (delta-one) complete."
else
  echo "STAGE 3 (delta-one): SKIPPED"
fi

# Upload log to GCS
gsutil -q cp /var/log/prediction-pipeline.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo ""
echo "============================================================"
echo "Prediction pipeline complete."
echo "============================================================"
date
shutdown -h now
STARTUP_EOF

echo "--- ${VM_NAME}: ${START_DATE} → ${END_DATE} ---"

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  rm "$STARTUP_FILE"
else
  echo "  Creating VM..."
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB \
    --scopes=cloud-platform \
    --metadata-from-file=startup-script="${STARTUP_FILE}" \
    --no-restart-on-failure

  rm "$STARTUP_FILE"

  echo "  VM created: ${VM_NAME}"
fi

echo ""
echo "============================================================"
echo "Prediction Pipeline VM Launched"
echo "  VM:     ${VM_NAME} (${ZONE})"
echo "  Range:  ${START_DATE} → ${END_DATE} (${TOTAL_DAYS} days)"
echo ""
echo "Monitor:"
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/prediction-pipeline.log"
echo ""
echo "Logs (after completion):"
echo "  gsutil cat ${GCS_STAGING}/logs/${VM_NAME}.log | tail -50"
echo ""
echo "Delete when done:"
echo "  gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} --quiet"
echo "============================================================"
