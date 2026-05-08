#!/usr/bin/env bash
# Market Tick Data Service — VM Historical Backfill Script
#
# Backfills sports odds data (Odds API) via MTDS production pipeline.
# Designed to run on GCE VMs with GCS write access.
#
# Usage (on a GCE VM):
#   bash vm_mtds_backfill.sh --asset-group SPORTS --tier 1 --start 2020-06-01 --end 2021-06-01
#   bash vm_mtds_backfill.sh --asset-group SPORTS --tier 2 --start 2025-01-01 --end 2026-03-28
#
# Prerequisites:
#   - GCE VM with SA that has Secret Manager + GCS access (--scopes=cloud-platform)
#   - Python 3.13+ installed
#   - Codebase tarball at GCS_TARBALL_PATH (optional — if already unpacked, skip)
set -euo pipefail

# ---------- Defaults ----------
WORK_DIR="${WORK_DIR:-/tmp/mtds_backfill}"
START_DATE="${START_DATE:-2020-06-01}"
END_DATE="${END_DATE:-2021-06-01}"
ASSET_GROUP=""
TIER=""
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-7}"  # 7 days per batch — odds API is rate-limited
GCS_TARBALL_PATH="${GCS_TARBALL_PATH:-}"
VENUES=""
DATA_TYPES=""

# ---------- Parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --tier) TIER="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --work-dir) WORK_DIR="$2"; shift 2 ;;
    --tarball) GCS_TARBALL_PATH="$2"; shift 2 ;;
    --venues) VENUES="$2"; shift 2 ;;
    --data-types) DATA_TYPES="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

CATEGORY_FLAG=""
if [[ -n "$ASSET_GROUP" ]]; then
  CATEGORY_FLAG="--asset-group ${ASSET_GROUP}"
fi

TIER_FLAG=""
if [[ -n "$TIER" ]]; then
  TIER_FLAG="--tier ${TIER}"
fi

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

VENUES_FLAG=""
if [[ -n "$VENUES" ]]; then
  VENUES_FLAG="--venues ${VENUES}"
fi

DATA_TYPES_FLAG=""
if [[ -n "$DATA_TYPES" ]]; then
  DATA_TYPES_FLAG="--data-types ${DATA_TYPES}"
fi

echo "============================================================"
echo "MTDS Backfill — VM Setup"
echo "  Work dir:  ${WORK_DIR}"
echo "  Category:  ${ASSET_GROUP:-ALL}"
echo "  Tier:      ${TIER:-default}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Venues:    ${VENUES:-all}"
echo "  DataTypes: ${DATA_TYPES:-all}"
echo "  Tarball:   ${GCS_TARBALL_PATH}"
echo "============================================================"

mkdir -p "${WORK_DIR}"
cd "${WORK_DIR}"

# ---------- Install system deps ----------
if ! command -v uv &>/dev/null; then
  echo "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

# ---------- Download and unpack codebase from GCS ----------
if [[ -n "$GCS_TARBALL_PATH" ]] && [[ ! -d "${WORK_DIR}/market-tick-data-service" ]]; then
  echo ""
  echo "--- Downloading codebase from GCS ---"
  gsutil -q cp "${GCS_TARBALL_PATH}" "${WORK_DIR}/codebase.tar.gz"
  echo "  Unpacking..."
  tar xzf "${WORK_DIR}/codebase.tar.gz" -C "${WORK_DIR}"
  rm "${WORK_DIR}/codebase.tar.gz"
  echo "  Done. Repos:"
  ls -d "${WORK_DIR}"/*/
fi

# ---------- Create venv and install ----------
echo ""
echo "--- Setting up Python venv ---"
if [[ ! -d "${WORK_DIR}/.venv" ]]; then
  if command -v python3.13 &>/dev/null; then
    python3.13 -m venv "${WORK_DIR}/.venv"
  else
    uv venv "${WORK_DIR}/.venv" --python 3.13
  fi
fi
source "${WORK_DIR}/.venv/bin/activate"
echo "  Python: $(python3 --version)"

echo "Installing packages (editable, only repos present in tarball)..."
# GCS wheel cache — skip compilation for C extensions (web3, pandas, etc.)
WHEEL_CACHE="/tmp/wheel-cache"
CODE_BUCKET="${CODE_BUCKET:-deployment-scripts-central-element-323112}"
WHEEL_GCS="gs://${CODE_BUCKET}/wheels/py313-linux-x86_64"
mkdir -p "$WHEEL_CACHE"
if gsutil -q ls "$WHEEL_GCS/" >/dev/null 2>&1; then
  echo "  Downloading cached wheels from GCS..."
  gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
  echo "  Downloaded $(ls "$WHEEL_CACHE"/*.whl 2>/dev/null | wc -l) cached wheels"
fi

# --no-sources: ignore [tool.uv.sources] path overrides that reference
# sibling paths like ../unified-api-contracts (wrong layout in tarball).
INSTALL_ARGS="--no-sources"
for pkg in \
  unified-internal-contracts \
  unified-api-contracts \
  unified-trading-library \
  unified-cloud-interface \
  unified-config-interface \
  unified-trading-library \
  unified-features-interface \
  unified-reference-data-interface \
  market-tick-data-service; do
  if [[ -d "${WORK_DIR}/${pkg}" ]]; then
    INSTALL_ARGS="${INSTALL_ARGS} -e ${WORK_DIR}/${pkg}"
  fi
done
uv pip install --find-links "$WHEEL_CACHE" ${INSTALL_ARGS}

# Extra runtime deps
uv pip install --find-links "$WHEEL_CACHE" pandas pyarrow google-cloud-secret-manager google-cloud-storage aiohttp

# Upload any newly compiled wheels to GCS for next VM
if [[ ! -f "$WHEEL_CACHE/.uploaded" ]]; then
  echo "  Caching compiled wheels to GCS..."
  uv pip wheel --wheel-dir "$WHEEL_CACHE" ${INSTALL_ARGS} -q 2>/dev/null || true
  gsutil -m -q cp "$WHEEL_CACHE"/*.whl "$WHEEL_GCS/" 2>/dev/null || true
  touch "$WHEEL_CACHE/.uploaded"
fi

echo ""
echo "--- Verifying imports ---"
python3 -c "
from market_tick_data_service.cli.main import main_service_cli
print('  market_tick_data_service: OK')
"

# ---------- Run backfill in date chunks ----------
OUTPUT_DIR="${WORK_DIR}/logs"
mkdir -p "${OUTPUT_DIR}"

echo ""
echo "============================================================"
echo "Starting MTDS backfill: ${START_DATE} → ${END_DATE}"
echo "  Category: ${ASSET_GROUP:-ALL}"
echo "  Tier:     ${TIER:-default}"
echo "  Log: ${OUTPUT_DIR}/backfill.log"
echo "============================================================"

# Generate date chunks using Python
CHUNKS=$(python3 -c "
from datetime import datetime, timedelta
start = datetime.strptime('${START_DATE}', '%Y-%m-%d')
end = datetime.strptime('${END_DATE}', '%Y-%m-%d')
chunk_days = ${CHUNK_SIZE}
current = start
while current <= end:
    chunk_end = min(current + timedelta(days=chunk_days - 1), end)
    print(f'{current.strftime(\"%Y-%m-%d\")} {chunk_end.strftime(\"%Y-%m-%d\")}')
    current = chunk_end + timedelta(days=1)
")

TOTAL_CHUNKS=$(echo "$CHUNKS" | wc -l | tr -d ' ')
CHUNK_NUM=0

cd "${WORK_DIR}/market-tick-data-service"

echo "$CHUNKS" | while read -r CHUNK_START CHUNK_END; do
  CHUNK_NUM=$((CHUNK_NUM + 1))
  echo ""
  echo "--- Chunk ${CHUNK_NUM}/${TOTAL_CHUNKS}: ${CHUNK_START} → ${CHUNK_END} ---"

  # GCP_PROJECT_ID: resolve from env, GCE metadata, or default
  if [[ -z "${GCP_PROJECT_ID:-}" ]]; then
    export GCP_PROJECT_ID=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id 2>/dev/null || echo "central-element-323112")
    export GOOGLE_CLOUD_PROJECT="${GCP_PROJECT_ID}"
    echo "  Resolved GCP_PROJECT_ID=${GCP_PROJECT_ID} from metadata"
  fi

  CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID="${GCP_PROJECT_ID}" \
    "${WORK_DIR}/.venv/bin/python" -m market_tick_data_service \
      --operation download \
      --mode batch \
      ${CATEGORY_FLAG} \
      ${TIER_FLAG} \
      --start-date "${CHUNK_START}" \
      --end-date "${CHUNK_END}" \
      ${FORCE_FLAG} \
      ${VENUES_FLAG} \
      ${DATA_TYPES_FLAG} \
      --log-level INFO \
    2>&1 | tee -a "${OUTPUT_DIR}/backfill.log"

  EXIT_CODE=${PIPESTATUS[0]}
  if [[ $EXIT_CODE -ne 0 ]]; then
    echo "WARNING: Chunk ${CHUNK_START}→${CHUNK_END} exited with code ${EXIT_CODE}" | tee -a "${OUTPUT_DIR}/backfill.log"
    # Continue to next chunk — shard-level isolation
  fi

  # Progress summary (parseable by log watchers)
  echo "PROGRESS: chunk=${CHUNK_NUM}/${TOTAL_CHUNKS} range=${CHUNK_START}→${CHUNK_END} status=$([ $EXIT_CODE -eq 0 ] && echo OK || echo WARN) time=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "${OUTPUT_DIR}/backfill.log"
done

echo ""
echo "============================================================"
echo "MTDS Backfill complete."
echo "  Log: ${OUTPUT_DIR}/backfill.log"
echo "============================================================"
