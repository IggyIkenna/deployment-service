#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Instruments Service — VM Historical Backfill Script
#
# Backfills instrument reference data from 2020 to present across all domains.
# Designed to run on GCE VMs with GCS write access.
#
# The codebase is pre-packaged as a tarball on GCS (uploaded by the launcher).
# No git clone needed — the VM just downloads, unpacks, installs, and runs.
#
# Usage (on a GCE VM):
#   bash vm_instruments_backfill.sh --asset-group CEFI --start 2020-01-01 --end 2022-12-31
#   bash vm_instruments_backfill.sh --asset-group DEFI --start 2020-01-01 --end 2026-02-28
#   bash vm_instruments_backfill.sh --asset-group TRADFI
#
# Prerequisites:
#   - GCE VM with SA that has Secret Manager + GCS access (--scopes=cloud-platform)
#   - Python 3.11+ installed
#   - Codebase tarball at GCS_TARBALL_PATH
set -euo pipefail

# ---------- Defaults ----------
WORK_DIR="${WORK_DIR:-/tmp/instruments_backfill}"
START_DATE="${START_DATE:-2020-01-01}"
END_DATE="${END_DATE:-2026-02-28}"
ASSET_GROUP=""  # empty = all
FORCE=false
VENUES=""  # empty = all venues for category
CHUNK_SIZE="${CHUNK_SIZE:-30}"  # Days per batch invocation
GCS_TARBALL_PATH="${GCS_TARBALL_PATH:-}"

# ---------- Parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --venues) VENUES="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --work-dir) WORK_DIR="$2"; shift 2 ;;
    --tarball) GCS_TARBALL_PATH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

CATEGORY_FLAG=""
if [[ -n "$ASSET_GROUP" ]]; then
  CATEGORY_FLAG="--asset-group ${ASSET_GROUP}"
fi

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

VENUES_FLAG=""
# instruments-service CLI does not accept --venues; flag is intentionally suppressed.

echo "============================================================"
echo "Instruments Backfill — VM Setup"
echo "  Work dir:  ${WORK_DIR}"
echo "  Category:  ${ASSET_GROUP:-ALL}"
echo "  Venues:    ${VENUES:-ALL}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
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
if [[ -n "$GCS_TARBALL_PATH" ]] && [[ ! -d "${WORK_DIR}/instruments-service" ]]; then
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
  # Use system python3.13 (deadsnakes PPA) — has proper SSL cert handling
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
  timeout 180 gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
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
  instruments-service; do
  if [[ -d "${WORK_DIR}/${pkg}" ]]; then
    INSTALL_ARGS="${INSTALL_ARGS} -e ${WORK_DIR}/${pkg}"
  fi
done
uv pip install --find-links "$WHEEL_CACHE" ${INSTALL_ARGS}

# Extra runtime deps
uv pip install --find-links "$WHEEL_CACHE" pandas pyarrow google-cloud-secret-manager google-cloud-storage aiohttp ccxt

# Upload any newly compiled wheels to GCS for next VM
if [[ ! -f "$WHEEL_CACHE/.uploaded" ]]; then
  echo "  Caching compiled wheels to GCS..."
  uv pip wheel --wheel-dir "$WHEEL_CACHE" ${INSTALL_ARGS} -q 2>/dev/null || true
  timeout 180 gsutil -m -q cp "$WHEEL_CACHE"/*.whl "$WHEEL_GCS/" 2>/dev/null || true
  touch "$WHEEL_CACHE/.uploaded"
fi

echo ""
echo "--- Verifying imports ---"
python3 -c "
from instruments_service.cli.main import main_service_cli
from unified_api_contracts.registry.venue_mapping import VenueMapping
vm = VenueMapping()
print(f'  instruments_service: OK')
print(f'  Venues with start dates: {len(vm.venue_start_dates)}')
earliest = min(vm.venue_start_dates.values())
print(f'  Earliest venue: {earliest}')
"

# ---------- Run backfill in date chunks ----------
OUTPUT_DIR="${WORK_DIR}/logs"
mkdir -p "${OUTPUT_DIR}"

echo ""
echo "============================================================"
echo "Starting instruments backfill: ${START_DATE} → ${END_DATE}"
echo "  Category: ${ASSET_GROUP:-ALL}"
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

cd "${WORK_DIR}/instruments-service"

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
    "${WORK_DIR}/.venv/bin/python" -m instruments_service \
      --operation instruments \
      --mode batch \
      ${CATEGORY_FLAG} \
      --start-date "${CHUNK_START}" \
      --end-date "${CHUNK_END}" \
      ${FORCE_FLAG} \
      ${VENUES_FLAG} \
      --log-level INFO \
    2>&1 | tee -a "${OUTPUT_DIR}/backfill.log"

  EXIT_CODE=${PIPESTATUS[0]}
  if [[ $EXIT_CODE -ne 0 ]]; then
    echo "WARNING: Chunk ${CHUNK_START}→${CHUNK_END} exited with code ${EXIT_CODE}" | tee -a "${OUTPUT_DIR}/backfill.log"
    # Continue to next chunk — shard-level isolation means partial success is fine
  fi
done

echo ""
echo "============================================================"
echo "Backfill complete."
echo "  Log: ${OUTPUT_DIR}/backfill.log"
echo "============================================================"
