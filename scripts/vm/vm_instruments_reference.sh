#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Instruments Service — VM Sports Reference Data Backfill
#
# Runs instruments-service --operation instruments --mode batch --asset-group SPORTS
# for a date range. Supports --sports-entity to scope to a single manifest entity,
# enabling 17 parallel entity-scoped VMs (one per entity type).
#
# Entity types (manifest names):
#   API_FOOTBALL_FIXTURES, API_FOOTBALL_LEAGUES, API_FOOTBALL_TEAMS,
#   API_FOOTBALL_STANDINGS, API_FOOTBALL_INJURIES,
#   API_FOOTBALL_FIXTURE_STATS, API_FOOTBALL_FIXTURE_EVENTS,
#   API_FOOTBALL_FIXTURE_LINEUPS, API_FOOTBALL_PLAYER_STATS
#   FOOTYSTATS_MATCHES, FOOTYSTATS_PREDICTIONS,
#   SFI_LEAGUES, SFI_STANDINGS, TRANSFERMARKT_LEAGUES, TRANSFERMARKT_TEAMS,
#   UNDERSTAT_XG, WEATHER
#
# Usage (on a GCE VM):
#   bash vm_instruments_reference.sh --start 2024-01-01 --end 2024-12-31 \
#     --sports-entity API_FOOTBALL_INJURIES --work-dir /tmp/instruments
#
# Prerequisites:
#   - GCE VM with SA that has GCS + Secret Manager access (--scopes=cloud-platform)
#   - Python 3.13+ installed
#   - Codebase tarball already unpacked at WORK_DIR
set -euo pipefail

# ---------- Defaults ----------
WORK_DIR="${WORK_DIR:-/tmp/instruments}"
START_DATE="${START_DATE:-2019-01-01}"
END_DATE="${END_DATE:-$(date +%F)}"
FORCE=false
SPORTS_ENTITY=""
VENUES="API_FOOTBALL"

# ---------- Parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --sports-entity) SPORTS_ENTITY="$2"; shift 2 ;;
    --venues) VENUES="$2"; shift 2 ;;
    --work-dir) WORK_DIR="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

ENTITY_FLAG=""
if [[ -n "${SPORTS_ENTITY}" ]]; then
  ENTITY_FLAG="--sports-entity ${SPORTS_ENTITY}"
fi

echo "============================================================"
echo "Instruments Service — Sports Reference Data Backfill"
echo "  Work dir:    ${WORK_DIR}"
echo "  Range:       ${START_DATE} → ${END_DATE}"
echo "  Force:       ${FORCE}"
echo "  Venues:      ${VENUES}"
echo "  Entity:      ${SPORTS_ENTITY:-ALL}"
echo "============================================================"

mkdir -p "${WORK_DIR}"
cd "${WORK_DIR}"

# ---------- Install system deps ----------
if ! command -v uv &>/dev/null; then
  echo "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
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
  timeout 180 gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
  echo "  Downloaded $(ls "$WHEEL_CACHE"/*.whl 2>/dev/null | wc -l) cached wheels"
fi

# --no-sources: ignore [tool.uv.sources] path overrides that reference
# sibling paths like ../unified-api-contracts (wrong layout in tarball).
INSTALL_ARGS="--no-sources"
for pkg in \
  unified-api-contracts \
  unified-trading-library \
  instruments-service; do
  if [[ -d "${WORK_DIR}/${pkg}" ]]; then
    INSTALL_ARGS="${INSTALL_ARGS} -e ${WORK_DIR}/${pkg}"
  fi
done
uv pip install --find-links "$WHEEL_CACHE" ${INSTALL_ARGS}

# Extra runtime deps (GCS, Secret Manager for API keys)
uv pip install --find-links "$WHEEL_CACHE" google-cloud-storage google-cloud-secret-manager

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
print('  instruments_service CLI: OK')
from instruments_service.reference_data.adapters.sports import create_sports_reference_adapter
print('  sports reference adapter: OK')
"

# ---------- GCP project resolution ----------
if [[ -z "${GCP_PROJECT_ID:-}" ]]; then
  export GCP_PROJECT_ID=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id 2>/dev/null || echo "central-element-323112")
  export GOOGLE_CLOUD_PROJECT="${GCP_PROJECT_ID}"
  echo "  Resolved GCP_PROJECT_ID=${GCP_PROJECT_ID} from metadata"
fi

# ---------- Run instruments-service for full date range ----------
# Single process invocation: the CLI's batch mode iterates dates internally.
# This keeps module-level caches (leagues/teams/standings DataFrames) alive
# across dates, saving ~67 API calls per date after the first.
OUTPUT_DIR="${WORK_DIR}/logs"
mkdir -p "${OUTPUT_DIR}"

echo ""
echo "============================================================"
echo "Starting instruments-service backfill: ${START_DATE} → ${END_DATE}"
echo "  Log: ${OUTPUT_DIR}/instruments.log"
echo "============================================================"

cd "${WORK_DIR}/instruments-service"

set +e
CLOUD_PROVIDER=gcp \
CLOUD_MOCK_MODE=false \
DATA_MODE=real \
IS_TEST_RUN=false \
GCP_PROJECT_ID="${GCP_PROJECT_ID}" \
GOOGLE_CLOUD_PROJECT="${GCP_PROJECT_ID}" \
  "${WORK_DIR}/.venv/bin/instruments-service" \
    --operation instruments \
    --mode batch \
    --asset-group SPORTS \
    --venues ${VENUES} \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    ${FORCE_FLAG} \
    ${ENTITY_FLAG} \
    --log-level INFO \
  2>&1 | tee -a "${OUTPUT_DIR}/instruments.log"
EXIT_CODE=${PIPESTATUS[0]}
set -e

echo ""
echo "============================================================"
echo "Instruments reference data backfill complete."
echo "  Range: ${START_DATE} → ${END_DATE}"
echo "  Exit code: ${EXIT_CODE}"
echo "  Log: ${OUTPUT_DIR}/instruments.log"
echo "============================================================"
