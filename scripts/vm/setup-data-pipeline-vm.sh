#!/usr/bin/env bash
# Setup a GCE VM for data pipeline work (MTDS backfill, migrations, etc.)
#
# This script is the SSOT for VM setup. It handles:
#   1. Python 3.13 installation (required by UAC)
#   2. Build tools for C extensions (ckzg, lru-dict needed by web3/UTL)
#   3. Venv creation with correct Python version
#   4. Code deployment from GCS tarballs
#   5. Dependency installation
#
# Usage as startup script (no SSH needed — runs on boot):
#   gcloud compute instances create my-vm \
#     --zone=asia-northeast1-b \
#     --machine-type=e2-standard-4 \
#     --image-family=ubuntu-2404-lts-amd64 \
#     --image-project=ubuntu-os-cloud \
#     --scopes=cloud-platform \
#     --metadata=startup-script-url=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh \
#     --metadata=VM_TASK=cefi-backfill,VM_VENUE=BINANCE-FUTURES,VM_START_DATE=2020-01-01,VM_END_DATE=2026-04-15
#
# Usage via SSH (manual):
#   bash setup-data-pipeline-vm.sh
#
# Requirements:
#   - Ubuntu 24.04 LTS (has deadsnakes PPA for Python 3.13)
#   - cloud-platform scope (for GCS + Secret Manager access)
#   - Same region as GCS buckets (asia-northeast1) for fast I/O
#
# Lessons learned (2026-04-15):
#   - MUST use python3.13 (not python3/python3.12) — UAC requires >=3.13
#   - MUST install build-essential + python3.13-dev — C extensions fail without them
#   - MUST use python3.13 -m venv (not python3 -m venv) — wrong Python in venv
#   - Tarballs extract to -C target dir, must mkdir -p first
#   - nohup must use full venv path (~/venv/bin/python) not bare 'python'
#   - GCE metadata server uses HTTP internally — no SSL cert issues on Ubuntu
#   - Tardis free data on 1st of month — skip auth to save API calls
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="/var/log/vm-setup.log"
WORKSPACE="/home/ikennaigboaka/workspace"
VENV="/home/ikennaigboaka/venv"
LOGS="/home/ikennaigboaka/logs"
CODE_BUCKET="${CODE_BUCKET:-deployment-scripts-central-element-323112}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

# ── 1. System packages ──
log "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
add-apt-repository -y ppa:deadsnakes/ppa 2>/dev/null || true
apt-get update -qq
apt-get install -y -qq \
  python3.13 python3.13-venv python3.13-dev \
  build-essential \
  git curl pipx

log "Python 3.13: $(python3.13 --version)"

# ── 2. Create venv with correct Python ──
log "Creating venv with Python 3.13..."
rm -rf "$VENV"
python3.13 -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version) at $(which python)"

# Install uv inside venv (10x faster than pip for dependency resolution)
pip install uv -q 2>&1 | tail -1
log "uv: $(uv --version)"

# ── 2b. Read VM metadata early (needed for selective tarball install) ──
_meta() { curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" 2>/dev/null || echo "${2:-}"; }
VM_TASK=$(_meta VM_TASK)
VM_VENUE=$(_meta VM_VENUE)
VM_START_DATE=$(_meta VM_START_DATE)
VM_END_DATE=$(_meta VM_END_DATE)
VM_CATEGORY=$(_meta VM_CATEGORY CEFI)
VM_OPERATION=$(_meta VM_OPERATION download)
VM_SERVICE=$(_meta VM_SERVICE market_tick_data_service)
VM_SPORTS_PROVIDER=$(_meta VM_SPORTS_PROVIDER)
VM_SPORTS_ENTITY=$(_meta VM_SPORTS_ENTITY)
VM_STRATEGY=$(_meta VM_STRATEGY)
VM_PIPELINE_MODE=$(_meta VM_PIPELINE_MODE)
VM_DATA_TYPES=$(_meta VM_DATA_TYPES)
log "VM metadata: SERVICE=$VM_SERVICE TASK=$VM_TASK CATEGORY=$VM_CATEGORY PROVIDER=$VM_SPORTS_PROVIDER"
log "VM metadata: STRATEGY=$VM_STRATEGY PIPELINE_MODE=$VM_PIPELINE_MODE"

# ── 3. Deploy code ──
# Core repos (always required) + optional service repos.
# create-code-tarballs.sh uploads these to gs://{CODE_BUCKET}/code/:
#   unified-api-contracts-code.tar.gz  (core)
#   unified-trading-library-code.tar.gz (core)
#   mtds-code.tar.gz                   (core)
#   instruments-service-code.tar.gz    (optional)
#   features-sports-service-code.tar.gz (optional)
#   ... any --include repo
log "Deploying code from GCS..."
mkdir -p "$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds" "$LOGS"

# Service → tarball mapping. Only install what VM_SERVICE needs.
# UAC + UTL are always required (shared contracts + library).
declare -A SERVICE_TARBALLS=(
  ["market_tick_data_service"]="mtds-code"
  ["instruments_service"]="instruments-service-code"
  ["features_sports_service"]="features-sports-service-code"
  ["features_onchain_service"]="features-onchain-service-code"
  ["market_data_processing_service"]="market-data-processing-service-code"
  ["features_delta_one_service"]="features-delta-one-service-code"
  ["strategy_service"]="strategy-service-code"
  ["execution_service"]="execution-service-code"
  ["pnl_attribution_service"]="pnl-attribution-service-code"
  ["risk_and_exposure_service"]="risk-and-exposure-service-code"
  ["ml_training_service"]="ml-training-service-code"
  ["ml_inference_service"]="ml-inference-service-code"
  ["position_balance_monitor_service"]="position-balance-monitor-service-code"
  ["features_volatility_service"]="features-volatility-service-code"
  ["features_cross_instrument_service"]="features-cross-instrument-service-code"
  ["features_calendar_service"]="features-calendar-service-code"
  ["features_multi_timeframe_service"]="features-multi-timeframe-service-code"
  ["features_commodity_service"]="features-commodity-service-code"
  ["deployment_service"]="deployment-service-code"
)
declare -A TARBALL_DIRS=(
  ["unified-api-contracts-code"]="uac"
  ["unified-trading-library-code"]="utl"
  ["unified-trading-library-code"]="uei"
  ["mtds-code"]="mtds"
  ["instruments-service-code"]="instruments"
  ["features-sports-service-code"]="fss"
  ["features-onchain-service-code"]="fos"
  ["market-data-processing-service-code"]="mdps"
  ["features-delta-one-service-code"]="fd1"
  ["strategy-service-code"]="strategy"
  ["execution-service-code"]="execution"
  ["pnl-attribution-service-code"]="pnl"
  ["risk-and-exposure-service-code"]="risk"
  ["ml-training-service-code"]="ml-train"
  ["ml-inference-service-code"]="ml-infer"
  ["position-balance-monitor-service-code"]="pbm"
  ["features-volatility-service-code"]="fvol"
  ["features-cross-instrument-service-code"]="fci"
  ["features-calendar-service-code"]="fcal"
  ["features-multi-timeframe-service-code"]="fmt"
  ["features-commodity-service-code"]="fcom"
  ["deployment-service-code"]="deployment"
)

# Always install core (UAC + UTL) + the service tarball for VM_SERVICE
NEEDED_TARBALLS=("unified-api-contracts-code" "unified-trading-library-code" "unified-trading-library-code")
SERVICE_TARBALL="${SERVICE_TARBALLS[$VM_SERVICE]:-}"
if [ -n "$SERVICE_TARBALL" ]; then
  NEEDED_TARBALLS+=("$SERVICE_TARBALL")
else
  log "WARNING: Unknown VM_SERVICE=$VM_SERVICE — installing all available tarballs"
  for k in "${!TARBALL_DIRS[@]}"; do NEEDED_TARBALLS+=("$k"); done
fi
log "Tarballs to install: ${NEEDED_TARBALLS[*]}"

INSTALLED_DIRS=()

if gsutil ls "gs://${CODE_BUCKET}/code/" >/dev/null 2>&1; then
  for tarball_name in "${NEEDED_TARBALLS[@]}"; do
    dir="${TARBALL_DIRS[$tarball_name]}"
    tarball_path="/tmp/${tarball_name}.tar.gz"
    if gsutil -q cp "gs://${CODE_BUCKET}/code/${tarball_name}.tar.gz" "$tarball_path" 2>/dev/null; then
      mkdir -p "$WORKSPACE/$dir"
      tar xzf "$tarball_path" -C "$WORKSPACE/$dir"
      INSTALLED_DIRS+=("$WORKSPACE/$dir")
      log "Deployed $tarball_name → $WORKSPACE/$dir"
    else
      log "WARNING: tarball $tarball_name not found in GCS — skipping"
    fi
  done
  log "Code deployed from GCS (${#INSTALLED_DIRS[@]} repos)"
elif [ -f /home/ikennaigboaka/unified-api-contracts-code.tar.gz ]; then
  tar xzf /home/ikennaigboaka/unified-api-contracts-code.tar.gz -C "$WORKSPACE/uac"
  tar xzf /home/ikennaigboaka/unified-trading-library-code.tar.gz -C "$WORKSPACE/utl"
  tar xzf /home/ikennaigboaka/mtds-code.tar.gz -C "$WORKSPACE/mtds"
  INSTALLED_DIRS=("$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds")
  log "Code deployed from local tarballs"
else
  log "ERROR: No code tarballs found. SCP them to /home/ikennaigboaka/ or upload to gs://${CODE_BUCKET}/code/"
  exit 1
fi

# ── 4. Install dependencies (with GCS wheel cache) ──
# External deps (web3, pandas, etc.) are slow to compile from source.
# Cache compiled wheels in GCS so subsequent VMs skip compilation.
# Code repos (UAC/UTL/service) are installed as editable (-e) — always fresh.
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://${CODE_BUCKET}/wheels/py313-linux-x86_64"
mkdir -p "$WHEEL_CACHE"

# Try to download cached wheels
if gsutil -q ls "$WHEEL_GCS/" >/dev/null 2>&1; then
  log "Downloading cached wheels from GCS..."
  gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
  WHEEL_COUNT=$(ls "$WHEEL_CACHE"/*.whl 2>/dev/null | wc -l)
  log "Downloaded $WHEEL_COUNT cached wheels"
fi

log "Installing Python dependencies..."
# --no-sources: ignore [tool.uv.sources] in pyproject.toml which points to
# sibling paths like ../unified-api-contracts that don't exist in our
# tarball layout (we use short names: uac, utl, instruments).
# Instead, editable installs resolve deps from each other since all are
# installed in the same call.
INSTALL_ARGS=("--no-sources")
for dir in "${INSTALLED_DIRS[@]}"; do
  INSTALL_ARGS+=("-e" "$dir")
done
log "  uv pip install ${INSTALL_ARGS[*]}"
uv pip install --find-links "$WHEEL_CACHE" "${INSTALL_ARGS[@]}" 2>&1 | tail -5

# Upload any newly compiled wheels to GCS for next VM
NEW_WHEELS=$(find "$VENV/lib" -name "*.whl" -newer "$WHEEL_CACHE" 2>/dev/null | wc -l)
if [[ "$NEW_WHEELS" -gt 0 ]] || [[ ! -f "$WHEEL_CACHE/.uploaded" ]]; then
  log "Caching compiled wheels to GCS..."
  # Build wheels for all installed packages (captures compiled C extensions)
  uv pip wheel --wheel-dir "$WHEEL_CACHE" "${INSTALL_ARGS[@]}" -q 2>/dev/null || true
  gsutil -m -q cp "$WHEEL_CACHE"/*.whl "$WHEEL_GCS/" 2>/dev/null || true
  touch "$WHEEL_CACHE/.uploaded"
  log "Wheels cached to $WHEEL_GCS"
fi

python -c 'from unified_api_contracts.sports import LEAGUE_REGISTRY; print(f"UAC OK: {len(LEAGUE_REGISTRY)} leagues")'
# Verify whichever service is installed
python -c 'import market_tick_data_service; print("MTDS OK")' 2>/dev/null || true
python -c 'import instruments_service; print("instruments-service OK")' 2>/dev/null || true
log "Dependencies installed successfully (${#INSTALLED_DIRS[@]} packages)"

# ── 5. Auto-launch task (metadata already read in step 2b) ──
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-gcp}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-false}"

if [[ "$VM_PIPELINE_MODE" == "backtest" ]]; then
  # Full L1-L7 pipeline for the category — uses backfill-cluster.sh from
  # deployment-service (uploaded alongside this script).
  BACKFILL_SCRIPT="$WORKSPACE/deployment/scripts/vm/backfill-cluster.sh"
  BACKFILL_ARGS="--cluster ${VM_CATEGORY,,} --start-date $VM_START_DATE --end-date $VM_END_DATE"
  [[ -n "$VM_STRATEGY" ]] && BACKFILL_ARGS="$BACKFILL_ARGS --strategy $VM_STRATEGY"

  if [[ -f "$BACKFILL_SCRIPT" ]]; then
    log "Backtest mode: running full pipeline via backfill-cluster.sh"
    log "  Args: $BACKFILL_ARGS"
    nohup bash "$BACKFILL_SCRIPT" $BACKFILL_ARGS \
      > "$LOGS/backtest-pipeline.log" 2>&1 &
    log "Backtest pipeline launched PID: $!"
  else
    log "WARNING: backfill-cluster.sh not found at $BACKFILL_SCRIPT — falling back to e2e-testing"
    # Try e2e-testing run-full-pipeline.sh as fallback
    E2E_SCRIPT="$WORKSPACE/e2e/scripts/${VM_CATEGORY,,}/run-full-pipeline.sh"
    if [[ -f "$E2E_SCRIPT" ]]; then
      nohup bash "$E2E_SCRIPT" --start-date "$VM_START_DATE" --end-date "$VM_END_DATE" \
        > "$LOGS/backtest-pipeline.log" 2>&1 &
      log "E2E pipeline launched PID: $!"
    else
      log "ERROR: No pipeline script found for category $VM_CATEGORY"
    fi
  fi
elif [ -n "$VM_TASK" ]; then
  # Build CLI args — --venues is optional (some services don't use it)
  # Service-specific operation defaults (instruments-service uses "instruments", MTDS uses "download")
  _OP="$VM_OPERATION"
  if [[ "$VM_SERVICE" == "instruments_service" && "$_OP" == "download" ]]; then
    _OP="instruments"
  fi
  CLI_ARGS="--operation $_OP --mode batch --category $VM_CATEGORY"
  [[ -n "$VM_VENUE" ]] && CLI_ARGS="$CLI_ARGS --venues $VM_VENUE"
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  [[ -n "$VM_SPORTS_PROVIDER" ]] && CLI_ARGS="$CLI_ARGS --sports-provider $VM_SPORTS_PROVIDER"
  [[ -n "$VM_SPORTS_ENTITY" ]] && CLI_ARGS="$CLI_ARGS --sports-entity $VM_SPORTS_ENTITY"
  [[ -n "$VM_STRATEGY" ]] && CLI_ARGS="$CLI_ARGS --strategy $VM_STRATEGY"
  [[ -n "$VM_DATA_TYPES" ]] && CLI_ARGS="$CLI_ARGS --data-types $VM_DATA_TYPES"

  log "Auto-launching: python -m $VM_SERVICE $CLI_ARGS"
  nohup "$VENV/bin/python" -m "$VM_SERVICE" $CLI_ARGS \
    > "$LOGS/backfill.log" 2>&1 &
  log "Task launched PID: $!"
else
  log "No VM_TASK metadata — setup complete, ready for manual launch"
fi

log "=== VM setup complete ==="
echo "READY" > /tmp/vm_ready
