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

# Tarball → workspace directory mapping
declare -A TARBALL_MAP=(
  ["unified-api-contracts-code"]="uac"
  ["unified-trading-library-code"]="utl"
  ["mtds-code"]="mtds"
  ["instruments-service-code"]="instruments"
  ["features-sports-service-code"]="fss"
  ["features-onchain-service-code"]="fos"
  ["market-data-processing-service-code"]="mdps"
  ["features-delta-one-service-code"]="fd1"
)

INSTALLED_DIRS=()

if gsutil ls "gs://${CODE_BUCKET}/code/" >/dev/null 2>&1; then
  for tarball_name in "${!TARBALL_MAP[@]}"; do
    dir="${TARBALL_MAP[$tarball_name]}"
    tarball_path="/tmp/${tarball_name}.tar.gz"
    if gsutil -q cp "gs://${CODE_BUCKET}/code/${tarball_name}.tar.gz" "$tarball_path" 2>/dev/null; then
      mkdir -p "$WORKSPACE/$dir"
      tar xzf "$tarball_path" -C "$WORKSPACE/$dir"
      INSTALLED_DIRS+=("$WORKSPACE/$dir")
      log "Deployed $tarball_name → $WORKSPACE/$dir"
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

# ── 4. Install dependencies ──
log "Installing Python dependencies..."
INSTALL_ARGS=()
for dir in "${INSTALLED_DIRS[@]}"; do
  INSTALL_ARGS+=("-e" "$dir")
done
uv pip install "${INSTALL_ARGS[@]}" 2>&1 | tail -1
python -c 'from unified_api_contracts.sports import LEAGUE_REGISTRY; print(f"UAC OK: {len(LEAGUE_REGISTRY)} leagues")'
# Verify whichever service is installed
python -c 'import market_tick_data_service; print("MTDS OK")' 2>/dev/null || true
python -c 'import instruments_service; print("instruments-service OK")' 2>/dev/null || true
log "Dependencies installed successfully (${#INSTALLED_DIRS[@]} packages)"

# ── 5. Read task from metadata (startup-script mode) ──
# Read VM metadata — use -sf (silent + fail on HTTP errors) so missing
# attributes return empty string via || fallback, not HTML 404 pages.
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

export GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-gcp}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-false}"

if [ -n "$VM_TASK" ]; then
  # Build CLI args — --venues is optional (some services don't use it)
  CLI_ARGS="--operation $VM_OPERATION --mode batch --category $VM_CATEGORY"
  [[ -n "$VM_VENUE" ]] && CLI_ARGS="$CLI_ARGS --venues $VM_VENUE"
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  [[ -n "$VM_SPORTS_PROVIDER" ]] && CLI_ARGS="$CLI_ARGS --sports-provider $VM_SPORTS_PROVIDER"
  [[ -n "$VM_SPORTS_ENTITY" ]] && CLI_ARGS="$CLI_ARGS --sports-entity $VM_SPORTS_ENTITY"

  log "Auto-launching: python -m $VM_SERVICE $CLI_ARGS"
  nohup "$VENV/bin/python" -m "$VM_SERVICE" $CLI_ARGS \
    > "$LOGS/backfill.log" 2>&1 &
  log "Task launched PID: $!"
else
  log "No VM_TASK metadata — setup complete, ready for manual launch"
fi

log "=== VM setup complete ==="
echo "READY" > /tmp/vm_ready
