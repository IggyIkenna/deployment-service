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
  git curl

log "Python 3.13: $(python3.13 --version)"

# ── 2. Create venv with correct Python ──
log "Creating venv with Python 3.13..."
rm -rf "$VENV"
python3.13 -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version) at $(which python)"

# ── 3. Deploy code ──
log "Deploying code from GCS..."
mkdir -p "$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds" "$LOGS"

# Try GCS first (for startup-script mode), fall back to local tarballs
if gsutil ls "gs://${CODE_BUCKET}/code/" >/dev/null 2>&1; then
  gsutil -q cp "gs://${CODE_BUCKET}/code/unified-api-contracts-code.tar.gz" /tmp/
  gsutil -q cp "gs://${CODE_BUCKET}/code/unified-trading-library-code.tar.gz" /tmp/
  gsutil -q cp "gs://${CODE_BUCKET}/code/mtds-code.tar.gz" /tmp/
  tar xzf /tmp/unified-api-contracts-code.tar.gz -C "$WORKSPACE/uac"
  tar xzf /tmp/unified-trading-library-code.tar.gz -C "$WORKSPACE/utl"
  tar xzf /tmp/mtds-code.tar.gz -C "$WORKSPACE/mtds"
  log "Code deployed from GCS"
elif [ -f /home/ikennaigboaka/unified-api-contracts-code.tar.gz ]; then
  tar xzf /home/ikennaigboaka/unified-api-contracts-code.tar.gz -C "$WORKSPACE/uac"
  tar xzf /home/ikennaigboaka/unified-trading-library-code.tar.gz -C "$WORKSPACE/utl"
  tar xzf /home/ikennaigboaka/mtds-code.tar.gz -C "$WORKSPACE/mtds"
  log "Code deployed from local tarballs"
elif [ -f /tmp/unified-api-contracts-code.tar.gz ]; then
  tar xzf /tmp/unified-api-contracts-code.tar.gz -C "$WORKSPACE/uac"
  tar xzf /tmp/unified-trading-library-code.tar.gz -C "$WORKSPACE/utl"
  tar xzf /tmp/mtds-code.tar.gz -C "$WORKSPACE/mtds"
  log "Code deployed from /tmp tarballs"
else
  log "ERROR: No code tarballs found. SCP them to /home/ikennaigboaka/ or upload to gs://${CODE_BUCKET}/code/"
  exit 1
fi

# ── 4. Install dependencies ──
log "Installing Python dependencies..."
pip install -e "$WORKSPACE/uac" -e "$WORKSPACE/utl" -e "$WORKSPACE/mtds" 2>&1 | tail -1
python -c 'import market_tick_data_service; print("MTDS OK")'
python -c 'from unified_api_contracts.sports import LEAGUE_REGISTRY; print(f"UAC OK: {len(LEAGUE_REGISTRY)} leagues")'
log "Dependencies installed successfully"

# ── 5. Read task from metadata (startup-script mode) ──
VM_TASK=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_TASK 2>/dev/null || echo "")
VM_VENUE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_VENUE 2>/dev/null || echo "")
VM_START_DATE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_START_DATE 2>/dev/null || echo "")
VM_END_DATE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_END_DATE 2>/dev/null || echo "")
VM_CATEGORY=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_CATEGORY 2>/dev/null || echo "CEFI")
VM_OPERATION=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_OPERATION 2>/dev/null || echo "download")

export GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-gcp}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-false}"

if [ -n "$VM_TASK" ] && [ -n "$VM_VENUE" ]; then
  log "Auto-launching task: $VM_TASK venue=$VM_VENUE dates=$VM_START_DATE→$VM_END_DATE"
  nohup "$VENV/bin/python" -m market_tick_data_service \
    --operation "$VM_OPERATION" --mode batch --category "$VM_CATEGORY" \
    --venues "$VM_VENUE" --start-date "$VM_START_DATE" --end-date "$VM_END_DATE" \
    > "$LOGS/backfill.log" 2>&1 &
  log "Task launched PID: $!"
else
  log "No VM_TASK metadata — setup complete, ready for manual launch"
fi

log "=== VM setup complete ==="
echo "READY" > /tmp/vm_ready
