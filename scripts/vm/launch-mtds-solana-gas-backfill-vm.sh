#!/usr/bin/env bash
# Launch a GCE VM for Solana gas fee backfill.
#
# Uses getBlock with full transactions to extract per-transaction fee data.
# Alchemy Solana archival RPC has data from ~2021-01-01 onwards.
# Retry logic (12 retries, exponential backoff) is baked into the Solana gas fee client.
#
# Usage:
#   bash launch_solana_gas_vm.sh                  # Launch VM
#   bash launch_solana_gas_vm.sh --dry-run        # Print plan only
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
START_DATE="${START_DATE:-2021-01-01}"
END_DATE="$(date +%Y-%m-%d)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

VM_NAME="mtds-gas-fees-solana"
GCS_STAGING="gs://market-data-tick-defi-${PROJECT_ID}/_vm_staging/gas_fees"
TARBALL_NAME="gas_fees_codebase.tar.gz"
GCS_TARBALL="${GCS_STAGING}/${TARBALL_NAME}"

# Reuse existing tarball from EVM fleet (same codebase)
echo "=== Checking for existing codebase tarball ==="
if gsutil -q stat "${GCS_TARBALL}" 2>/dev/null; then
  echo "  Tarball exists at ${GCS_TARBALL}"
else
  echo "  ERROR: No tarball found. Run launch_gas_fees_fleet.sh first to upload codebase."
  exit 1
fi

# Fetch Alchemy API key
ALCHEMY_KEY=$(gcloud secrets versions access latest --secret=alchemy-api-key --project="${PROJECT_ID}" 2>/dev/null || echo "")
if [[ -z "$ALCHEMY_KEY" ]]; then
  echo "ERROR: Could not fetch alchemy-api-key from Secret Manager"
  exit 1
fi

echo ""
echo "=== Launching Solana VM ==="
echo "  Date range: ${START_DATE} → ${END_DATE}"

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/gas_fees
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/gas-fees.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export ALCHEMY_API_KEY="${ALCHEMY_KEY}"
export GAS_FEE_SOLANA=true

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Chain:   SOLANA"
echo "  Range:   ${START_DATE} → ${END_DATE}"
date

# Stream logs to GCS every 60s
(while true; do
  sleep 60
  gsutil -q cp /var/log/gas-fees.log \
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
  -e unified-trading-library \
  -e market-tick-data-service

# Quick Solana RPC test
echo "Testing Alchemy Solana RPC..."
python3 -c "
import os, json, urllib.request
key = os.environ.get('ALCHEMY_API_KEY', '')
url = f'https://solana-mainnet.g.alchemy.com/v2/{key}'
payload = json.dumps({'jsonrpc':'2.0','id':1,'method':'getSlot','params':[]}).encode()
req = urllib.request.Request(url, data=payload, headers={'Content-Type':'application/json'})
resp = urllib.request.urlopen(req, timeout=10)
data = json.loads(resp.read())
print(f'Alchemy Solana OK — current slot: {data[\"result\"]}')
" 2>&1 || echo "Alchemy Solana test failed"

# Run Solana gas fee backfill
echo ""
echo "=== Starting Solana gas fee backfill ==="
echo "  Date range: ${START_DATE} → ${END_DATE}"
date

cd market-tick-data-service
GAS_FEE_SOLANA=true python3 -m market_tick_data_service \
  --operation collect-gas-fees \
  --mode batch \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --gas-fee-chains 99999 \
  2>&1

EXIT_CODE=\$?
echo ""
echo "=== Solana gas fee backfill complete (exit=\${EXIT_CODE}) ==="
date

# Final log upload
kill \$LOG_PID 2>/dev/null || true
gsutil -q cp /var/log/gas-fees.log \
  ${GCS_STAGING}/logs/${VM_NAME}_final.log

echo "Shutting down..."
shutdown -h now
STARTUP_EOF

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  rm "$STARTUP_FILE"
else
  # Delete existing VM if present
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  echo "  Creating VM ${VM_NAME}..."
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
echo "Solana VM launched! Monitor:"
echo ""
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/gas-fees.log"
echo ""
echo "  gsutil cat ${GCS_STAGING}/logs/${VM_NAME}_\$(date +%Y%m%d).log | tail -20"
echo ""
echo "Check results:"
echo "  gsutil ls gs://gas-fees-${PROJECT_ID}/gas_fees/chain_id=solana/ | wc -l"
echo "============================================================"
