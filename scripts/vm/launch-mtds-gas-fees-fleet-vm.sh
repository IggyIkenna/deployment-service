#!/usr/bin/env bash
# Launch 8 GCE VMs for full gas fee backfill — one VM per chain.
#
# Each VM uses chain-specific start dates from UAC GAS_FEE_CHAIN_START_DATES.
# Retry logic (12 retries, exponential backoff) is baked into the gas fee client.
# All VMs share a single codebase tarball on GCS.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Usage:
#   bash launch_gas_fees_fleet.sh                  # Launch all 8 VMs
#   bash launch_gas_fees_fleet.sh --dry-run        # Print plan only
#   bash launch_gas_fees_fleet.sh --chain 1        # Single chain only
#   bash launch_gas_fees_fleet.sh --env staging    # Staging env tier
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
SINGLE_CHAIN=""
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
END_DATE="$(date +%Y-%m-%d)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Chain configs: chain_id:name:start_date (from UAC GAS_FEE_CHAIN_START_DATES)
CHAINS=(
  "1:ethereum:2020-01-01"
  "10:optimism:2021-11-12"
  "56:bsc:2020-09-01"
  "137:polygon:2020-06-01"
  "8453:base:2023-08-09"
  "42161:arbitrum:2021-09-01"
  "43114:avalanche:2020-09-22"
  "59144:linea:2023-07-12"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --chain) SINGLE_CHAIN="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

GCS_STAGING="gs://market-data-tick-defi-${PROJECT_ID}/_vm_staging/gas_fees"
TARBALL_NAME="gas_fees_codebase.tar.gz"

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "unified-trading-library"
  "market-tick-data-service"
)

# ---------- Step 1: Package codebase (once) ----------
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

# ---------- Step 2: Upload to GCS (once) ----------
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

# ---------- Step 3: Fetch Alchemy API key ----------
ALCHEMY_KEY=$(gcloud secrets versions access latest --secret=alchemy-api-key --project="${PROJECT_ID}" 2>/dev/null || echo "")
if [[ -z "$ALCHEMY_KEY" ]]; then
  echo "ERROR: Could not fetch alchemy-api-key from Secret Manager"
  exit 1
fi

# ---------- Step 4: Launch VMs ----------
echo ""
echo "=== Step 4: Launching VMs ==="

launch_vm() {
  local CHAIN_ID="$1"
  local CHAIN_NAME="$2"
  local START_DATE="$3"
  local VM_NAME="mtds-gas-fees-${CHAIN_NAME}"

  echo ""
  echo "--- ${CHAIN_NAME} (chain_id=${CHAIN_ID}): ${START_DATE} → ${END_DATE} ---"

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
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Chain:   ${CHAIN_NAME} (${CHAIN_ID})"
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

# Quick API test
echo "Testing Alchemy API..."
python3 -c "
import os, json, urllib.request
key = os.environ.get('ALCHEMY_API_KEY', '')
url = f'https://eth-mainnet.g.alchemy.com/v2/{key}'
payload = json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_blockNumber','params':[]}).encode()
req = urllib.request.Request(url, data=payload, headers={'Content-Type':'application/json'})
resp = urllib.request.urlopen(req, timeout=10)
data = json.loads(resp.read())
print(f'Alchemy OK — latest block: {int(data[\"result\"], 16)}')
" 2>&1 || echo "Alchemy test failed"

# Run gas fee backfill for this chain
echo ""
echo "=== Starting gas fee backfill: ${CHAIN_NAME} ==="
echo "  Chain ID:   ${CHAIN_ID}"
echo "  Date range: ${START_DATE} → ${END_DATE}"
date

cd market-tick-data-service
python3 -m market_tick_data_service \
  --operation collect-gas-fees \
  --mode batch \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --gas-fee-chains ${CHAIN_ID} \
  --gas-fee-sample-interval 100 \
  2>&1

EXIT_CODE=\$?
echo ""
echo "=== Gas fee backfill complete: ${CHAIN_NAME} (exit=\${EXIT_CODE}) ==="
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
    # O-1 β remediation 2026-05-12: observability invariants for ManifestWriter
    # concurrency safety + canonical VM lifecycle metadata.
    METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    METADATA="${METADATA},VM_NAME=${VM_NAME}"
    METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
    METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

    gcloud compute instances create "${VM_NAME}" \
      --project="${PROJECT_ID}" \
      --zone="${ZONE}" \
      --machine-type="${MACHINE_TYPE}" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --metadata="${METADATA}" \
      --metadata-from-file=startup-script="${STARTUP_FILE}" \
      --boot-disk-size=50GB \
      --boot-disk-type=pd-ssd \
      --labels=purpose=mtds-gas-fees-fleet,env="${DEPLOYMENT_ENV}",chain="${CHAIN_NAME}"
    echo "  VM ${VM_NAME} created."
    rm "$STARTUP_FILE"
  fi
}

# Launch VMs
for chain_spec in "${CHAINS[@]}"; do
  IFS=':' read -r CHAIN_ID CHAIN_NAME START_DATE <<< "$chain_spec"

  # If --chain specified, only launch that one
  if [[ -n "$SINGLE_CHAIN" && "$CHAIN_ID" != "$SINGLE_CHAIN" ]]; then
    continue
  fi

  launch_vm "$CHAIN_ID" "$CHAIN_NAME" "$START_DATE"
done

echo ""
echo "============================================================"
echo "Fleet launched! Monitor all VMs:"
echo ""
echo "  gcloud compute instances list --filter='name~mtds-gas-fees' --project=${PROJECT_ID}"
echo ""
echo "SSH into a specific VM:"
echo "  gcloud compute ssh mtds-gas-fees-ethereum --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/gas-fees.log"
echo ""
echo "Check GCS logs:"
echo "  gsutil ls ${GCS_STAGING}/logs/"
echo ""
echo "Check results per chain:"
echo "  for c in 1 10 56 137 8453 42161 43114 59144; do"
echo "    echo \"chain_id=\$c: \$(gsutil ls gs://gas-fees-${PROJECT_ID}/gas_fees/chain_id=\$c/ 2>/dev/null | wc -l) dates\""
echo "  done"
echo "============================================================"
