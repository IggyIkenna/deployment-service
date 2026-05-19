#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch 1 GCE VM for targeted DeFi instruments backfill
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/common/launch_defi_backfill_vm.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# VM_NAME `instr-backfill-defi-targeted` matches the existing
# `instr-backfill-defi` watchdog prefix added during Tab 11 migration #2.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Backfills 7 venues with full historical data:
#   CURVE-AVALANCHE, CURVE-OPTIMISM, BALANCER-ETHEREUM,
#   UNISWAPV3-ETHEREUM, UNISWAPV3-POLYGON, RAYDIUM-SOLANA, UNISWAPV4-ETHEREUM
#
# Usage:
#   bash launch_defi_backfill_vm.sh               # Launch VM
#   bash launch_defi_backfill_vm.sh --dry-run      # Print plan only
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"
GCS_BUCKET="gs://instruments-store-defi-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging"
TARBALL_NAME="instruments_backfill_codebase.tar.gz"

VENUES="CURVE-AVALANCHE CURVE-OPTIMISM BALANCER-ETHEREUM UNISWAPV3-ETHEREUM UNISWAPV3-POLYGON RAYDIUM-SOLANA UNISWAPV4-ETHEREUM"
VM_NAME="instr-backfill-defi-targeted"
START_DATE="2020-01-01"
END_DATE="2026-04-04"

# ---------- Step 1: Package codebase ----------
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "instruments-service"
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
      --include='unified_api_contracts/registry/data/' \
      --include='unified_api_contracts/registry/data/**' \
      --include='unified_api_contracts/canonical/domain/sports/data/' \
      --include='unified_api_contracts/canonical/domain/sports/data/**' \
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
GCS_SCRIPT="${GCS_STAGING}/vm_instruments_backfill.sh"

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Uploading backfill script..."
  gsutil -q cp "${SCRIPT_DIR}/vm_instruments_backfill.sh" "${GCS_SCRIPT}"
  echo "  Done."
  rm "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball + script to ${GCS_STAGING}/"
fi

# ---------- Step 3: Launch VM ----------
echo ""
echo "=== Step 3: Launching VM ==="
echo "  VM:       ${VM_NAME}"
echo "  Category: DEFI"
echo "  Venues:   ${VENUES}"
echo "  Range:    ${START_DATE} → ${END_DATE}"

STARTUP_FILE=$(mktemp)
LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT_ID")"
cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
${LOG_TRAP}
set -euo pipefail
export WORK_DIR=/tmp/instruments_backfill
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/instruments-backfill.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Category: DEFI"
echo "  Venues:   ${VENUES}"
echo "  Range:    ${START_DATE} → ${END_DATE}"
echo "  Env:      ${DEPLOYMENT_ENV}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase + script
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_instruments_backfill.sh
chmod +x \${WORK_DIR}/vm_instruments_backfill.sh

# Unpack
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run backfill with venue filter
echo "Starting backfill..."
bash \${WORK_DIR}/vm_instruments_backfill.sh \\
  --asset-group DEFI \\
  --start ${START_DATE} \\
  --end ${END_DATE} \\
  --force \\
  --venues "${VENUES}" \\
  --work-dir \${WORK_DIR}

# Upload log
gsutil -q cp /var/log/instruments-backfill.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo "Backfill complete. Shutting down..."
date
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
  echo "  Creating VM..."
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
    --labels=purpose=defi-backfill,env="${DEPLOYMENT_ENV}" \
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
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/instruments-backfill.log"
echo ""
echo "Check GCS results:"
echo "  gsutil ls ${GCS_STAGING}/logs/"
echo "  gsutil ls gs://instruments-store-defi-${PROJECT_ID}/instrument_availability/by_date/ | wc -l"
echo "============================================================"
