#!/usr/bin/env bash
# Launch GCE VM for MTDS sports odds backfill (Odds API historical)
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/sports/launch_mtds_backfill_vm.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Distinct from `launch-mtds-backfill-vm.sh` (generic per-AG MTDS
# backfill driver, also migrated this Tab 11 cycle) — this launcher is
# sports-Odds-API-specific.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# 1. Packages the local codebase as a tarball
# 2. Uploads tarball + backfill script to GCS
# 3. Provisions a VM that downloads odds for all dates and shuts down
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Usage:
#   bash launch-mtds-sports-odds-backfill-vm.sh                              # Full 5.8yr run
#   bash launch-mtds-sports-odds-backfill-vm.sh --dry-run                    # Print plan
#   bash launch_mtds_backfill_vm.sh --start 2025-01-04 --end 2025-01-04  # 1-day test
#   bash launch_mtds_backfill_vm.sh --tier 2 --start 2025-01-01 --end 2026-03-28  # Tier 2
#   bash launch-mtds-sports-odds-backfill-vm.sh --env staging  # Staging env tier
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2020-06-01}"
END_DATE="${END_DATE:-2026-03-28}"
TIER="${TIER:-1}"
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-7}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
VM_NAME_OVERRIDE=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --tier) TIER="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Helper `vm_mtds_backfill.sh` was at `../common/` relative to the source
# location (e2e-testing/scripts/sports/). After Tab 11 migration the
# helper sits alongside this launcher in deployment-service/scripts/vm/
# (copied during the #1 migration of the generic launch-mtds-backfill-vm.sh).
COMMON_DIR="${SCRIPT_DIR}"
GCS_BUCKET="gs://market-data-tick-sports-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/mtds_backfill"
TARBALL_NAME="mtds_backfill_codebase.tar.gz"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-odds-1}"

echo "============================================================"
echo "MTDS Odds Backfill VM Launcher"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Tier:      ${TIER}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Workspace: ${WORKSPACE_ROOT}"
echo "============================================================"

# ---------- Step 1: Package codebase ----------
echo ""
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "unified-cloud-interface"
  "unified-config-interface"
  "unified-trading-library"
  "unified-features-interface"
  "unified-reference-data-interface"
  "market-tick-data-service"
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
GCS_SCRIPT="${GCS_STAGING}/vm_mtds_backfill.sh"

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Uploading backfill script..."
  gsutil -q cp "${COMMON_DIR}/vm_mtds_backfill.sh" "${GCS_SCRIPT}"
  echo "  Done."
  rm "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball + script to ${GCS_STAGING}/"
fi

# ---------- Step 3: Launch VM ----------
echo ""
echo "=== Step 3: Launching VM ==="

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/mtds_backfill
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/mtds-backfill.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Range: ${START_DATE} → ${END_DATE}"
echo "  Tier:  ${TIER}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase + script from GCS
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_mtds_backfill.sh
chmod +x \${WORK_DIR}/vm_mtds_backfill.sh

# Unpack tarball
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run backfill
bash \${WORK_DIR}/vm_mtds_backfill.sh \\
  --asset-group SPORTS \\
  --tier ${TIER} \\
  --start ${START_DATE} \\
  --end ${END_DATE} \\
  --chunk-size ${CHUNK_SIZE} \\
  ${FORCE_FLAG} \\
  --work-dir \${WORK_DIR}

# Upload log to GCS
gsutil -q cp /var/log/mtds-backfill.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo "Backfill complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

echo "--- ${VM_NAME}: ${START_DATE} → ${END_DATE} (tier ${TIER}) ---"

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  rm "$STARTUP_FILE"
else
  echo "  Creating VM..."
  # Delete existing VM if present (from previous run)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

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
    --boot-disk-size=50GB \
    --metadata="${METADATA}" \
    --metadata-from-file=startup-script="${STARTUP_FILE}" \
    --labels=purpose=mtds-sports-odds-backfill,env="${DEPLOYMENT_ENV}"

  rm "$STARTUP_FILE"
  echo "  VM created: ${VM_NAME}"
  echo ""
  echo "  Monitor: gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/mtds-backfill.log"
  echo "  Logs:    gsutil cat ${GCS_STAGING}/logs/${VM_NAME}.log"
fi

echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
