#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# DEPRECATION NOTE (2026-05-08, Phase 8A of features_repo_consolidation_2026_05_08):
# For NEW single-VM features-sports backfills use the consolidated launcher:
#   bash launch-features-vm.sh --feature-family sports --asset-group SPORTS ...
# This launcher is preserved for its multi-VM chunk-split parallelisation
# (10-way fan-out for multi-year windows) the consolidated launcher does not
# model. Will be archived alongside the legacy features-sports-service repo
# when Phase 7 lands.
#
# Sports Features Parallel Backfill — launch N VMs to cover a date range
#
# Uses the proven GCS tarball + editable install pattern from e2e-testing.
# Splits the date range into N chronological chunks (earliest first per VM,
# no inter-VM dependencies — each VM reads reference data from GCS independently).
#
# Migrated 2026-05-08 (Tab 11) from
# `features-sports-service/scripts/launch_parallel_backfill.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Usage:
#   bash launch-features-sports-parallel-backfill-vm.sh --start 2020-06-01 --end 2025-07-23
#   bash launch-features-sports-parallel-backfill-vm.sh --start 2020-06-01 --end 2025-07-23 --vms 10
#   bash launch-features-sports-parallel-backfill-vm.sh --start 2020-06-01 --end 2025-07-23 --dry-run
#   bash launch-features-sports-parallel-backfill-vm.sh --status           # check all VM progress
#
# Each VM unpacks a codebase tarball, installs editably via uv, then runs
# vm_fss_features.sh for its date chunk. vm_fss_features.sh is read from
# `e2e-testing/scripts/common/vm_fss_features.sh` (preserved at the
# original location — used by other ad-hoc launchers too; will land
# alongside this script in the next migration cycle).
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
NUM_VMS="${NUM_VMS:-10}"
START_DATE=""
END_DATE=""
TABLES=""
DRY_RUN=false
STATUS_ONLY=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# WORKSPACE_ROOT default: 3 levels up (deployment-service/scripts/vm/ →
# unified-trading-system-repos/). Was `../..` at the source location
# (features-sports-service/scripts/); fixed for the new canonical path.
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"

# GCS bucket for staging tarball + logs + progress
GCS_BUCKET="${GCS_BUCKET:-gs://features-sports-${PROJECT_ID}}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/fss_backfill"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --vms) NUM_VMS="$2"; shift 2 ;;
    --tables) TABLES="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --status) STATUS_ONLY=true; shift ;;
    --zone) ZONE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --bucket) GCS_BUCKET="$2"; GCS_STAGING="${GCS_BUCKET}/_vm_staging/fss_backfill"; shift 2 ;;
    --service-account) SERVICE_ACCOUNT="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Status mode — check progress via VM logs in GCS
# ---------------------------------------------------------------------------
if [[ "$STATUS_ONLY" == "true" ]]; then
  echo "============================================================"
  echo "Sports Features Backfill — VM Status"
  echo "============================================================"

  # Canonical durable-log path written by lc_log_upload_trap_block:
  # gs://deployment-scripts-<pid>/vm-logs/<vm_name>/run.log (+ EXIT_STATUS).
  LOG_BASE="gs://deployment-scripts-${PROJECT_ID}/vm-logs"
  LOG_FILES=$(gsutil ls "${LOG_BASE}/fss-backfill-vm-*/run.log" 2>/dev/null || true)
  if [[ -z "$LOG_FILES" ]]; then
    echo "No log files found under ${LOG_BASE}/fss-backfill-vm-*/"
    echo "VMs may still be starting up. Check directly:"
    echo "  gcloud compute instances list --filter='name~fss-backfill-vm'"
    exit 0
  fi

  for FILE in $LOG_FILES; do
    # .../vm-logs/<vm_name>/run.log → <vm_name>
    VM_NAME=$(basename "$(dirname "$FILE")")
    EXIT_STATUS=$(gsutil cat "${LOG_BASE}/${VM_NAME}/EXIT_STATUS" 2>/dev/null || echo "running")
    echo ""
    echo "--- ${VM_NAME} (exit_status=${EXIT_STATUS}) ---"
    gsutil cat "$FILE" 2>/dev/null | rg "(Date |COMPLETE|FAILED|WARNING|FSS Features|VM EXIT)" || true
  done

  echo ""
  echo "------------------------------------------------------------"
  echo "Live VMs:"
  gcloud compute instances list --filter="name~fss-backfill-vm" --format="table(name,zone,status)" 2>/dev/null || echo "  (none)"
  echo "============================================================"
  exit 0
fi

# ---------------------------------------------------------------------------
# Launch mode — validate inputs
# ---------------------------------------------------------------------------
if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Usage: bash scripts/launch_parallel_backfill.sh --start YYYY-MM-DD --end YYYY-MM-DD [--vms N]"
  echo "       bash scripts/launch_parallel_backfill.sh --status"
  exit 1
fi

echo "============================================================"
echo "Sports Features Parallel Backfill"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  VMs:       ${NUM_VMS}"
echo "  Tables:    ${TABLES:-all}"
echo "  Force:     ${FORCE}"
echo "  Dry run:   ${DRY_RUN}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Workspace: ${WORKSPACE_ROOT}"
echo "  Bucket:    ${GCS_BUCKET}"
echo "  Staging:   ${GCS_STAGING}"
echo "============================================================"

# ---------------------------------------------------------------------------
# Split date range into N chunks (Python — handles uneven splits)
# ---------------------------------------------------------------------------
CHUNKS=$(python3 -c "
from datetime import datetime, timedelta
import json

start = datetime.strptime('${START_DATE}', '%Y-%m-%d')
end = datetime.strptime('${END_DATE}', '%Y-%m-%d')
n = ${NUM_VMS}

total_days = (end - start).days
chunk_size = max(1, total_days // n)

chunks = []
for i in range(n):
    c_start = start + timedelta(days=i * chunk_size)
    if i == n - 1:
        c_end = end
    else:
        c_end = start + timedelta(days=(i + 1) * chunk_size - 1)
    if c_start > end:
        break
    c_end = min(c_end, end)
    chunks.append({
        'vm': i + 1,
        'start': c_start.strftime('%Y-%m-%d'),
        'end': c_end.strftime('%Y-%m-%d'),
        'days': (c_end - c_start).days + 1
    })

print(json.dumps(chunks))
")

echo ""
echo "Date range splits:"
echo "$CHUNKS" | python3 -c "
import sys, json
chunks = json.load(sys.stdin)
for c in chunks:
    print(f\"  VM {c['vm']:2d}: {c['start']} → {c['end']}  ({c['days']} days)\")
"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY RUN] Would package tarball, upload to GCS, and launch ${NUM_VMS} VMs."
  exit 0
fi

# ---------------------------------------------------------------------------
# Step 1: Package codebase tarball (same pattern as e2e-testing launchers)
# ---------------------------------------------------------------------------
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "market-tick-data-service"
  "instruments-service"
  "features-service"
)

TARBALL_PATH="/tmp/fss_backfill_codebase.tar.gz"
STAGING_DIR=$(mktemp -d)

for repo in "${REPOS[@]}"; do
  REPO_PATH="${WORKSPACE_ROOT}/${repo}"
  if [[ ! -d "$REPO_PATH" ]]; then
    echo "  WARNING: ${repo} not found at ${REPO_PATH}, skipping"
    continue
  fi
  echo "  Syncing ${repo}..."
  mkdir -p "${STAGING_DIR}/${repo}"
  # Use git ls-files to get the correct file list (respects .gitignore negations)
  # then rsync only those files
  if git -C "${REPO_PATH}" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "${REPO_PATH}" ls-files -z | rsync -a \
      --files-from=- \
      --from0 \
      --exclude='.git' \
      --exclude='.venv*' \
      --exclude='__pycache__' \
      --exclude='*.egg-info' \
      "${REPO_PATH}/" "${STAGING_DIR}/${repo}/"
    # Also include untracked but not ignored files (uncommitted new files)
    git -C "${REPO_PATH}" ls-files --others --exclude-standard -z 2>/dev/null | rsync -a \
      --files-from=- \
      --from0 \
      "${REPO_PATH}/" "${STAGING_DIR}/${repo}/" 2>/dev/null || true
  else
    rsync -a \
      --exclude='.git' \
      --exclude='.venv*' \
      --exclude='__pycache__' \
      --exclude='*.egg-info' \
      --exclude='node_modules' \
      "${REPO_PATH}/" "${STAGING_DIR}/${repo}/"
  fi
done

echo "  Compressing..."
(cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
TARBALL_SIZE=$(du -h "${TARBALL_PATH}" | cut -f1)
echo "  Tarball: ${TARBALL_PATH} (${TARBALL_SIZE})"
rm -rf "${STAGING_DIR}"

# ---------------------------------------------------------------------------
# Step 2: Upload tarball + runner script to GCS
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Uploading to GCS ==="

GCS_TARBALL="${GCS_STAGING}/fss_backfill_codebase.tar.gz"
GCS_RUNNER="${GCS_STAGING}/vm_fss_features.sh"
VM_RUNNER_SCRIPT="${WORKSPACE_ROOT}/e2e-testing/scripts/common/vm_fss_features.sh"

if [[ ! -f "$VM_RUNNER_SCRIPT" ]]; then
  echo "ERROR: vm_fss_features.sh not found at ${VM_RUNNER_SCRIPT}"
  exit 1
fi

echo "  Uploading tarball..."
gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
echo "  Uploading runner script..."
gsutil -q cp "${VM_RUNNER_SCRIPT}" "${GCS_RUNNER}"
echo "  Done."
rm -f "${TARBALL_PATH}"

# ---------------------------------------------------------------------------
# Step 3: Launch VMs
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Launching ${NUM_VMS} VMs ==="

TABLES_FLAG=""
if [[ -n "$TABLES" ]]; then
  TABLES_FLAG="--tables ${TABLES}"
fi

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--no-skip-existing"
fi

launch_vm() {
  local VM_NUM=$1
  local CHUNK_START=$2
  local CHUNK_END=$3
  local VM_NAME="fss-backfill-vm-${VM_NUM}"

  # Durable-log streamer (deployment-service/scripts/vm/lib/launcher_common.sh):
  # continuous canonical-path run.log stream every 30s + heartbeat + terminal
  # EXIT_STATUS marker + guaranteed final upload + shutdown — self-delete-proof
  # observability so the /deployments surface + exit_code monitor see this VM.
  # Replaces the old 60s loop that streamed to a NON-canonical staging path and
  # never wrote the EXIT_STATUS marker the monitor reads.
  local LOG_TRAP
  LOG_TRAP="$(lc_log_upload_trap_block "${VM_NAME}" "${PROJECT_ID}" "SPORTS" "features-sports-parallel-backfill")"

  local STARTUP_FILE
  STARTUP_FILE=$(mktemp)
  cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/fss_backfill
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

${LOG_TRAP}

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Range: ${CHUNK_START} → ${CHUNK_END}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase + runner from GCS
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_RUNNER} \${WORK_DIR}/vm_fss_features.sh
chmod +x \${WORK_DIR}/vm_fss_features.sh

# Unpack tarball
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run backfill via vm_fss_features.sh
bash \${WORK_DIR}/vm_fss_features.sh \\
  --start ${CHUNK_START} \\
  --end ${CHUNK_END} \\
  --skip-fetch \\
  ${TABLES_FLAG} \\
  ${FORCE_FLAG} \\
  --work-dir \${WORK_DIR}

echo "Backfill complete."
date
# run.log + EXIT_STATUS upload + shutdown handled by the lc_log_upload_trap_block
# EXIT trap above (a backfill failure under set -e exits non-zero → the trap
# records that rc as the terminal EXIT_STATUS).
exit 0
STARTUP_EOF

  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

  echo "  Launching ${VM_NAME} (${CHUNK_START} → ${CHUNK_END}) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."

  # Delete existing VM if present (from previous run)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  # Service account flag
  SA_FLAG=""
  if [[ -n "$SERVICE_ACCOUNT" ]]; then
    SA_FLAG="--service-account=${SERVICE_ACCOUNT}"
  fi

  # O-1 β remediation 2026-05-12: observability invariants for ManifestWriter
  # concurrency safety + canonical VM lifecycle metadata.
  METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

  # shellcheck disable=SC2086
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    ${PROVISIONING_FLAGS} \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    ${SA_FLAG} \
    --metadata="${METADATA}" \
    --metadata-from-file=startup-script="${STARTUP_FILE}" \
    --labels=purpose=features-sports-parallel-backfill,env="${DEPLOYMENT_ENV}"

  rm -f "$STARTUP_FILE"
  echo "  ${VM_NAME} created."
}

echo "$CHUNKS" | python3 -c "
import sys, json
chunks = json.load(sys.stdin)
for c in chunks:
    print(f\"{c['vm']} {c['start']} {c['end']}\")
" | while read VM_NUM CHUNK_START CHUNK_END; do
  launch_vm "$VM_NUM" "$CHUNK_START" "$CHUNK_END"
done

echo ""
echo "============================================================"
echo "All ${NUM_VMS} VMs launched."
echo ""
echo "Monitor progress:"
echo "  bash scripts/launch_parallel_backfill.sh --status"
echo ""
echo "Check individual VM logs:"
echo "  gcloud compute ssh fss-backfill-vm-<N> --zone=${ZONE} -- tail -f /var/log/run.log"
echo "  gsutil cat gs://deployment-scripts-${PROJECT_ID}/vm-logs/fss-backfill-vm-<N>/run.log | tail -20"
echo ""
echo "Stop all VMs:"
echo "  for i in \$(seq 1 ${NUM_VMS}); do gcloud compute instances delete fss-backfill-vm-\$i --zone=${ZONE} --quiet; done"
echo "============================================================"
