#!/usr/bin/env bash
# Full API Football sweep — covers ALL dates 2019-01-01 → today
#
# Strategy:
#   1. Package codebase and upload to GCS exactly ONCE
#   2. Launch 8 year-chunk VMs in parallel (one per calendar year)
#   3. Each VM uses entity-level manifest checking → skips already-done API calls
#   4. No --force: existing manifest entries are preserved
#
# This covers ALL API_FOOTBALL entities:
#   fixtures, leagues, teams, standings, injuries,
#   fixture_stats, fixture_events, fixture_lineups, player_stats
#
# Usage:
#   bash full_api_football_sweep.sh
#   bash full_api_football_sweep.sh --dry-run
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "${SCRIPT_DIR}/../common" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

GCS_BUCKET="gs://instruments-store-sports-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/instruments_reference"
TARBALL_NAME="instruments_reference_codebase.tar.gz"
TARBALL_PATH="/tmp/${TARBALL_NAME}"
GCS_TARBALL="${GCS_STAGING}/${TARBALL_NAME}"
GCS_SCRIPT="${GCS_STAGING}/vm_instruments_reference.sh"

# Year-chunked ranges for complete coverage
# Format: "vm-name|start|end"
RANGES=(
  "sports-full-sweep-2019|2019-01-01|2019-12-31"
  "sports-full-sweep-2020|2020-01-01|2020-12-31"
  "sports-full-sweep-2021|2021-01-01|2021-12-31"
  "sports-full-sweep-2022|2022-01-01|2022-12-31"
  "sports-full-sweep-2023|2023-01-01|2023-12-31"
  "sports-full-sweep-2024|2024-01-01|2024-12-31"
  "sports-full-sweep-2025|2025-01-01|2025-12-31"
  "sports-full-sweep-2026|2026-01-01|2026-04-10"
)

echo "============================================================"
echo "API Football Full Sweep"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VMs:      ${#RANGES[@]} (one per year, parallel)"
echo "  DryRun:   ${DRY_RUN}"
echo "============================================================"
echo ""

# ---- Step 1: Package codebase ONCE ----
echo "=== Step 1: Packaging codebase (once for all VMs) ==="
REPOS=("unified-api-contracts" "unified-trading-library" "instruments-service")
if ! $DRY_RUN; then
  STAGING_DIR=$(mktemp -d)
  for repo in "${REPOS[@]}"; do
    echo "  Syncing ${repo}..."
    mkdir -p "${STAGING_DIR}/${repo}"
    rsync -a \
      --exclude='.git' \
      --exclude='.venv*' \
      --exclude='__pycache__' \
      --exclude='*.egg-info' \
      --exclude='node_modules' \
      --exclude='.mypy_cache' \
      --exclude='.pytest_cache' \
      --exclude='tests' \
      --exclude='htmlcov' \
      --exclude='.coverage*' \
      "${WORKSPACE_ROOT}/${repo}/" "${STAGING_DIR}/${repo}/"
  done
  echo "  Compressing..."
  (cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
  TARBALL_SIZE=$(du -h "${TARBALL_PATH}" | cut -f1)
  echo "  Tarball: ${TARBALL_PATH} (${TARBALL_SIZE})"
  rm -rf "${STAGING_DIR}"
else
  echo "  [DRY RUN] Would package tarball"
fi

# ---- Step 2: Upload to GCS ONCE ----
echo ""
echo "=== Step 2: Uploading to GCS (once for all VMs) ==="
if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Uploading vm script..."
  gsutil -q cp "${COMMON_DIR}/vm_instruments_reference.sh" "${GCS_SCRIPT}"
  rm "${TARBALL_PATH}"
  echo "  Done → ${GCS_STAGING}/"
else
  echo "  [DRY RUN] Would upload to ${GCS_STAGING}/"
fi

# ---- Step 3: Launch all VMs in parallel ----
echo ""
echo "=== Step 3: Launching ${#RANGES[@]} VMs in parallel ==="

launch_vm() {
  local vm_name="$1"
  local start_date="$2"
  local end_date="$3"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${vm_name} (${start_date} → ${end_date})"
    return 0
  fi

  STARTUP_FILE=$(mktemp)
  # shellcheck disable=SC2cat
  cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/instruments
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/instruments-reference.log) 2>&1

# Production service flags — explicit to prevent any env pollution on the VM
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export DATA_MODE=real
export IS_TEST_RUN=false

echo "=== VM Startup: ${vm_name} ==="
echo "  Range: ${start_date} → ${end_date}"
date

apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

mkdir -p \${WORK_DIR}
echo "Downloading codebase from GCS..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_instruments_reference.sh
chmod +x \${WORK_DIR}/vm_instruments_reference.sh
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz

bash \${WORK_DIR}/vm_instruments_reference.sh \\
  --start ${start_date} \\
  --end ${end_date} \\
  --work-dir \${WORK_DIR}

gsutil -q cp /var/log/instruments-reference.log \\
  ${GCS_STAGING}/logs/${vm_name}.log

echo "Backfill complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

  # Delete any existing VM with same name
  gcloud compute instances delete "${vm_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  gcloud compute instances create "${vm_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB \
    --metadata-from-file=startup-script="${STARTUP_FILE}"

  rm "$STARTUP_FILE"
  echo "  Created: ${vm_name} (${start_date} → ${end_date})"
}

# Launch all in parallel
PIDS=()
for entry in "${RANGES[@]}"; do
  IFS='|' read -r vm_name start_date end_date <<< "${entry}"
  launch_vm "${vm_name}" "${start_date}" "${end_date}" &
  PIDS+=($!)
done

# Wait for all VM creation jobs to complete
FAILED=0
for pid in "${PIDS[@]}"; do
  if ! wait "${pid}"; then
    FAILED=$((FAILED + 1))
  fi
done

echo ""
echo "============================================================"
if [[ ${FAILED} -gt 0 ]]; then
  echo "WARNING: ${FAILED} VM creation(s) failed"
else
  echo "All ${#RANGES[@]} VMs launched successfully"
fi
echo "============================================================"
echo ""
echo "Monitor logs (once VMs complete):"
for entry in "${RANGES[@]}"; do
  IFS='|' read -r vm_name _ _ <<< "${entry}"
  echo "  gsutil cat ${GCS_STAGING}/logs/${vm_name}.log"
done
echo ""
echo "Check VM status:"
echo "  gcloud compute instances list --filter=\"name~'sports-full-sweep-'\" --zones=${ZONE}"
