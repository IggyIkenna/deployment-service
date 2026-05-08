#!/usr/bin/env bash
# Launch 3 GCE VMs for instruments-service sports reference data backfill (v3)
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/sports/launch_instruments_reference_v3.sh` per
# the "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this
# file. Supersedes the older `launch_instruments_reference_vm.sh` (v1)
# at the source location; v1 is deferred to a follow-up cycle.
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Key improvements over v2:
#   - Per-entity skip logic: orchestrator checks manifest per entity (10 entities)
#     and only fetches those that are actually missing. No wasted API calls.
#   - Blank manifest entries: zero-fixture dates write row_count=0 for all 4
#     per-fixture entities so they're never re-fetched.
#   - GCS fixture ID resolution: enrichment-only mode reads fixture IDs from
#     existing GCS parquet instead of re-calling API Football (saves 33 calls/date).
#   - Single process: full date range in one invocation so module-level caches
#     (leagues/teams/standings DataFrames) persist across dates.
#
# Date range: 2020-06-01 → 2026-04-10 (~2,141 days), split across 3 VMs:
#   VM1: 2020-06-01 → 2022-05-31 (~730 days)
#   VM2: 2022-06-01 → 2024-05-31 (~730 days)
#   VM3: 2024-06-01 → 2026-04-10 (~681 days)
#
# Usage:
#   bash launch_instruments_reference_v3.sh                    # Launch + schedule
#   bash launch_instruments_reference_v3.sh --dry-run          # Print plan only
#   bash launch_instruments_reference_v3.sh --now              # Launch immediately
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
START_NOW=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --now) START_NOW=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Helper `vm_instruments_reference.sh` was at `../common/` relative to the
# source location (e2e-testing/scripts/sports/). After Tab 11 migration
# the helper sits alongside this launcher in deployment-service/scripts/vm/.
COMMON_DIR="${SCRIPT_DIR}"
GCS_BUCKET="gs://instruments-store-sports-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/instruments_reference_v3"
TARBALL_NAME="instruments_reference_codebase_v3.tar.gz"

# 3 VM date ranges
VM_CONFIGS=(
  "sports-ref-v3-1:2020-06-01:2022-05-31"
  "sports-ref-v3-2:2022-06-01:2024-05-31"
  "sports-ref-v3-3:2024-06-01:2026-04-10"
)

echo "============================================================"
echo "Instruments Reference Data VM Launcher (v3)"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Workspace: ${WORKSPACE_ROOT}"
echo "  VMs:       ${#VM_CONFIGS[@]}"
for cfg in "${VM_CONFIGS[@]}"; do
  IFS=':' read -r name start end <<< "$cfg"
  echo "    ${name}: ${start} → ${end}"
done
echo "============================================================"

# ---------- Step 1: Package codebase ----------
echo ""
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
GCS_SCRIPT="${GCS_STAGING}/vm_instruments_reference.sh"

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  echo "  Uploading backfill script..."
  gsutil -q cp "${COMMON_DIR}/vm_instruments_reference.sh" "${GCS_SCRIPT}"
  echo "  Done."
  rm "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball + script to ${GCS_STAGING}/"
fi

# ---------- Step 3: Create VMs ----------
echo ""
echo "=== Step 3: Creating VMs ==="

for cfg in "${VM_CONFIGS[@]}"; do
  IFS=':' read -r VM_NAME START_DATE END_DATE <<< "$cfg"

  STARTUP_FILE=$(mktemp)
  cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/instruments
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/instruments-reference.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Range: ${START_DATE} → ${END_DATE}"
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
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_instruments_reference.sh
chmod +x \${WORK_DIR}/vm_instruments_reference.sh

# Unpack tarball
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run instruments-service backfill
bash \${WORK_DIR}/vm_instruments_reference.sh \\
  --start ${START_DATE} \\
  --end ${END_DATE} \\
  --work-dir \${WORK_DIR}

# Upload log to GCS
gsutil -q cp /var/log/instruments-reference.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo "Backfill complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

  echo "--- ${VM_NAME}: ${START_DATE} → ${END_DATE} ---"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    rm "$STARTUP_FILE"
    continue
  fi

  # Delete existing VM if present
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  if $START_NOW; then
    # Create and start immediately
    gcloud compute instances create "${VM_NAME}" \
      --project="${PROJECT_ID}" \
      --zone="${ZONE}" \
      --machine-type="${MACHINE_TYPE}" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size=30GB \
      --metadata-from-file=startup-script="${STARTUP_FILE}"
    echo "  VM created and RUNNING: ${VM_NAME}"
  else
    # Create in stopped state for scheduled start
    gcloud compute instances create "${VM_NAME}" \
      --project="${PROJECT_ID}" \
      --zone="${ZONE}" \
      --machine-type="${MACHINE_TYPE}" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size=30GB \
      --no-address \
      --metadata-from-file=startup-script="${STARTUP_FILE}"

    # Stop the VM immediately (it starts in RUNNING by default)
    echo "  Stopping VM for scheduled start..."
    gcloud compute instances stop "${VM_NAME}" \
      --project="${PROJECT_ID}" \
      --zone="${ZONE}" \
      --quiet
    echo "  VM created in TERMINATED state: ${VM_NAME}"
  fi

  rm "$STARTUP_FILE"
done

# ---------- Step 4: Create Cloud Scheduler jobs ----------
if ! $START_NOW && ! $DRY_RUN; then
  echo ""
  echo "=== Step 4: Creating Cloud Scheduler jobs (midnight April 11 UTC) ==="

  # Service account for Compute Engine API calls
  SA_EMAIL="$(gcloud iam service-accounts list \
    --project="${PROJECT_ID}" \
    --filter="email~compute" \
    --format="value(email)" | head -1)"

  if [[ -z "$SA_EMAIL" ]]; then
    SA_EMAIL="${PROJECT_ID//-/_}@appspot.gserviceaccount.com"
    echo "  WARNING: No compute SA found, using default: ${SA_EMAIL}"
  fi

  for cfg in "${VM_CONFIGS[@]}"; do
    IFS=':' read -r VM_NAME _ _ <<< "$cfg"
    JOB_NAME="${VM_NAME}-start"

    # Delete existing job if present
    gcloud scheduler jobs delete "${JOB_NAME}" \
      --project="${PROJECT_ID}" \
      --location=asia-northeast1 \
      --quiet 2>/dev/null || true

    # Schedule VM start at midnight April 11 UTC (= 09:00 JST)
    # Cron: minute hour day month day-of-week
    gcloud scheduler jobs create http "${JOB_NAME}" \
      --project="${PROJECT_ID}" \
      --location=asia-northeast1 \
      --schedule="0 0 11 4 *" \
      --time-zone="UTC" \
      --uri="https://compute.googleapis.com/compute/v1/projects/${PROJECT_ID}/zones/${ZONE}/instances/${VM_NAME}/start" \
      --http-method=POST \
      --oauth-service-account-email="${SA_EMAIL}" \
      --oauth-token-scope="https://www.googleapis.com/auth/compute" \
      --attempt-deadline=60s

    echo "  Scheduled: ${JOB_NAME} → start ${VM_NAME} at 2026-04-11 00:00 UTC"
  done
fi

echo ""
echo "============================================================"
echo "Done."
if $START_NOW; then
  echo "VMs are RUNNING now."
else
  echo "VMs will start at 2026-04-11 00:00 UTC (midnight)."
fi
echo ""
echo "Monitor:"
for cfg in "${VM_CONFIGS[@]}"; do
  IFS=':' read -r VM_NAME _ _ <<< "$cfg"
  echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/instruments-reference.log"
done
echo ""
echo "Logs after completion:"
for cfg in "${VM_CONFIGS[@]}"; do
  IFS=':' read -r VM_NAME _ _ <<< "$cfg"
  echo "  gsutil cat ${GCS_STAGING}/logs/${VM_NAME}.log"
done
echo "============================================================"
