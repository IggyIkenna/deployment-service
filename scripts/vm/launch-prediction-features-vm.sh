#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# DEPRECATION NOTE (2026-05-08, Phase 8A of features_repo_consolidation_2026_05_08):
# For NEW single-VM prediction features backfills use the consolidated launcher:
#   bash launch-features-vm.sh --feature-family cross_instrument \
#       --asset-group PREDICTION --start-date YYYY-MM-DD --end-date YYYY-MM-DD ...
# This launcher is preserved for its chunk-staging logic (CHUNK_SIZE=7d) the
# consolidated launcher does not model. Will be archived when Phase 7 lands.
#
# Launch GCE VM for Polymarket prediction features computation
#
# Runs features-cross-instrument-service for PREDICTION category.
# Reads tick data from market-data-tick-prediction-{pid} bucket,
# writes features to features-cross-instrument-prediction-{pid} bucket.
#
# Prereqs: tick data must be in GCS (run tick data backfill first).
#
# Usage:
#   bash launch_prediction_features_vm.sh                             # Full run
#   bash launch_prediction_features_vm.sh --dry-run                   # Print plan
#   bash launch_prediction_features_vm.sh --start 2026-03-01 --end 2026-04-04
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2025-03-13}"
END_DATE="${END_DATE:-2026-04-05}"
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
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --start) START_DATE="$2"; shift 2 ;;
    --end) END_DATE="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod)    DEPLOYMENT_ENV_SHORT="prd" ;;
  staging) DEPLOYMENT_ENV_SHORT="stg" ;;
  dev)     DEPLOYMENT_ENV_SHORT="dev" ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# CANONICAL env-tiered bucket (Phase-0f repoint 2026-07-12) — was the legacy flat
# `market-data-tick-prediction-${PROJECT_ID}`, which is being decommissioned; features read
# canonical raw ticks.
GCS_BUCKET="gs://market-data-tick-pred-${DEPLOYMENT_ENV_SHORT}-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/prediction_features"
TARBALL_NAME="prediction_features_codebase.tar.gz"
VM_NAME="${VM_NAME_OVERRIDE:-prediction-features-1}"

# Durable observability: continuous GCS log stream + heartbeat + terminal
# EXIT_STATUS marker, so a dead/hung VM's full log + status are queryable
# without SSH (≤30s loss). SSOT: scripts/vm/lib/launcher_common.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"
LOG_TRAP="$(lc_log_upload_trap_block "${VM_NAME}" "${PROJECT_ID}" "prediction" "prediction-features")"

TOTAL_DAYS=$(python3 -c "
from datetime import datetime
d1 = datetime.strptime('${START_DATE}', '%Y-%m-%d')
d2 = datetime.strptime('${END_DATE}', '%Y-%m-%d')
print((d2-d1).days + 1)
" 2>/dev/null || echo "?")

echo "============================================================"
echo "Prediction Features VM Launcher"
echo "  Project:    ${PROJECT_ID}"
echo "  Zone:       ${ZONE}"
echo "  Machine:    ${MACHINE_TYPE}"
echo "  Range:      ${START_DATE} → ${END_DATE} (${TOTAL_DAYS} days)"
echo "  Chunk:      ${CHUNK_SIZE} days per batch"
echo "  Force:      ${FORCE}"
echo "  Workspace:  ${WORKSPACE_ROOT}"
echo "============================================================"

# ---------- Step 1: Package codebase ----------
echo ""
echo "=== Step 1: Packaging codebase ==="

REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "features-cross-instrument-service"
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

  # Include runtime-topology.yaml so services don't warn about missing config
  TOPOLOGY_SRC="${WORKSPACE_ROOT}/unified-trading-pm/configs/runtime-topology.yaml"
  if [[ -f "${TOPOLOGY_SRC}" ]]; then
    mkdir -p "${STAGING_DIR}/unified-trading-pm/configs"
    cp "${TOPOLOGY_SRC}" "${STAGING_DIR}/unified-trading-pm/configs/"
    echo "  Copied runtime-topology.yaml"
  fi

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

if ! $DRY_RUN; then
  echo "  Uploading tarball..."
  # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
  # account (a short-lived WIF token in an interactive AO slot can't refresh
  # unattended), while `gcloud storage` resolves via ADC, which stays valid. See
  # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
  gcloud storage cp "${TARBALL_PATH}" "${GCS_TARBALL}" --quiet
  echo "  Done."
  rm "${TARBALL_PATH}"
else
  echo "  [DRY RUN] Would upload tarball to ${GCS_STAGING}/"
fi

# ---------- Step 3: Launch VM ----------
echo ""
echo "=== Step 3: Launching VM ==="

FORCE_FLAG=""
if $FORCE; then
  FORCE_FLAG="--force"
fi

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" << 'STARTUP_EOF'
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/prediction_features
export HOME=/root
export PATH="/root/.local/bin:$PATH"
export WORKSPACE_ROOT=/tmp/prediction_features
STARTUP_EOF

# Append variable expansions (not single-quoted). The LOG_TRAP block must lead
# (it sets up tee→/var/log/run.log + the continuous GCS streamer + EXIT trap)
# before any workload runs. Replaces the old exec>(tee ...)-then-upload-at-end
# pattern (which froze + lost logs on a mid-run hang).
cat >> "$STARTUP_FILE" << STARTUP_EOF
${LOG_TRAP}
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Range: ${START_DATE} → ${END_DATE}"
echo "  Chunk: ${CHUNK_SIZE} days"
echo "  Env:   ${DEPLOYMENT_ENV}"
date

# Install Python 3.13
apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

# Download codebase from GCS
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarball..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz

# Create venv + install
echo "Setting up Python venv..."
python3.13 -m venv \${WORK_DIR}/.venv
source \${WORK_DIR}/.venv/bin/activate

# GCS wheel cache — skip compilation for C extensions
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://deployment-scripts-${PROJECT_ID}/wheels/py313-linux-x86_64"
mkdir -p "\$WHEEL_CACHE"
gsutil -m -q cp "\$WHEEL_GCS/*.whl" "\$WHEEL_CACHE/" 2>/dev/null || true

# --no-sources: ignore [tool.uv.sources] path overrides
INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library features-cross-instrument-service; do
  if [[ -d "\${WORK_DIR}/\${pkg}" ]]; then
    INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
  fi
done
# Tarballs have no .git history, so hatch-vcs/setuptools-scm can't detect a
# version for UAC/UTL — pretend-version unblocks the editable install.
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
uv pip install --find-links "\$WHEEL_CACHE" \${INSTALL_ARGS}
uv pip install --find-links "\$WHEEL_CACHE" pandas pyarrow google-cloud-secret-manager google-cloud-storage aiohttp

echo "Verifying imports..."
python3 -c "from features_cross_instrument_service.cli.main import main; print('  features-cross-instrument-service: OK')"

# Generate date list
DATES=\$(python3 -c "
from datetime import datetime, timedelta
start = datetime.strptime('${START_DATE}', '%Y-%m-%d')
end = datetime.strptime('${END_DATE}', '%Y-%m-%d')
current = start
while current <= end:
    print(current.strftime('%Y-%m-%d'))
    current += timedelta(days=1)
")

TOTAL_DATES=\$(echo "\$DATES" | wc -l | tr -d ' ')
DATE_NUM=0

echo ""
echo "============================================================"
echo "Starting features computation: ${START_DATE} → ${END_DATE} (\${TOTAL_DATES} days)"
echo "============================================================"

echo "\$DATES" | while read -r PROC_DATE; do
  DATE_NUM=\$((DATE_NUM + 1))
  echo ""
  echo "--- [\${DATE_NUM}/\${TOTAL_DATES}] \${PROC_DATE} ---"

  set +e
  CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID="${PROJECT_ID}" \\
    "\${WORK_DIR}/.venv/bin/features-cross-instrument" \\
      --operation compute \\
      --mode batch \\
      --asset-group PREDICTION \\
      --date "\${PROC_DATE}" \\
      ${FORCE_FLAG} \\
      --log-level INFO \\
    2>&1
  FEAT_EXIT=\${PIPESTATUS[0]:-\$?}
  set -e

  if [[ \$FEAT_EXIT -ne 0 ]]; then
    echo "WARNING: features date \${PROC_DATE} exited with code \${FEAT_EXIT}"
  fi
done

# Upload log to GCS
gsutil -q cp /var/log/prediction-features.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo "Features complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

echo "--- ${VM_NAME}: ${START_DATE} → ${END_DATE} ---"

if $DRY_RUN; then
  echo "  [DRY RUN] Would create VM: ${VM_NAME}"
  echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
  rm "$STARTUP_FILE"
else
  echo "  Creating VM..."
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
    --labels=purpose=prediction-features,env="${DEPLOYMENT_ENV}",managed-by=deployment-service

  rm "$STARTUP_FILE"
  echo "  VM created: ${VM_NAME}"
fi

echo ""
echo "============================================================"
echo "Prediction Features VM Launched"
echo "  VM:     ${VM_NAME} (${ZONE})"
echo "  Range:  ${START_DATE} → ${END_DATE} (${TOTAL_DAYS} days)"
echo ""
echo "Monitor:"
echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/prediction-features.log"
echo ""
echo "Delete when done:"
echo "  gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} --quiet"
echo "============================================================"
