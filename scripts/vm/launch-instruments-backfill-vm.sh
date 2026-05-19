#!/usr/bin/env bash
# Launch GCE VMs for parallel instruments backfill
#
# 1. Packages the local codebase (respecting .gitignore) as a tarball
# 2. Uploads tarball + backfill script to GCS
# 3. Provisions VMs that download, unpack, install, and run
# VMs auto-shutdown after completion.
#
# Migrated 2026-05-08 (Tab 11) from
# `e2e-testing/scripts/common/launch_instruments_backfill_vms.sh` per the
# "VM launcher script SSOT" rule (CLAUDE.md). Canonical home: this file.
# Also fills the `_SERVICE_LAUNCHER_SCRIPTS["instruments-service"]` entry
# in `deployment-api/deployment_api/services/deploy_missing.py` (was
# missing on disk; Deploy-Missing UI button silently broke for
# instruments-service).
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md.
#
# Usage:
#   bash launch-instruments-backfill-vm.sh               # Launch all 5 VMs
#   bash launch-instruments-backfill-vm.sh --dry-run      # Print plan without executing
#
# VM allocation (5 VMs):
#   VM1: CeFi  2020-01-01 → 2022-06-30
#   VM2: CeFi  2022-07-01 → 2024-12-31
#   VM3: CeFi  2025-01-01 → 2026-02-28
#   VM4: DeFi  2020-01-01 → 2026-02-28  (fetches universe once, date-filters)
#   VM5: TradFi 2020-01-01 → 2026-02-28
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Manifest-driven skip is the default. --force ⇒ bypass manifest, refetch all day-shards.
if $FORCE; then
  FORCE_FLAG_INNER="--force"
  echo "MODE: --force ON — manifest-driven skip BYPASSED. Every day-shard will be re-fetched."
else
  FORCE_FLAG_INNER=""
  echo "MODE: --force OFF (default) — manifest-driven skip ACTIVE. Already-captured + empty_confirmed shards will be skipped."
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GCS_BUCKET="gs://instruments-store-defi-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging"
TARBALL_NAME="instruments_backfill_codebase.tar.gz"

# ---------- Step 1: Package codebase ----------
echo "=== Step 1: Packaging codebase ==="

# Repos needed for instruments-service to run.
# Only repos present in the workspace are archived. The install script
# dynamically skips repos not in the tarball — their deps resolve from PyPI.
REPOS=(
  "unified-api-contracts"
  "unified-trading-library"
  "instruments-service"
)

TARBALL_PATH="/tmp/${TARBALL_NAME}"

if ! $DRY_RUN; then
  echo "  Creating tarball from workspace: ${WORKSPACE_ROOT}"
  # rsync copies working tree (including uncommitted changes) while
  # respecting .gitignore via --filter=':- .gitignore'.  This is
  # preferred over git-archive which only exports committed files.
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

# ---------- Step 3: Launch VMs ----------
echo ""
echo "=== Step 3: Launching VMs ==="

# VM definitions: name|category|start|end
declare -a VMS=(
  "instr-backfill-cefi-1|CEFI|2020-01-01|2022-06-30"
  "instr-backfill-cefi-2|CEFI|2022-07-01|2024-12-31"
  "instr-backfill-cefi-3|CEFI|2025-01-01|2026-02-28"
  "instr-backfill-defi|DEFI|2020-01-01|2026-02-28"
  "instr-backfill-tradfi|TRADFI|2020-01-01|2026-02-28"
  "instr-backfill-sports|SPORTS|2020-06-01|2026-03-28"
)

for VM_DEF in "${VMS[@]}"; do
  IFS='|' read -r VM_NAME ASSET_GROUP START_DATE END_DATE <<< "$VM_DEF"

  # Write startup script to a temp file (avoids quoting issues)
  STARTUP_FILE=$(mktemp)
  cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/instruments_backfill
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/instruments-backfill.log) 2>&1

# Set GCP project for ServiceRuntime
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
# Env tier for bucket-resolution (Phase 0f, 2026-05-11). resolve_bucket_name(env=...)
# reads DEPLOYMENT_ENV; this VM operates entirely against \${DEPLOYMENT_ENV}-tier buckets.
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Category: ${ASSET_GROUP}"
echo "  Range: ${START_DATE} → ${END_DATE}"
date

# Install Python 3.13 from deadsnakes PPA (uses system OpenSSL + cert store)
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
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_instruments_backfill.sh
chmod +x \${WORK_DIR}/vm_instruments_backfill.sh

# Unpack codebase
echo "Unpacking codebase..."
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz
ls -d \${WORK_DIR}/*/

# Run backfill — honest_coverage SSOT: without --force, manifest-driven skip
# applies (capture_status in {captured,empty_confirmed} → skipped;
# attempted_failed + expected_unattempted → retried). Outer launcher's
# --force flag propagates here as $FORCE_FLAG_INNER, default empty.
echo "Starting backfill..."
bash \${WORK_DIR}/vm_instruments_backfill.sh \\
  --asset-group ${ASSET_GROUP} \\
  --start ${START_DATE} \\
  --end ${END_DATE} \\
  ${FORCE_FLAG_INNER} \\
  --work-dir \${WORK_DIR}

# Upload log to GCS
gsutil -q cp /var/log/instruments-backfill.log \\
  ${GCS_STAGING}/logs/${VM_NAME}.log

echo "Backfill complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

  echo ""
  echo "--- ${VM_NAME}: ${ASSET_GROUP} ${START_DATE} → ${END_DATE} ---"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    echo "  Machine: ${MACHINE_TYPE}, Zone: ${ZONE}"
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
      --metadata-from-file=startup-script="${STARTUP_FILE}" \
      --metadata="${METADATA}" \
      --boot-disk-size=50GB \
      --boot-disk-type=pd-ssd \
      --labels=purpose=instruments-backfill,asset-group="${ASSET_GROUP,,}",env="${DEPLOYMENT_ENV}"
    echo "  VM ${VM_NAME} created."
    rm "$STARTUP_FILE"
  fi
done

echo ""
echo "============================================================"
echo "All VMs launched."
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='name~instr-backfill' --project=${PROJECT_ID}"
echo ""
echo "SSH + tail log:"
echo "  gcloud compute ssh instr-backfill-cefi-1 --zone=${ZONE} --project=${PROJECT_ID} -- tail -f /var/log/instruments-backfill.log"
echo ""
echo "Check GCS results:"
echo "  gsutil ls ${GCS_STAGING}/logs/"
echo "  gsutil ls gs://instruments-store-cefi-${PROJECT_ID}/instrument_availability/by_date/ | wc -l"
echo "  gsutil ls gs://instruments-store-defi-${PROJECT_ID}/instrument_availability/by_date/ | wc -l"
echo "  gsutil ls gs://instruments-store-tradfi-${PROJECT_ID}/instrument_availability/by_date/ | wc -l"
echo "============================================================"
