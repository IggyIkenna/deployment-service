#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that runs the Phase 3C Aave V3 lending rate validation harness.
#
# Validates that LendingRateImpactCalculator.post_trade_rate() matches real
# on-chain Aave V3 supply-rate changes within ±10 bps for ≥90% of historical
# large-supply events (>10M token, Sep 2025 → May 2026).
#
# VM outputs:
#   gs://{PROJECT_ID}-defi-validation/results/lending/{DATE}/{CORRELATION_ID}/results.json
#
# Event stream:
#   gs://{PROJECT_ID}-events/events/lending-rate-validation/{DATE}/{CORRELATION_ID}/hour={H}/*.jsonl
#   Required: STARTED within 60s + STOPPED/FAILED at exit.
#
# Singleton lock: Alchemy is a shared API key; concurrent VMs thrash on
# per-IP rate limits. Refuses to launch if any aave-lending-rate-val-* VM
# is RUNNING in the zone. Pass --force to bypass.
#
# Watchdog registration: VM prefix "aave-lending-rate-val-" is registered
# in VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py (None = heartbeat-only
# since this VM does NOT write per-VM manifest shards).
#
# Usage:
#   bash launch-aave-lending-rate-validation-vm.sh              # default block range
#   bash launch-aave-lending-rate-validation-vm.sh --force      # bypass singleton lock
#   bash launch-aave-lending-rate-validation-vm.sh --dry-run    # print plan only
#   bash launch-aave-lending-rate-validation-vm.sh \
#       --block-start 20800000 --block-end 22500000             # explicit range
#
# Event verification after launch:
#   gcloud compute instances list --filter="name~^aave-lending-rate-val-" --project=central-element-323112
#   gsutil ls gs://central-element-323112-events/events/lending-rate-validation/$(date +%Y-%m-%d)/
#   gsutil ls gs://central-element-323112-defi-validation/results/lending/$(date +%Y-%m-%d)/
#
# WATCHDOG NOTE: after editing VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py,
# operator MUST relaunch the watchdog VM:
#   gcloud compute instances delete vm-zombie-watchdog-... --zone=asia-northeast1-c --quiet
#   bash deployment-service/scripts/vm/launch-vm-zombie-watchdog.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-a}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
BLOCK_START="${BLOCK_START:-23300000}"
BLOCK_END="${BLOCK_END:-25086000}"
TARGET_EVENTS="${TARGET_EVENTS:-60}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --block-start) BLOCK_START="$2"; shift 2 ;;
    --block-end) BLOCK_END="$2"; shift 2 ;;
    --target-events) TARGET_EVENTS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ── Singleton lock: Alchemy shared key → one VM at a time ──
VM_PREFIX="aave-lending-rate-val-"
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Aave lending rate validation VM already running in ${ZONE}: ${EXISTING}
Refusing duplicate launch — Alchemy is a shared key; concurrent VMs hit per-IP rate limits.

Options:
  Inspect:  gcloud compute instances describe ${EXISTING} --zone=${ZONE} --project=${PROJECT_ID}
  Events:   gsutil ls gs://${PROJECT_ID}-events/events/lending-rate-validation/$(date +%Y-%m-%d)/
  Force:    bash $0 --force

CAUTION — do NOT delete ${EXISTING} unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete ${EXISTING} --zone=${ZONE} --project=${PROJECT_ID} --quiet
EOF
    exit 1
  fi
fi

# ── Fetch Alchemy API key from Secret Manager ──
if ! $DRY_RUN; then
  ALCHEMY_KEY="$(gcloud secrets versions access latest \
    --secret=alchemy-api-key \
    --project="${PROJECT_ID}" 2>/dev/null || echo "")"
  if [[ -z "$ALCHEMY_KEY" ]]; then
    echo "ERROR: Could not fetch alchemy-api-key from Secret Manager (project=${PROJECT_ID})" >&2
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
CORRELATION_ID="$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
VM_NAME="${VM_PREFIX}${RUN_TS}"
TODAY="$(date +%Y-%m-%d)"

GCS_RESULTS="gs://${PROJECT_ID}-defi-validation/results/lending/${TODAY}/${CORRELATION_ID}/results.json"

echo "=== Phase 3C Aave Lending Rate Validation ==="
echo "  VM name:        ${VM_NAME}"
echo "  Zone:           ${ZONE}"
echo "  Machine:        ${MACHINE_TYPE}"
echo "  Block range:    ${BLOCK_START} → ${BLOCK_END}"
echo "  Target events:  ${TARGET_EVENTS}"
echo "  Correlation ID: ${CORRELATION_ID}"
echo "  Results GCS:    ${GCS_RESULTS}"
echo ""

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — exiting."
  exit 0
fi

# Durable-log streamer (deployment-service/scripts/vm/lib/launcher_common.sh):
# continuous GCS run.log stream every 30s + heartbeat + terminal EXIT_STATUS
# marker + guaranteed final upload + shutdown — self-delete-proof observability
# so the /deployments surface + exit_code monitor see this VM.
LOG_TRAP="$(lc_log_upload_trap_block "${VM_NAME}" "${PROJECT_ID}" "DEFI" "lending-rate-validation")"

STARTUP_SCRIPT=$(cat <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
export HOME=/root
export PATH="/root/.local/bin:\$PATH"
export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export WEB3_PROVIDER_URI="https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"

${LOG_TRAP}

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Correlation ID: ${CORRELATION_ID}"
date

# Install Python 3.13 + uv
apt-get update -qq && apt-get install -yqq \
  curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

WORK_DIR=/opt/lending-rate-validation

# Download and unpack execution-service tarball
# NOTE: pre-built tarballs from create-code-tarballs.sh are FLAT (no subdir prefix);
# we extract each into its own named subdir so 'uv pip install -e <repo>' can find it.
mkdir -p \${WORK_DIR}
echo "Downloading codebase tarballs..."
for repo in unified-api-contracts unified-trading-library execution-service; do
  gsutil -q cp "gs://${CODE_BUCKET}/code/\${repo}-code.tar.gz" "\${WORK_DIR}/\${repo}.tar.gz"
  mkdir -p "\${WORK_DIR}/\${repo}"
  tar xzf "\${WORK_DIR}/\${repo}.tar.gz" -C "\${WORK_DIR}/\${repo}"
  rm "\${WORK_DIR}/\${repo}.tar.gz"
done

# Create venv and install
python3.13 -m venv "\${WORK_DIR}/.venv"
source "\${WORK_DIR}/.venv/bin/activate"
cd "\${WORK_DIR}"

WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://${CODE_BUCKET}/wheels/py313-linux-x86_64"
mkdir -p "\$WHEEL_CACHE"
gsutil -m -q cp "\$WHEEL_GCS/*.whl" "\$WHEEL_CACHE/" 2>/dev/null || true

# betfairlightweight forces requests<2.33.0 but execution-service requires >=2.33.0
# (CVE-2026-25645). Locally resolved via [tool.uv].override-dependencies in pyproject;
# uv pip install needs explicit --override flag with a constraints file.
cat > /tmp/uv-overrides.txt <<EOF
requests>=2.33.0,<3.0.0
EOF
# Tarballs have no .git history, so hatch-vcs/setuptools-scm can't detect a
# version for UAC/UTL — pretend-version unblocks the editable install.
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
uv pip install --override /tmp/uv-overrides.txt --find-links "\$WHEEL_CACHE" --index-url https://pypi.org/simple --no-sources \
  -e unified-api-contracts \
  -e unified-trading-library \
  -e execution-service

echo "=== Running Phase 3C Aave V3 lending rate validation ==="
cd execution-service

# PYTHONPATH ensures `tests.defi_execution.*` imports resolve (CLI imports harness
# functions from the test module; sys.path.insert() in-script doesn't always take
# effect when invoked as `python3 scripts/...`).
export PYTHONPATH="\${WORK_DIR}/execution-service:\${PYTHONPATH:-}"

# Run validation. Disable set -e around the call so that a non-zero exit
# (e.g., pass_rate < threshold → FAILED gate → sys.exit(1)) doesn't halt
# the startup script before the self-shutdown step below.
# Closes aave_lending_rate_val_vm_no_shutdown_2026_05_16.md root cause.
set +e
python3 scripts/run_lending_rate_validation.py \\
  --block-start ${BLOCK_START} \\
  --block-end ${BLOCK_END} \\
  --target-events ${TARGET_EVENTS} \\
  --output-gcs-path "${GCS_RESULTS}" \\
  --correlation-id "${CORRELATION_ID}"
EXIT_CODE=\$?
set -e

echo ""
echo "=== Validation complete: exit=\${EXIT_CODE} ==="
date

# run.log + EXIT_STATUS upload + shutdown handled by the lc_log_upload_trap_block
# EXIT trap above (durable even on validation FAILED gate). Surface the workload
# rc so the trap records it as the terminal EXIT_STATUS.
exit \${EXIT_CODE}
STARTUP_EOF
)

# Create the VM
METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},CORRELATION_ID=${CORRELATION_ID}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

STARTUP_FILE="$(mktemp /tmp/startup-lending-rate-XXXX.sh)"
printf '%s' "$STARTUP_SCRIPT" > "$STARTUP_FILE"

echo "Creating VM ${VM_NAME}..."
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
  --labels="purpose=lending-rate-validation,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}"

rm "${STARTUP_FILE}"

echo ""
echo "============================================================"
echo "VM launched: ${VM_NAME}"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='name~^aave-lending-rate-val-' --project=${PROJECT_ID}"
echo ""
echo "Event stream (STARTED within 60s expected):"
echo "  gsutil ls gs://${PROJECT_ID}-events/events/lending-rate-validation/${TODAY}/${CORRELATION_ID}/"
echo ""
echo "Results (after STOPPED):"
echo "  gsutil cat ${GCS_RESULTS} | python3 -m json.tool | grep pass_rate"
echo ""
echo "VM logs:"
echo "  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "============================================================"
