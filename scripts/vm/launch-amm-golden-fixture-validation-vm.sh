#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch one (or all) GCE VMs for Phase 2 AMM golden-swap fixture validation.
#
# Each VM validates a single pool shape's fixture corpus via
# ``execution-service/scripts/run_amm_golden_validation.py``.
# One VM per shape for parallel sweep (--all-shapes fans out 7 VMs).
#
# VM outputs:
#   gs://{PROJECT_ID}-defi-validation/results/amm/{DATE}/{SHAPE}/{CORRELATION_ID}/results.json
#
# Event stream:
#   gs://{PROJECT_ID}-events/events/amm-golden-validation/{DATE}/{CORRELATION_ID}/hour={H}/*.jsonl
#   Required: STARTED within 60s + STOPPED/FAILED at exit.
#
# Singleton lock: per-shape lock (VM name pattern amm-golden-{shape}-{date}).
# Refuses to launch a shape if an amm-golden-{shape}-* VM is RUNNING.
# Pass --force to bypass.
#
# Watchdog registration: VM prefix "amm-golden-" registered in
# VM_PREFIX_TO_BUCKET (None = heartbeat-only; no per-VM manifest shards).
#
# Usage:
#   bash launch-amm-golden-fixture-validation-vm.sh --shape UNISWAP_V3
#   bash launch-amm-golden-fixture-validation-vm.sh --all-shapes          # 7 VMs
#   bash launch-amm-golden-fixture-validation-vm.sh --shape UNISWAP_V3 --capture  # fresh RPC
#   bash launch-amm-golden-fixture-validation-vm.sh --shape UNISWAP_V3 --dry-run
#   bash launch-amm-golden-fixture-validation-vm.sh --shape UNISWAP_V3 --force
#
# Event verification:
#   gcloud compute instances list --filter="name~^amm-golden-" --project=central-element-323112
#   gsutil ls gs://central-element-323112-events/events/amm-golden-validation/$(date +%Y-%m-%d)/
#   gsutil ls gs://central-element-323112-defi-validation/results/amm/$(date +%Y-%m-%d)/
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
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-2}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false
SHAPE=""
ALL_SHAPES_FLAG=false
CAPTURE=false
TARGET_SWAPS="${TARGET_SWAPS:-30}"

# All valid shapes
SHAPES=(
  "UNISWAP_V3"
  "UNISWAP_V4"
  "UNISWAP_V2"
  "CURVE_STABLE"
  "BALANCER_WEIGHTED"
  "SOLANA_AMM"
  "SOLIDLY_FORK"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --all-shapes) ALL_SHAPES_FLAG=true; shift ;;
    --capture) CAPTURE=true; shift ;;
    --shape) SHAPE="$2"; shift 2 ;;
    --target-swaps) TARGET_SWAPS="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT_ID")"
lc_validate_env "$DEPLOYMENT_ENV"

# If --all-shapes, recurse for each shape
if $ALL_SHAPES_FLAG; then
  echo "=== AMM Golden Validation — fanning out ${#SHAPES[@]} VMs ==="
  FAILED_SHAPES=()
  for s in "${SHAPES[@]}"; do
    echo ""
    echo "--- Launching shape: ${s} ---"
    EXTRA_FLAGS=()
    $CAPTURE && EXTRA_FLAGS+=("--capture")
    $FORCE && EXTRA_FLAGS+=("--force")
    $DRY_RUN && EXTRA_FLAGS+=("--dry-run")
    if bash "${BASH_SOURCE[0]}" \
        --shape "${s}" \
        --zone "${ZONE}" \
        --project "${PROJECT_ID}" \
        --env "${DEPLOYMENT_ENV}" \
        --target-swaps "${TARGET_SWAPS}" \
        "${EXTRA_FLAGS[@]}"; then
      echo "  ${s}: launched OK"
    else
      echo "  ${s}: FAILED to launch" >&2
      FAILED_SHAPES+=("${s}")
    fi
  done

  if [[ ${#FAILED_SHAPES[@]} -gt 0 ]]; then
    echo ""
    echo "ERROR: Failed to launch shapes: ${FAILED_SHAPES[*]}" >&2
    exit 1
  fi
  echo ""
  echo "All ${#SHAPES[@]} shapes launched."
  exit 0
fi

if [[ -z "$SHAPE" ]]; then
  echo "ERROR: --shape <SHAPE> or --all-shapes required" >&2
  echo "Valid shapes: ${SHAPES[*]}" >&2
  exit 1
fi

# Validate shape
VALID_SHAPE=false
for s in "${SHAPES[@]}"; do
  [[ "$s" == "$SHAPE" ]] && VALID_SHAPE=true && break
done
if ! $VALID_SHAPE; then
  echo "ERROR: Unknown shape '${SHAPE}'. Valid: ${SHAPES[*]}" >&2
  exit 1
fi

# Normalize: UNISWAP_V3 → uniswap-v3 for VM name
SHAPE_SLUG="${SHAPE//_/-}"
SHAPE_SLUG="$(echo "$SHAPE_SLUG" | tr '[:upper:]' '[:lower:]')"

# ── Per-shape singleton lock ──
VM_PREFIX="amm-golden-${SHAPE_SLUG}-"
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: AMM golden validation VM for shape ${SHAPE} already running: ${EXISTING}
Refusing duplicate launch.

Options:
  Force: bash $0 --shape ${SHAPE} --force

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

RUN_TS="$(date +%Y%m%d-%H%M%S)"
CORRELATION_ID="$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
VM_NAME="${VM_PREFIX}${RUN_TS}"
TODAY="$(date +%Y-%m-%d)"

GCS_RESULTS="gs://${PROJECT_ID}-defi-validation/results/amm/${TODAY}/${SHAPE_SLUG}/${CORRELATION_ID}/results.json"

CAPTURE_CLI_FLAG=""
$CAPTURE && CAPTURE_CLI_FLAG="--capture --target-swaps ${TARGET_SWAPS}"

echo "=== Phase 2 AMM Golden Validation: ${SHAPE} ==="
echo "  VM name:        ${VM_NAME}"
echo "  Zone:           ${ZONE}"
echo "  Machine:        ${MACHINE_TYPE}"
echo "  Capture:        ${CAPTURE}"
echo "  Correlation ID: ${CORRELATION_ID}"
echo "  Results GCS:    ${GCS_RESULTS}"
echo ""

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — exiting."
  exit 0
fi

# Fetch Alchemy key only if capturing on-chain data
ALCHEMY_KEY=""
if $CAPTURE; then
  ALCHEMY_KEY="$(gcloud secrets versions access latest \
    --secret=alchemy-api-key \
    --project="${PROJECT_ID}" 2>/dev/null || echo "")"
  if [[ -z "$ALCHEMY_KEY" ]]; then
    echo "ERROR: Could not fetch alchemy-api-key from Secret Manager (needed for --capture)" >&2
    exit 1
  fi
fi

# Durable-log streamer (deployment-service/scripts/vm/lib/launcher_common.sh):
# continuous GCS run.log stream every 30s + heartbeat + terminal EXIT_STATUS
# marker + guaranteed final upload + shutdown — self-delete-proof observability
# so the /deployments surface + exit_code monitor see this VM.
LOG_TRAP="$(lc_log_upload_trap_block "${VM_NAME}" "${PROJECT_ID}" "DEFI" "amm-golden-validation")"

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
$( [[ -n "$ALCHEMY_KEY" ]] && echo "export WEB3_PROVIDER_URI=\"https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}\"" )

${LOG_TRAP}

echo "=== VM Startup: ${VM_NAME} ==="
echo "  Shape:          ${SHAPE}"
echo "  Correlation ID: ${CORRELATION_ID}"
date

# Install Python 3.13 + uv
apt-get update -qq && apt-get install -yqq \
  curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

WORK_DIR=/opt/amm-golden-validation

mkdir -p \${WORK_DIR}
echo "Downloading codebase tarballs..."
# NOTE: pre-built tarballs are FLAT (no subdir prefix); extract each into its
# own named subdir so 'uv pip install -e <repo>' can find it.
for repo in unified-api-contracts unified-trading-library execution-service; do
  gsutil -q cp "gs://${CODE_BUCKET}/code/\${repo}-code.tar.gz" "\${WORK_DIR}/\${repo}.tar.gz"
  mkdir -p "\${WORK_DIR}/\${repo}"
  tar xzf "\${WORK_DIR}/\${repo}.tar.gz" -C "\${WORK_DIR}/\${repo}"
  rm "\${WORK_DIR}/\${repo}.tar.gz"
done

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

echo "=== Running Phase 2 AMM golden validation: ${SHAPE} ==="
cd execution-service

# PYTHONPATH ensures `tests.*` imports resolve.
export PYTHONPATH="\${WORK_DIR}/execution-service:\${PYTHONPATH:-}"

python3 scripts/run_amm_golden_validation.py \
  --shape "${SHAPE}" \
  ${CAPTURE_CLI_FLAG} \
  --output-gcs-path "${GCS_RESULTS}" \
  --correlation-id "${CORRELATION_ID}"

EXIT_CODE=\$?
echo ""
echo "=== Validation complete: shape=${SHAPE} exit=\${EXIT_CODE} ==="
date

# run.log + EXIT_STATUS upload + shutdown handled by the lc_log_upload_trap_block
# EXIT trap above. Surface the workload rc so the trap records it as EXIT_STATUS.
exit \${EXIT_CODE}
STARTUP_EOF
)

METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},AMM_SHAPE=${SHAPE}"
METADATA="${METADATA},CORRELATION_ID=${CORRELATION_ID}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_startup_file "$STARTUP_SCRIPT"

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
  --boot-disk-size=30GB \
  --boot-disk-type=pd-ssd \
  --labels="purpose=amm-golden-validation,env=${DEPLOYMENT_ENV},shape=${SHAPE_SLUG},run-ts=${RUN_TS}"

echo ""
echo "============================================================"
echo "VM launched: ${VM_NAME} (shape=${SHAPE})"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='name~^amm-golden-' --project=${PROJECT_ID}"
echo ""
echo "Event stream (STARTED within 60s expected):"
echo "  gsutil ls gs://${PROJECT_ID}-events/events/amm-golden-validation/${TODAY}/${CORRELATION_ID}/"
echo ""
echo "Results (after STOPPED):"
echo "  gsutil cat ${GCS_RESULTS} | python3 -m json.tool | grep -E '\"failed\"|\"passed\"'"
echo ""
echo "VM logs:"
echo "  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "============================================================"
