#!/usr/bin/env bash
# Setup an EC2 VM for data pipeline work (MTDS backfill, migrations, etc.)
#
# AWS EC2 equivalent of setup-data-pipeline-vm.sh (GCP).
# Reads task configuration from environment variables set in the EC2 user-data
# (or overridden via SSH export before calling this script).
#
# Usage as user-data (download-and-run):
#   #!/bin/bash
#   aws s3 cp s3://uts-prod-deployment-state/vm/setup-data-pipeline-vm-aws.sh /tmp/
#   export S3_CODE_BUCKET=uts-prod-deployment-state
#   export VM_NAME=my-backfill-vm
#   export VM_TASK=cefi-backfill
#   export VM_SERVICE=market_tick_data_service
#   export VM_ASSET_GROUP=CEFI
#   export CLOUD_PROVIDER=aws
#   export DEPLOYMENT_ENV=prod
#   bash /tmp/setup-data-pipeline-vm-aws.sh
#
# Usage via SSH (manual):
#   bash setup-data-pipeline-vm-aws.sh
#
# Environment variables (all optional with defaults):
#   S3_CODE_BUCKET    S3 bucket for code tarballs (default: uts-prod-deployment-state)
#   VM_NAME           Human-readable VM name for log tagging
#   VM_TASK           Pipeline task type (e.g. cefi-backfill, instruments-backfill)
#   VM_SERVICE        Python service module name (e.g. market_tick_data_service)
#   VM_ASSET_GROUP    CEFI|DEFI|TRADFI|SPORTS|PREDICTION
#   VM_VENUE          Venue identifier for single-venue tasks
#   VM_START_DATE     Date range start (YYYY-MM-DD)
#   VM_END_DATE       Date range end (YYYY-MM-DD)
#   VM_OPERATION      Service operation (default: download)
#   VM_PIPELINE_MODE  live|batch
#   DEPLOYMENT_ENV    prod|staging|dev (default: prod)
#   CLOUD_PROVIDER    Always aws for this script (set automatically)
set -euo pipefail

S3_CODE_BUCKET="${S3_CODE_BUCKET:-uts-prod-deployment-state}"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
WORKSPACE="/home/ubuntu/workspace"
VENV="/home/ubuntu/venv"
LOGS="/home/ubuntu/logs"
LOG="/var/log/vm-setup.log"

# Task metadata — read from environment (user-data sets these as exports)
VM_NAME="${VM_NAME:-unknown-vm}"
VM_TASK="${VM_TASK:-}"
VM_SERVICE="${VM_SERVICE:-market_tick_data_service}"
VM_ASSET_GROUP="${VM_ASSET_GROUP:-CEFI}"
VM_VENUE="${VM_VENUE:-}"
VM_START_DATE="${VM_START_DATE:-}"
VM_END_DATE="${VM_END_DATE:-}"
VM_OPERATION="${VM_OPERATION:-download}"
VM_PIPELINE_MODE="${VM_PIPELINE_MODE:-batch}"
VM_STRATEGY="${VM_STRATEGY:-}"
VM_DATA_TYPES="${VM_DATA_TYPES:-}"
VM_INSTRUMENT_IDS="${VM_INSTRUMENT_IDS:-}"
VM_CHUNK_DAYS="${VM_CHUNK_DAYS:-30}"
VM_FORCE="${VM_FORCE:-}"
VM_SHUTDOWN_ON_COMPLETION="${VM_SHUTDOWN_ON_COMPLETION:-false}"
MANIFEST_PER_VM_SHARDS="${MANIFEST_PER_VM_SHARDS:-true}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
CLOUD_PROVIDER="aws"

export CLOUD_PROVIDER CLOUD_MOCK_MODE=false DEPLOYMENT_ENV AWS_DEFAULT_REGION="$AWS_REGION"
export MANIFEST_PER_VM_SHARDS VM_NAME

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

log "=== AWS Data Pipeline VM Setup: ${VM_NAME} ==="
log "S3_CODE_BUCKET=$S3_CODE_BUCKET  VM_TASK=$VM_TASK  VM_SERVICE=$VM_SERVICE"
log "ASSET_GROUP=$VM_ASSET_GROUP  DEPLOYMENT_ENV=$DEPLOYMENT_ENV"

# ── 0. File descriptor limit ──
log "Raising file descriptor limit..."
ulimit -n 65536 || log "WARNING: ulimit -n 65536 failed (current: $(ulimit -n))"

# ── 1. System packages ──
log "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential git curl unzip

# Install uv (handles Python 3.13 without deadsnakes PPA)
log "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
log "uv: $(uv --version)"

log "Installing Python 3.13 via uv..."
uv python install 3.13
PY313=$(uv python find 3.13)
log "Python 3.13: $($PY313 --version) at $PY313"

# ── 2. Create venv ──
log "Creating venv..."
rm -rf "$VENV"
"$PY313" -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version)"

# ── 3. Deploy code from S3 ──
log "Deploying code from s3://$S3_CODE_BUCKET/code/..."
mkdir -p "$WORKSPACE/unified-api-contracts" \
         "$WORKSPACE/unified-trading-library" \
         "$WORKSPACE/market-tick-data-service" \
         "$WORKSPACE/deployment-service" \
         "$LOGS"

_fetch_tarball() {
    local tarball_name="$1" target_dir="$2"
    local s3_uri="s3://$S3_CODE_BUCKET/code/${tarball_name}.tar.gz"
    if aws s3 ls "$s3_uri" --region "$AWS_REGION" &>/dev/null; then
        log "  Fetching ${tarball_name}..."
        aws s3 cp "$s3_uri" "/tmp/${tarball_name}.tar.gz" --region "$AWS_REGION" --quiet
        mkdir -p "$WORKSPACE/$target_dir"
        tar xzf "/tmp/${tarball_name}.tar.gz" -C "$WORKSPACE/$target_dir/"
        rm "/tmp/${tarball_name}.tar.gz"
    else
        log "  SKIP ${tarball_name} — not found in S3 (non-fatal)"
    fi
}

# Core tarballs (always installed)
_fetch_tarball "unified-api-contracts-code" "unified-api-contracts"
_fetch_tarball "unified-trading-library-code" "unified-trading-library"
_fetch_tarball "mtds-code" "market-tick-data-service"
_fetch_tarball "deployment-service-code" "deployment-service"

# Service-specific tarball (derived from VM_SERVICE)
declare -A SERVICE_TARBALLS=(
    ["market_tick_data_service"]="mtds-code"          # already installed above
    ["instruments_service"]="instruments-service-code"
    ["features_sports_service"]="features-sports-service-code"
    ["features_onchain_service"]="features-service-code"
    ["strategy_service"]="strategy-service-code"
    ["execution_service"]="execution-service-code"
    ["ml_service"]="ml-service-code"
)
if [[ -n "${SERVICE_TARBALLS[$VM_SERVICE]:-}" ]]; then
    _tarball="${SERVICE_TARBALLS[$VM_SERVICE]}"
    _repo="${_tarball%-code}"
    [[ "$_tarball" != "mtds-code" ]] && _fetch_tarball "$_tarball" "$_repo"
fi

# ── 4. Fetch heartbeat_daemon.py ──
aws s3 cp "s3://$S3_CODE_BUCKET/vm/heartbeat_daemon.py" "/home/ubuntu/heartbeat_daemon.py" \
    --region "$AWS_REGION" --quiet 2>/dev/null || true

# ── 5. Install Python packages via uv ──
log "Installing Python packages..."
WHEEL_CACHE="/tmp/wheel-cache"
mkdir -p "$WHEEL_CACHE"

# Try to pull cached wheels from S3 (speeds up installs on repeated launches)
# timeout-guard: prevents indefinite hangs (similar to gsutil -m fix for GCP)
timeout 180 aws s3 sync "s3://$S3_CODE_BUCKET/wheels/py313-linux-x86_64/" "$WHEEL_CACHE/" \
    --region "$AWS_REGION" --quiet 2>/dev/null || true

INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library market-tick-data-service \
            deployment-service instruments-service features-service \
            strategy-service execution-service ml-service; do
    [[ -d "$WORKSPACE/$pkg" ]] && INSTALL_ARGS="$INSTALL_ARGS -e $WORKSPACE/$pkg"
done
uv pip install --find-links "$WHEEL_CACHE" $INSTALL_ARGS
uv pip install --find-links "$WHEEL_CACHE" pandas pyarrow boto3 2>/dev/null || true

# Push wheels back to S3 cache for next launch
# timeout-guard: prevents indefinite hangs (similar to gsutil -m fix for GCP)
timeout 180 aws s3 sync "$WHEEL_CACHE/" "s3://$S3_CODE_BUCKET/wheels/py313-linux-x86_64/" \
    --region "$AWS_REGION" --quiet 2>/dev/null || true

log "Install complete."

# ── 6. Build CLI args ──
CLI_ARGS=""
[[ -n "$VM_ASSET_GROUP" ]]    && CLI_ARGS="$CLI_ARGS --asset-group $(echo "$VM_ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"
[[ -n "$VM_OPERATION" ]]      && CLI_ARGS="$CLI_ARGS --operation $VM_OPERATION"
[[ -n "$VM_PIPELINE_MODE" ]]  && CLI_ARGS="$CLI_ARGS --mode $VM_PIPELINE_MODE"
[[ -n "$VM_VENUE" ]]          && CLI_ARGS="$CLI_ARGS --venue $VM_VENUE"
[[ -n "$VM_START_DATE" ]]     && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
[[ -n "$VM_END_DATE" ]]       && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
[[ -n "$VM_CHUNK_DAYS" ]]     && CLI_ARGS="$CLI_ARGS --chunk-days $VM_CHUNK_DAYS"
[[ -n "$VM_STRATEGY" ]]       && CLI_ARGS="$CLI_ARGS --strategy $VM_STRATEGY"
[[ -n "$VM_PIPELINE_MODE" ]]  && CLI_ARGS="$CLI_ARGS --pipeline-mode $VM_PIPELINE_MODE"
[[ -n "$VM_DATA_TYPES" ]]     && CLI_ARGS="$CLI_ARGS --data-types $VM_DATA_TYPES"
[[ "$VM_FORCE" == "true" ]]   && CLI_ARGS="$CLI_ARGS --force"
# VM_INSTRUMENT_IDS: semicolon-separated (semicolons avoid gcloud --metadata comma split)
if [[ -n "$VM_INSTRUMENT_IDS" ]]; then
    _ids=$(echo "$VM_INSTRUMENT_IDS" | tr ';' ' ')
    CLI_ARGS="$CLI_ARGS --instrument-ids $_ids"
fi

# ── 7. Start heartbeat daemon (optional) ──
if [[ -f "/home/ubuntu/heartbeat_daemon.py" ]]; then
    nohup python /home/ubuntu/heartbeat_daemon.py \
        --vm-name "$VM_NAME" \
        --cloud aws \
        --region "$AWS_REGION" \
        >> "$LOGS/heartbeat.log" 2>&1 &
    log "Heartbeat daemon started (PID $!)"
fi

# ── 8. Run service workload ──
log ""
log "=== Running service: python -m $VM_SERVICE $CLI_ARGS ==="
date

SERVICE_DIR="$WORKSPACE/${VM_SERVICE//_/-}"
[[ -d "$SERVICE_DIR" ]] || SERVICE_DIR="$WORKSPACE/market-tick-data-service"
cd "$SERVICE_DIR"

set +e
python -m "$VM_SERVICE" $CLI_ARGS
EXIT_CODE=$?
set -e

log ""
if [[ $EXIT_CODE -eq 0 ]]; then
    log "=== Service completed successfully: $VM_NAME (exit 0) ==="
else
    log "=== Service FAILED: $VM_NAME (exit $EXIT_CODE) ==="
fi
date

# ── 9. Upload logs to S3 ──
LOG_DEST="s3://$S3_CODE_BUCKET/vm-logs/$VM_NAME/"
log "Uploading logs to $LOG_DEST..."
aws s3 cp "$LOG" "${LOG_DEST}vm-setup.log" --region "$AWS_REGION" --quiet 2>/dev/null || true
[[ -d "$LOGS" ]] && aws s3 sync "$LOGS/" "$LOG_DEST" --region "$AWS_REGION" --quiet 2>/dev/null || true

# ── 10. Shutdown if requested ──
if [[ "$VM_SHUTDOWN_ON_COMPLETION" == "true" ]]; then
    log "VM_SHUTDOWN_ON_COMPLETION=true — shutting down in 60s..."
    shutdown -h +1 "VM task complete: $VM_NAME"
fi

exit $EXIT_CODE
