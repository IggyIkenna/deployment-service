#!/usr/bin/env bash
# AWS EC2 equivalent of launch-cefi-sharded-backfill.sh.
# Launches sharded CeFi + TradFi Tardis backfill fleet on EC2 (ap-northeast-1).
#
# GCP counterpart: launch-cefi-sharded-backfill.sh
# VM naming: cefi-sharded-{venue_lower}-{year}-{group}-{YYYYMMDD}
#            tradfi-sharded-{instrument}-{year}-{group}-{YYYYMMDD}
# Profile:   uts-backfill-prod
# Tarball:   s3://{S3_CODE_BUCKET}/code/mtds-code.tar.gz
#
# Singleton lock: refuse if any cefi-sharded-* or tradfi-sharded-* VM is RUNNING
# (unless FORCE=1). Shared Tardis account + project egress NAT = same rate-limit
# risk as GCP. See 2026-05-05 silent-drop incident.
#
# Instrument filter (operator-authoritative, 2026-04-19):
#   CeFi:  BTC + ETH (spot + futures + perps + options DERIBIT only)
#          SOL XRP BNB DOGE ADA AVAX LINK (spot + perps + futures, NO options)
#   TradFi: CME ES (S&P 500 E-mini) + CBOE VIX futures + options
#
# Usage:
#   bash launch-cefi-sharded-backfill-aws.sh [--env prod|staging|dev]
#
# Env overrides:
#   DRY_RUN=1        print plan only, no EC2 launch
#   FORCE=1          bypass singleton check
#   ONLY="venue:year:group ..."  filter to specific shards (relaunch-OOM workflow)
#   MACHINE_TYPE_HEAVY=m7i.2xlarge   override heavy instance type
#   MACHINE_TYPE_LIGHT=m7i.xlarge    override light instance type
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/aws_ec2_launch_lib.sh
source "${SCRIPT_DIR}/lib/aws_ec2_launch_lib.sh"

AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
DISK_GB=50
INSTANCE_PROFILE="uts-backfill-prod"
DRY_RUN="${DRY_RUN:-0}"
FORCE="${FORCE:-0}"
MAX_CONCURRENT="${MAX_CONCURRENT:-10}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
TODAY="$(date +%Y%m%d)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)    DEPLOYMENT_ENV="$2"; shift 2 ;;
    --region) AWS_REGION="$2"; shift 2 ;;
    *) echo "ERROR: unknown flag '$1' (use --env; other config via env vars)" >&2; exit 1 ;;
  esac
done

case "${AWS_REGION}" in
  ap-northeast-1) ;;
  *) echo "ERROR: region ${AWS_REGION} is not ap-northeast-1." >&2; exit 1 ;;
esac

lc_aws_validate_env "${DEPLOYMENT_ENV}"
lc_aws_resolve_account
S3_CODE_BUCKET="$(lc_aws_code_bucket "${AWS_ACCOUNT_ID}")"

# Singleton lock: shared Tardis account rate-limits under concurrent load.
if [[ "$FORCE" != "1" && "$DRY_RUN" != "1" ]]; then
  EXISTING="$(aws ec2 describe-instances \
    --region "${AWS_REGION}" \
    --filters \
      "Name=tag:purpose,Values=cefi-sharded-backfill,tradfi-sharded-backfill" \
      "Name=instance-state-name,Values=running,pending" \
    --query "Reservations[].Instances[].Tags[?Key=='Name']|[0][0].Value" \
    --output text 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" && "$EXISTING" != "None" ]]; then
    cat >&2 <<EOF
ERROR: CeFi/TradFi sharded-backfill VMs already running in ${AWS_REGION} (e.g. ${EXISTING}).

Refusing to launch a duplicate sharded backfill — Tardis rate-limits per-account.
Concurrent runs collide on 429 backoffs; the adapter retry budget can exhaust silently.

Options:
  Inspect:  aws ec2 describe-instances --filters 'Name=tag:purpose,Values=cefi-sharded-backfill,tradfi-sharded-backfill' --region ${AWS_REGION}
  Force:    FORCE=1 bash $0
EOF
    exit 1
  fi
fi

# ── Per-venue symbol universes (identical to GCP counterpart) ──
SYMBOLS_BINANCE_SPOT="btcusdt;ethusdt;solusdt;xrpusdt;bnbusdt;dogeusdt;adausdt;avaxusdt;linkusdt"
SYMBOLS_BINANCE_FUTURES="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT"
SYMBOLS_BYBIT="BTCUSDT;ETHUSDT;SOLUSDT;XRPUSDT;BNBUSDT;DOGEUSDT;ADAUSDT;AVAXUSDT;LINKUSDT"
SYMBOLS_DERIBIT="BTC;ETH;BTC-PERPETUAL;ETH-PERPETUAL"
SYMBOLS_COINBASE_SPOT="BTC-USD;ETH-USD;SOL-USD;XRP-USD;DOGE-USD;ADA-USD;AVAX-USD;LINK-USD"
SYMBOLS_OKX_SPOT="BTC-USDT;ETH-USDT;SOL-USDT;XRP-USDT;BNB-USDT;DOGE-USDT;ADA-USDT;AVAX-USDT;LINK-USDT"
SYMBOLS_OKX_SWAP="BTC-USDT-SWAP;ETH-USDT-SWAP;SOL-USDT-SWAP;XRP-USDT-SWAP;BNB-USDT-SWAP;DOGE-USDT-SWAP;ADA-USDT-SWAP;AVAX-USDT-SWAP;LINK-USDT-SWAP"
SYMBOLS_HYPERLIQUID="BTC;ETH;SOL;XRP;BNB;DOGE;ADA;AVAX;LINK"
SYMBOLS_UPBIT="KRW-BTC;KRW-ETH;KRW-SOL;KRW-XRP;KRW-DOGE;KRW-ADA;KRW-AVAX;KRW-LINK"

DATA_HEAVY="trades;book_snapshot_5"
DATA_LIGHT_PERPS="derivative_ticker;liquidations;futures_chain"
DATA_LIGHT_DERIBIT="derivative_ticker;options_chain;futures_chain"

SYMBOLS_CME_ES_2026="ESM26;ESU26;ESZ26"
SYMBOLS_CME_ES_2025="ESM25;ESU25;ESZ25"
SYMBOLS_CME_ES_2024="ESM24;ESU24;ESZ24"
SYMBOLS_CBOE_VIX_2026="VXM26;VXU26;VXZ26;VX"
SYMBOLS_CBOE_VIX_2025="VXM25;VXU25;VXZ25;VX"
SYMBOLS_CBOE_VIX_2024="VXM24;VXU24;VXZ24;VX"

_launched=0
_concurrent=0

_stagger() { sleep 2; }

_batch_guard() {
  _concurrent=$((_concurrent + 1))
  if (( _concurrent >= MAX_CONCURRENT )); then
    echo "  ... reached MAX_CONCURRENT=${MAX_CONCURRENT}, pausing 15s before next batch"
    sleep 15
    _concurrent=0
  fi
}

# Build user-data for a single MTDS shard
_make_user_data() {
  local vm_name="$1" asset_group="$2" venue="$3" start_date="$4" end_date="$5"
  local data_types="$6" symbols="$7"
  local force_flag=""
  [[ "${VM_FORCE:-false}" == "true" || "${VM_FORCE:-false}" == "1" ]] && force_flag="--force"

  local tmpfile
  tmpfile="$(mktemp /tmp/startup-cefi-shard-XXXX.sh)"

  cat > "${tmpfile}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
exec > >(tee /var/log/cefi-shard-bootstrap.log) 2>&1

export VM_NAME="${vm_name}"
export VM_TASK=cefi-backfill
export VM_SERVICE=market_tick_data_service
export VM_OPERATION=download
export VM_ASSET_GROUP="${asset_group}"
export VM_VENUE="${venue}"
export VM_START_DATE="${start_date}"
export VM_END_DATE="${end_date}"
export VM_DATA_TYPES="${data_types}"
export VM_INSTRUMENT_IDS="${symbols}"
export VM_FORCE="${VM_FORCE:-false}"
export VM_SHUTDOWN_ON_COMPLETION=true
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export CLOUD_PROVIDER=aws
export CLOUD_MOCK_MODE=false
export AWS_DEFAULT_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"

echo "=== AWS CeFi Sharded Backfill: \${VM_NAME} ==="
date

apt-get update -qq
apt-get install -yqq curl python3 python3-pip python3-venv unzip git

if ! command -v aws &>/dev/null; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
  bash /tmp/awscliv2/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
  rm -rf /tmp/awscliv2*
fi

GH_PAT="\$(aws secretsmanager get-secret-value --secret-id GH_PAT --query SecretString --output text 2>/dev/null || echo '')"
if [[ -z "\${GH_PAT}" ]]; then
  echo "ERROR: GH_PAT not found in Secrets Manager" >&2
  shutdown -h now; exit 1
fi

S3_CODE_BUCKET="unified-trading-deployment-scripts-\${AWS_ACCOUNT_ID}"
WORK_DIR=/opt/backfill/mtds
mkdir -p "\${WORK_DIR}"

for entry in \
    "mtds-code:market-tick-data-service" \
    "unified-api-contracts-code:unified-api-contracts" \
    "unified-trading-library-code:unified-trading-library" \
    "deployment-service-code:deployment-service"; do
  tarball_name="\${entry%%:*}"
  dir_name="\${entry##*:}"
  mkdir -p "\${WORK_DIR}/\${dir_name}"
  aws s3 cp "s3://\${S3_CODE_BUCKET}/code/\${tarball_name}.tar.gz" /tmp/\${tarball_name}.tar.gz --quiet
  tar xzf /tmp/\${tarball_name}.tar.gz -C "\${WORK_DIR}/\${dir_name}/"
  rm /tmp/\${tarball_name}.tar.gz
done

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

WHEEL_CACHE=/tmp/wheel-cache
mkdir -p "\${WHEEL_CACHE}"
aws s3 sync "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" "\${WHEEL_CACHE}/" --quiet 2>/dev/null || true

uv venv "\${WORK_DIR}/.venv" --python 3.13
source "\${WORK_DIR}/.venv/bin/activate"

INSTALL_ARGS="--no-sources"
for pkg in unified-api-contracts unified-trading-library market-tick-data-service deployment-service; do
  [[ -d "\${WORK_DIR}/\${pkg}" ]] && INSTALL_ARGS="\${INSTALL_ARGS} -e \${WORK_DIR}/\${pkg}"
done
uv pip install --find-links "\${WHEEL_CACHE}" \${INSTALL_ARGS}
uv pip install --find-links "\${WHEEL_CACHE}" pandas pyarrow boto3 2>/dev/null || true

aws s3 sync "\${WHEEL_CACHE}/" "s3://\${S3_CODE_BUCKET}/wheels/py313-linux-x86_64/" --quiet 2>/dev/null || true

echo ""
echo "=== Running MTDS shard: ${venue} ${start_date} → ${end_date} ==="
date

# VM_INSTRUMENT_IDS uses semicolons as separator; MTDS CLI expects space-separated.
INSTRUMENT_IDS_SPACE="\$(echo "\${VM_INSTRUMENT_IDS}" | tr ';' ' ')"
DATA_TYPES_SPACE="\$(echo "\${VM_DATA_TYPES}" | tr ';' ' ')"

cd "\${WORK_DIR}/market-tick-data-service"
CLOUD_PROVIDER=aws CLOUD_MOCK_MODE=false \
  python3 -m market_tick_data_service \
    --operation download --mode batch \
    --asset-group "\${VM_ASSET_GROUP}" \
    --venue "\${VM_VENUE}" \
    --start-date "\${VM_START_DATE}" --end-date "\${VM_END_DATE}" \
    --data-types \${DATA_TYPES_SPACE} \
    --instrument-ids \${INSTRUMENT_IDS_SPACE} \
    ${force_flag}

echo ""
echo "=== CeFi shard complete: \${VM_NAME} ==="
date
shutdown -h now
STARTUP_EOF

  echo "${tmpfile}"
}

launch_cefi_shard() {
  local venue="$1" year="$2" group="$3" data_types="$4" symbols="$5"

  local start_date end_date machine
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"; end_date="2026-04-17"
  else
    start_date="${year}-01-01"; end_date="${year}-12-31"
  fi

  if [[ "$group" == "heavy" ]]; then
    machine="${MACHINE_TYPE_HEAVY:-m7i.2xlarge}"
  else
    machine="${MACHINE_TYPE_LIGHT:-m7i.xlarge}"
  fi

  # ONLY filter (same shape as GCP: "venue:year:group")
  if [[ -n "${ONLY:-}" ]]; then
    local key="${venue}:${year}:${group}" matched=0
    for tok in $ONLY; do [[ "$tok" == "$key" ]] && matched=1 && break; done
    [[ $matched -eq 0 ]] && return 0
  fi

  local venue_lower; venue_lower="$(echo "$venue" | tr '[:upper:]' '[:lower:]' | tr -s '-' '-')"
  local vm_name="cefi-sharded-${venue_lower}-${year}-${group}-${TODAY}"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] ${vm_name}  venue=${venue} year=${year} group=${group}"
    echo "          data_types=${data_types}  symbols=${symbols}"
    _launched=$((_launched + 1)); return 0
  fi

  local ud_file
  ud_file="$(_make_user_data "${vm_name}" "CEFI" "${venue}" "${start_date}" "${end_date}" "${data_types}" "${symbols}")"

  local EXTRA_TAGS
  EXTRA_TAGS='[{"Key":"purpose","Value":"cefi-sharded-backfill"},{"Key":"venue","Value":"'"${venue}"'"},{"Key":"year","Value":"'"${year}"'"},{"Key":"group","Value":"'"${group}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

  echo "Launching ${vm_name} (${venue} ${year} ${group})"
  local INSTANCE_ID
  INSTANCE_ID="$(lc_aws_ec2_run "${vm_name}" "${AWS_REGION}" "${machine}" "${DISK_GB}" "${INSTANCE_PROFILE}" "${ud_file}" "${EXTRA_TAGS}")"
  rm -f "${ud_file}"
  echo "  → ${INSTANCE_ID}"

  _stagger; _batch_guard
  _launched=$((_launched + 1))
}

launch_tradfi_shard() {
  local instrument="$1" venue="$2" year="$3" group="$4" data_types="$5" symbols="$6"

  local start_date end_date
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"; end_date="2026-04-17"
  else
    start_date="${year}-01-01"; end_date="${year}-12-31"
  fi

  local machine="${MACHINE_TYPE_TRADFI:-m7i.xlarge}"

  if [[ -n "${ONLY:-}" ]]; then
    local key="${instrument}:${year}:${group}" matched=0
    for tok in $ONLY; do [[ "$tok" == "$key" ]] && matched=1 && break; done
    [[ $matched -eq 0 ]] && return 0
  fi

  local vm_name="tradfi-sharded-${instrument}-${year}-${group}-${TODAY}"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] ${vm_name}  venue=${venue} year=${year} group=${group}"
    _launched=$((_launched + 1)); return 0
  fi

  local ud_file
  ud_file="$(_make_user_data "${vm_name}" "TRADFI" "${venue}" "${start_date}" "${end_date}" "${data_types}" "${symbols}")"

  local EXTRA_TAGS
  EXTRA_TAGS='[{"Key":"purpose","Value":"tradfi-sharded-backfill"},{"Key":"instrument","Value":"'"${instrument}"'"},{"Key":"venue","Value":"'"${venue}"'"},{"Key":"year","Value":"'"${year}"'"},{"Key":"cloud","Value":"aws"},{"Key":"env","Value":"'"${DEPLOYMENT_ENV}"'"}]'

  echo "Launching ${vm_name} (${venue} ${year} ${group})"
  local INSTANCE_ID
  INSTANCE_ID="$(lc_aws_ec2_run "${vm_name}" "${AWS_REGION}" "${machine}" "${DISK_GB}" "${INSTANCE_PROFILE}" "${ud_file}" "${EXTRA_TAGS}")"
  rm -f "${ud_file}"
  echo "  → ${INSTANCE_ID}"

  _stagger; _batch_guard
  _launched=$((_launched + 1))
}

# ── CeFi sharding — year × heavy/light (mirrors GCP) ──────────────────────────
for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BINANCE-FUTURES" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BINANCE_FUTURES"
  launch_cefi_shard "BINANCE-FUTURES" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_BINANCE_FUTURES"
done

for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BINANCE-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BINANCE_SPOT"
done

for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "BYBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_BYBIT"
  launch_cefi_shard "BYBIT" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_BYBIT"
done

for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "DERIBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_DERIBIT"
  launch_cefi_shard "DERIBIT" "$year" "light" "$DATA_LIGHT_DERIBIT" "$SYMBOLS_DERIBIT"
done

for year in 2020 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "COINBASE-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_COINBASE_SPOT"
done

for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "OKX-SPOT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_OKX_SPOT"
done

for year in 2021 2022 2023 2024 2025 2026; do
  launch_cefi_shard "OKX-SWAP" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_OKX_SWAP"
  launch_cefi_shard "OKX-SWAP" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_OKX_SWAP"
done

for year in 2024 2025 2026; do
  launch_cefi_shard "HYPERLIQUID" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_HYPERLIQUID"
  launch_cefi_shard "HYPERLIQUID" "$year" "light" "$DATA_LIGHT_PERPS" "$SYMBOLS_HYPERLIQUID"
done

for year in 2022 2023 2024 2025 2026; do
  launch_cefi_shard "UPBIT" "$year" "heavy" "$DATA_HEAVY" "$SYMBOLS_UPBIT"
done

# ── TradFi — CME ES + CBOE VIX ─────────────────────────────────────────────────
for year in 2024 2025 2026; do
  case "$year" in
    2026) es_syms="$SYMBOLS_CME_ES_2026"; vix_syms="$SYMBOLS_CBOE_VIX_2026" ;;
    2025) es_syms="$SYMBOLS_CME_ES_2025"; vix_syms="$SYMBOLS_CBOE_VIX_2025" ;;
    2024) es_syms="$SYMBOLS_CME_ES_2024"; vix_syms="$SYMBOLS_CBOE_VIX_2024" ;;
  esac
  launch_tradfi_shard "es"  "CME-FUTURES"      "$year" "futures" "trades;book_snapshot_5" "$es_syms"
  launch_tradfi_shard "es"  "CME-OPTIONS"       "$year" "options" "trades;options_chain"   "$es_syms"
  launch_tradfi_shard "vix" "CBOE-VIX-FUTURES"  "$year" "futures" "trades;book_snapshot_5" "$vix_syms"
  launch_tradfi_shard "vix" "CBOE-VIX-OPTIONS"  "$year" "options" "trades;options_chain"   "$vix_syms"
done

if [[ "$DRY_RUN" == "1" ]]; then
  echo ""; echo "=========================================="
  echo "DRY-RUN: ${_launched} VMs would be launched"
  echo "=========================================="
else
  echo ""; echo "All ${_launched} VMs launched (${TODAY})"
fi
