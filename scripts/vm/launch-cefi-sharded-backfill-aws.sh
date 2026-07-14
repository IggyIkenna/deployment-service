#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# AWS EC2 equivalent of launch-cefi-sharded-backfill.sh.
# Launches sharded CeFi Tardis backfill fleet on EC2 (ap-northeast-1).
# (CeFi-ONLY — the former TradFi block was REMOVED 2026-06-23; TradFi rides the
# canonical Databento launchers. See launch_tradfi_shard NOTE below.)
#
# GCP counterpart: launch-cefi-sharded-backfill.sh
# VM naming: cefi-sharded-{venue_lower}-{year}-{group}-{YYYYMMDD}
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
#   (TradFi removed 2026-06-23 — Databento launchers serve it.)
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

# Tardis concurrent-VM cap (operator HARD RULE 2026-07-14, replaces the singleton lock):
# at most 3 Tardis-consuming VMs at a time ACROSS BOTH CLOUDS (one shared key); the lease
# does NOT lift the cap. The guard lib counts RUNNING GCP shards + running/pending AWS
# sharded-backfill instances. This script's static fan-out below is ~83 shards — far above
# the cap BY DESIGN, so a full-fleet AWS run is now REFUSED: slice it into <=3-VM waves
# (edit the venue loops) or use an explicit FORCE=1 operator override. Empirical basis
# (N=3 lease-ON grinds, N=6 lease-OFF collapses):
# unified-trading-pm/plans/active/issues/tardis_concurrent_ip_lockout_2026_07_12.md
# shellcheck source=./tardis-concurrency-guard.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"
if [[ "$DRY_RUN" != "1" ]]; then
  # GCP fleet zone/project for the cross-cloud count (the primary Tardis fleet lives there).
  tardis_concurrency_guard 83 "${GCP_ZONE:-asia-northeast1-c}" "${GCP_PROJECT:-central-element-323112}" || exit 1
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

  LOG_TRAP="$(lc_aws_log_upload_trap_block "${vm_name}" "${AWS_ACCOUNT_ID}" "cefi" "cefi-backfill")"

  cat > "${tmpfile}" <<STARTUP_EOF
#!/bin/bash
set -euo pipefail
${LOG_TRAP}

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
# Teardown owned by lc_aws_log_upload_trap_block EXIT trap (final S3 upload + EXIT_STATUS, then shutdown -h +1).
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

# NOTE (2026-06-23): launch_tradfi_shard REMOVED — see the GCP twin
# launch-cefi-sharded-backfill.sh. The Tardis venue tags CME-FUTURES /
# CBOE-VIX-FUTURES / CME-OPTIONS / CBOE-VIX-OPTIONS are NOT canonical TRADFI venues
# (UAC VENUES_BY_ASSET_GROUP["tradfi"] = {NASDAQ,NYSE,CME,ICE,CBOE}) → MTDS produced
# "No active venues for TRADFI" → 0 rows. TradFi is served by the canonical Databento
# launchers (launch-tradfi-bf-*-ohlcv-*.sh / launch-tradfi-backfill-vm.sh).

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

# TradFi block REMOVED 2026-06-23 (see launch_tradfi_shard NOTE above). This script
# is now CeFi-only; TradFi rides the canonical Databento launchers.

if [[ "$DRY_RUN" == "1" ]]; then
  echo ""; echo "=========================================="
  echo "DRY-RUN: ${_launched} VMs would be launched"
  echo "=========================================="
else
  echo ""; echo "All ${_launched} VMs launched (${TODAY})"
fi
