#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# MDPS build-continuous VM launcher — runs `--operation build-continuous` (the
# Panama-canal back-adjust continuous-contract stage; see
# market_data_processing_service/engine/build_continuous_engine.py) for one futures
# root (ES or MES), reading already-processed per-contract CME candles and writing the
# stitched continuous-future series + roll-schedule SSOT.
#
# Same VM_TASK=mdps-backfill routing as launch-mdps-backfill-vm.sh (the setup script
# runs VM_BACKFILL_CMD verbatim) — this launcher just builds a different CLI invocation
# (--operation build-continuous --root <ROOT> instead of the default process/backfill
# path), bridged through market_data_processing_service/cli/main.py's
# MDPS_OPERATION/MDPS_CONTINUOUS_ROOT/MDPS_ROLL_DAYS_BEFORE_EXPIRY env vars
# (market-data-processing-service@4b96134).
#
# Output:  processed_candles/by_date/day={D}/pipeline_mode=batch_databento/timeframe={tf}/
#              data_type={dt}/instrument_type=continuous_future/venue=CME/underlying={ROOT}/ticks.parquet
#          processed_candles/_continuous/{ROOT}/_meta/active_contracts.parquet
# Manifest: gs://market-data-tick-tradfi-*/​_index/availability_index.parquet
#
# No skip-if-fresh in run_build_continuous — every run recomputes + overwrites the
# whole requested date range (idempotent output, so a preemption relaunch simply
# replays the same range; there is no partial-progress checkpoint at a finer grain).
#
# Usage:
#   bash launch-mdps-build-continuous-vm.sh ES  2020-01-01 2026-07-25 dry
#   bash launch-mdps-build-continuous-vm.sh ES  2020-01-01 2026-07-25 full
#   bash launch-mdps-build-continuous-vm.sh MES 2020-01-01 2026-07-25 full
#
# Optional flags (before the positional args):
#   --env <prod|staging|dev>              default prod
#   --roll-days-before-expiry <N>         default 8 (ES open-interest crossover window)
#   --timeframes "1m 5m 15m 1h 4h 24h"    space-separated MDPS_TIMEFRAMES override
#   --vm-name <name>                      deterministic name (must still prefix-match
#                                          the registered "mdps-backfill-tradfi-" entry)
#   --on-demand                           opt out of SPOT (backfill VMs default SPOT)
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ROLL_DAYS_BEFORE_EXPIRY=""
TIMEFRAMES_OVERRIDE=""
VM_NAME_OVERRIDE="${VM_NAME_OVERRIDE:-}"
ON_DEMAND=false

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --roll-days-before-expiry) ROLL_DAYS_BEFORE_EXPIRY="$2"; shift 2 ;;
        --timeframes) TIMEFRAMES_OVERRIDE="$2"; shift 2 ;;
        --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=true; shift ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]}"

if [[ $# -lt 4 ]]; then
    echo "Usage: launch-mdps-build-continuous-vm.sh [flags] <ES|MES> <start-date> <end-date> <dry|full>" >&2
    exit 2
fi

ROOT="$1"
START_DATE="$2"
END_DATE="$3"
MODE="$4"

case "$ROOT" in
    ES|MES) ;;
    *) echo "Unknown --root: $ROOT (expected ES or MES)" >&2; exit 2 ;;
esac
case "$MODE" in
    dry|full) ;;
    *) echo "Mode must be 'dry' or 'full' (got: $MODE)" >&2; exit 2 ;;
esac
lc_validate_env "$DEPLOYMENT_ENV"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"
RUN_TS="$(lc_run_ts)"

env_short=""
case "$DEPLOYMENT_ENV" in
    prod)    env_short="prd" ;;
    staging) env_short="stg" ;;
    dev)     env_short="dev" ;;
esac
SOURCE_BUCKET="market-data-tick-tradfi-${env_short}-${PROJECT}"

# --vm-name override wins; default MUST prefix-match the registered
# "mdps-backfill-tradfi-" VM_PREFIX_TO_BUCKET entry (deployment_service/vm_prefix_registry.py)
# so the zombie watchdog + fleet cockpit recognise it — never hand-roll an unregistered prefix.
VM_NAME="mdps-backfill-tradfi-buildcontinuous-$(echo "$ROOT" | tr '[:upper:]' '[:lower:]')-${RUN_TS}"
if [[ -n "$VM_NAME_OVERRIDE" ]]; then
    VM_NAME="$VM_NAME_OVERRIDE"
fi

# Freshness check (+ auto-republish if stale) MUST run BEFORE the tarball SHAs below
# are resolved for the metadata pin — resolving first (as this launcher originally did)
# pins whatever "latest" WAS at that moment, which is the PRE-republish (pre-fix) tarball
# if this repo was stale; the VM then downloads that stale code despite the freshness
# check reporting "fresh" afterward. Root-caused 2026-07-26: a build-continuous launch
# silently ran the OLD process_candles_handler code path on stale market-data-processing-
# service, wasting a VM cycle on the wrong operation.
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-data-processing-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

if [[ -z "${UTL_TARBALL_SHA:-}" ]]; then
    UTL_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" unified-trading-library)"
fi
if [[ -z "${UAC_TARBALL_SHA:-}" ]]; then
    UAC_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" unified-api-contracts)"
fi
if [[ -z "${MDPS_TARBALL_SHA:-}" ]]; then
    MDPS_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" market-data-processing-service)"
fi

cmd="PROTOCOL_DATA_SOURCE_BUCKET_TRADFI=${SOURCE_BUCKET}"
cmd="$cmd MDPS_ASSET_GROUP=TRADFI"
cmd="$cmd MDPS_OPERATION=build-continuous"
cmd="$cmd MDPS_CONTINUOUS_ROOT=${ROOT}"
[[ -n "$ROLL_DAYS_BEFORE_EXPIRY" ]] && cmd="$cmd MDPS_ROLL_DAYS_BEFORE_EXPIRY=${ROLL_DAYS_BEFORE_EXPIRY}"
[[ -n "$TIMEFRAMES_OVERRIDE" ]] && cmd="$cmd MDPS_TIMEFRAMES='${TIMEFRAMES_OVERRIDE}'"
cmd="$cmd python -m market_data_processing_service --operation process --mode batch"
cmd="$cmd --start-date $START_DATE --end-date $END_DATE"
if [[ "$MODE" == "dry" ]]; then
    cmd="$cmd --dry-run"
fi

PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  cmd: $cmd"
echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

md="VM_TASK=mdps-backfill"
md="${md},VM_SERVICE=market_data_processing_service"
md="${md},VM_OPERATION=build-continuous-$(echo "$ROOT" | tr '[:upper:]' '[:lower:]')"
md="${md},VM_ASSET_GROUP=TRADFI"
md="${md},VM_START_DATE=${START_DATE}"
md="${md},VM_END_DATE=${END_DATE}"
md="${md},VM_BACKFILL_CMD=${cmd}"
md="${md},VM_BACKFILL_MODE=${MODE}"
md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
[[ -n "${UTL_TARBALL_SHA:-}" ]]  && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
[[ -n "${UAC_TARBALL_SHA:-}" ]]  && md="${md},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"
[[ -n "${MDPS_TARBALL_SHA:-}" ]] && md="${md},MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA}"

lc_write_tarball_pin_record "$VM_NAME" "$PROJECT" "launch-mdps-build-continuous-vm.sh" \
    "UTL_TARBALL_SHA=${UTL_TARBALL_SHA:-}" \
    "UAC_TARBALL_SHA=${UAC_TARBALL_SHA:-}" \
    "MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA:-}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-data-processing-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# Idempotent output (no skip-if-fresh in run_build_continuous), so a preemption
# relaunch replaying the SAME start/end/root just recomputes the same range.
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-mdps-build-continuous-vm.sh" \
    "VM_NAME_OVERRIDE=${VM_NAME}" \
    "RESUME_ROOT=${ROOT}" \
    "RESUME_START_DATE=${START_DATE}" \
    "RESUME_END_DATE=${END_DATE}" \
    "RESUME_MODE=${MODE}" \
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

# Download-heavy backfill VM: pd-balanced >=250GB per
# check_backfill_vm_disk_provisioning.py (pd-standard throttles under sustained writes).
# shellcheck disable=SC2086
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    ${PROVISIONING_FLAGS} \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
    --labels=purpose=mdps-build-continuous,root="$(echo "$ROOT" | tr '[:upper:]' '[:lower:]')",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo "  SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
