#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed for cefi/defi/tradfi/prediction + a re-run of
#   candle_orphan_sweep.py (same --manifest-fix-cutover) shows class_E == 0.
#
# Launch a short-lived SPOT GCE VM that runs the candle-corpus class-E/F
# record_captured backfill
# (market-data-processing-service/scripts/backfill_candle_manifest.py --asset-group <ag> --apply)
# for ONE asset_group, reading the durable orphan_sweep_{ag}.parquet report
# candle_orphan_sweep.py already wrote (staged under the CODE_BUCKET
# canonical-migration-candle-orphan-sweep/{run-ts}/... tree — pass its full URI
# via REPORT_URI, this launcher does not guess a fixed default path).
#
# Built for: unified-trading-pm/plans/active/issues/mdps_candle_manifest_near_total_coverage_gap_2026_07_27.md
# todo 1 -- cefi/defi/tradfi/prediction's ~2.6M unmanifested processed_candles/
# objects (class F pre-fix historical gap + DeFi's 7,936 class-E post-cutover
# orphans) need this backfill.
#
# READ + WRITE, NOT read-only (unlike the sweep): --apply calls record_captured
# (manifest rows). This is RECORD-ONLY -- it NEVER uploads a rewritten copy of the
# source object and NEVER deletes/mutates it either way (candle objects are
# already canonical-shaped; the gap is purely a missing manifest row).
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness VM shape as launch-backfill-orphan-e-vm.sh,
# which this launcher is a direct sibling of).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing + manifest join are fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): idempotent re-run
# (a preemption relaunch just re-footer-reads + re-records any incomplete cell --
# record_captured on an already-manifested cell is a normal, safe re-write, not
# harmful). ON_DEMAND=true opts out.
#
# The VM prefix MUST match the registered backfill-candle-manifest-{ag}- entry in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only, fixed
# per-AG report path not a per-VM shard) + its launcher_registry.py twin.
#
# Singleton lock is per-asset-group (prefix backfill-candle-manifest-{asset_group}-).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Backfill report (only written when --apply):
#       gs://{ag-tick-bucket}/_index/audit/candle_manifest_backfill_{asset_group}.parquet
#
# Usage:
#   REPORT_URI=gs://deployment-scripts-central-element-323112/canonical-migration-candle-orphan-sweep/<ts>/<vm>/orphan_sweep_cefi.parquet \
#     bash launch-backfill-candle-manifest-vm.sh cefi
#   REPORT_URI=... bash launch-backfill-candle-manifest-vm.sh --dry-run defi
#   REPORT_URI=... bash launch-backfill-candle-manifest-vm.sh --force tradfi
#   REPORT_URI=... bash launch-backfill-candle-manifest-vm.sh --env staging cefi
#   REPORT_URI=... ON_DEMAND=true bash launch-backfill-candle-manifest-vm.sh cefi
#
# Cost: e2-highmem-8 SPOT + 250GB. Single pass, apply mode by default.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN_MODE=false

_positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN_MODE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) _positional+=("$1"); shift ;;
    esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ASSET_GROUP="${1:-defi}"

case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction) ;;
    sports)
        echo "ERROR: sports is EXCLUDED from this launcher -- sports candle-manifest coverage is ~100% already (see mdps_candle_manifest_near_total_coverage_gap_2026_07_27.md)" >&2
        exit 2
        ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac

if [[ -z "${REPORT_URI:-}" ]]; then
    echo "ERROR: REPORT_URI env var is required -- the candle_orphan_sweep.py --report-out URI for ${ASSET_GROUP}" >&2
    exit 2
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="backfill-candle-manifest-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: backfill-candle-manifest VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP}

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect above that it
is genuinely stale. It may be another dispatch's actively progressing VM.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

SCRIPTS="/home/ikennaigboaka/workspace/mdps/scripts"

BACKFILL_CMD="python ${SCRIPTS}/backfill_candle_manifest.py"
BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP} --report-uri ${REPORT_URI}"
if $DRY_RUN_MODE; then
    BACKFILL_CMD="${BACKFILL_CMD} --dry-run"
else
    BACKFILL_CMD="${BACKFILL_CMD} --apply"
fi

METADATA="VM_TASK=backfill-candle-manifest"
METADATA="${METADATA},VM_SERVICE=market_data_processing_service"
METADATA="${METADATA},VM_OPERATION=backfill-candle-manifest"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# backfill_candle_manifest.py logs "footer-read N/M" and "recorded N cells" periodically
# -- "footer-read" recurs throughout the run, matching the launcher convention of keying
# the stall timer on a token that repeats, not just once at the top.
METADATA="${METADATA},STALL_PROGRESS_REGEX=footer-read"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-backfill-candle-manifest-vm.sh" \
    ASSET_GROUP="$ASSET_GROUP" DEPLOYMENT_ENV="$DEPLOYMENT_ENV" REPORT_URI="$REPORT_URI"

echo "Launching $VM_NAME: candle-manifest class-E/F record_captured backfill for asset_group=${ASSET_GROUP}"
echo "  Backfill: backfill_candle_manifest.py --asset-group ${ASSET_GROUP} --report-uri ${REPORT_URI} $([ "$DRY_RUN_MODE" == "true" ] && echo "--dry-run" || echo "--apply")"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-data-processing-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) -- see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    "${PROVISIONING_ARGS[@]}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=backfill-candle-manifest,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'footer-read N/M' progress lines -- this tool has"
echo "NO per-VM manifest shard to count rows against either; verify the VERDICT line +"
echo "record_captured counts landing in the manifest at completion."
