#!/usr/bin/env bash
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + cefi/defi/tradfi/prediction orphan_class_E backfilled to 0
#
# Launch a short-lived SPOT GCE VM that runs the class-E orphan
# characterise->canonicalise->record_captured backfill
# (instruments-service/scripts/backfill_orphan_class_e.py --asset-group <ag> --apply)
# for ONE asset_group, reading the durable orphan_sweep_{ag}.parquet report
# migration_orphan_sweep.py already wrote.
#
# Built for: unified-trading-pm/plans/active/issues/estate_orphan_assessment_2026_07_21.md
# todo 3b -- cefi (935,714) + prediction (3,137,183) real orphans need this backfill;
# a --dry-run attempted on the shared interactive dev box drove RSS to 12.4GB/15GB
# with 5.7GB swap in-use within ~2 minutes (the SAME `_load_manifested_cells()`
# unchunked-parquet-download root cause as cefi's orphan-sweep hang, reached here via
# `reverify_against_index()`), confirming this MUST run on a dedicated VM, not locally.
#
# Memory sizing: e2-highmem-8 default for every asset_group (not just cefi) -- unlike
# migration_orphan_sweep.py (fixed 2026-07-22 to batch + checkpoint), this backfill's
# --apply path still holds EVERY ConvertResult for the WHOLE report in memory at once
# (`results: list[ConvertResult]` in backfill_orphan_class_e.py::main(), no batching)
# on top of the same whole-index download reverify_against_index() does -- prediction's
# report alone is ~3.3x cefi's row count, so defaulting only cefi to a bigger machine
# (mirroring launch-orphan-sweep-vm.sh's per-ag case) would under-provision prediction.
# Bump via MACHINE_TYPE=... if a real run still OOMs (see that doc for the precedent).
#
# READ + WRITE, NOT read-only (unlike the orphan sweep): --apply calls record_captured
# (manifest rows) and, for LEGACY-shaped orphans only, uploads a canonicalised twin
# object (`convert_object()` -- CONVERT path; the common case for orphans discovered by
# migration_orphan_sweep.py, already canonical-shaped, is RECORD_ONLY -- footer-read only,
# no upload). NEVER deletes or mutates the source object either way.
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness VM shape as launch-orphan-sweep-vm.sh, which
# this launcher is a sibling of).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing + manifest join are fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): idempotent re-run
# (convert_object() reuses an existing canonical twin rather than re-uploading; a
# preemption relaunch just redoes any incomplete row group -- NOT harmful, but also NOT
# a measured-progress resume like migration_orphan_sweep.py's checkpoint; a full report
# re-run on relaunch is the safe, simple behavior here). ON_DEMAND=true opts out.
#
# The VM prefix MUST match the registered backfill-orphan-e-{ag}- entry in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only, fixed
# per-AG report path not a per-VM shard) + its launcher_registry.py twin.
#
# Singleton lock is per-asset-group (prefix backfill-orphan-e-{asset_group}-).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Backfill report (only written when --apply, per backfill_orphan_class_e.py::write_report):
#       gs://{ag-tick-bucket}/_index/audit/orphan_backfill_{asset_group}.parquet
#
# Usage:
#   bash launch-backfill-orphan-e-vm.sh cefi
#   bash launch-backfill-orphan-e-vm.sh prediction
#   bash launch-backfill-orphan-e-vm.sh --dry-run cefi     # characterise + sample-verify only, no writes
#   bash launch-backfill-orphan-e-vm.sh --force defi       # bypass singleton lock
#   bash launch-backfill-orphan-e-vm.sh --env staging cefi
#   ON_DEMAND=true bash launch-backfill-orphan-e-vm.sh cefi  # opt out of SPOT
#
# Cost: e2-highmem-8 SPOT + 100GB. Single pass, apply mode by default.
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
        echo "ERROR: sports is EXCLUDED from this launcher -- use backfill_orphan_class_e_sports.py directly" >&2
        exit 2
        ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# See the file header: EVERY asset_group defaults to e2-highmem-8 here (unlike
# launch-orphan-sweep-vm.sh's cefi-only bump) -- this tool's --apply path accumulates
# the WHOLE report's ConvertResult list in memory on top of the same whole-index load.
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="backfill-orphan-e-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: backfill-orphan-e VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
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

case "$DEPLOYMENT_ENV" in
    prod)    _ENV_SHORT="prd" ;;
    staging) _ENV_SHORT="stg" ;;
    *)       _ENV_SHORT="$DEPLOYMENT_ENV" ;;
esac
case "$ASSET_GROUP" in
    prediction) _TICK_BUCKET_DEFAULT="market-data-tick-pred-${_ENV_SHORT}-${PROJECT}" ;;
    *)          _TICK_BUCKET_DEFAULT="market-data-tick-${ASSET_GROUP}-${_ENV_SHORT}-${PROJECT}" ;;
esac
TICK_BUCKET="${TICK_BUCKET:-$_TICK_BUCKET_DEFAULT}"

SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"

BACKFILL_CMD="python ${SCRIPTS}/backfill_orphan_class_e.py"
BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
if $DRY_RUN_MODE; then
    BACKFILL_CMD="${BACKFILL_CMD} --dry-run"
else
    BACKFILL_CMD="${BACKFILL_CMD} --apply"
fi

METADATA="VM_TASK=backfill-orphan-e"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=backfill-orphan-e"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# backfill_orphan_class_e.py logs "converted N/M" every 1000 conversions (main()) and
# "report class-E rows: N" / "re-verify vs LIVE index" once each at startup -- "convert"
# is unique among these and repeats, matching the launcher convention of keying the
# stall timer on a token that recurs throughout the run, not just once at the top.
METADATA="${METADATA},STALL_PROGRESS_REGEX=convert"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-backfill-orphan-e-vm.sh" \
    ASSET_GROUP="$ASSET_GROUP" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: class-E orphan record_captured backfill for asset_group=${ASSET_GROUP}"
echo "  Backfill: backfill_orphan_class_e.py --asset-group ${ASSET_GROUP} $([ "$DRY_RUN_MODE" == "true" ] && echo "--dry-run" || echo "--apply")"
echo "  Reads:    gs://${TICK_BUCKET}/_index/audit/orphan_sweep_${ASSET_GROUP}.parquet"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) -- see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    "${PROVISIONING_ARGS[@]}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=backfill-orphan-e,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'converted N/M' progress lines -- this tool has"
echo "NO per-VM manifest shard to count rows against either; verify the VERDICT line +"
echo "record_captured counts landing in the manifest at completion."
