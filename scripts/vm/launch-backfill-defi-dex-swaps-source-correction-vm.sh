#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after the defi prod-run confirms 0 copy_errors/footer_failed AND a
#   --scope re-run over the full date range shows the steady-state needs_copy count
#   has stopped growing (the remaining, if any, is expected to be genuinely new
#   post-fix writes, not a backlog).
#
# Launch a short-lived SPOT GCE VM that runs the DeFi dex_pool_swaps source-
# correction backfill
# (market-data-processing-service/scripts/backfill_defi_dex_pool_swaps_source_correction.py --apply)
# over ALL days of defi's processed_candles/ corpus (1,155 distinct days measured
# 2026-08-03, 2023-01-01..2026-08-02).
#
# Built for: unified-trading-pm/plans/active/issues/mdps_candle_manifest_near_total_coverage_gap_2026_07_27.md
# (last open todo) -- defi's dex_pool_swaps objects mis-tagged
# pipeline_mode=batch_onchain_rpc (a pre-012ccec1 (2026-06-08) SOURCE_PRIORITY
# registration bug's fallback path) need per-object twin detection + copy-not-move +
# record_captured(source=onchain_subgraph) for genuine gaps (twin absent) -- a
# bounded prod sample (2026-08-03) found real gap sizes from 0 to ~10,300/day,
# rough order-of-magnitude estimate ~1-2M objects total across the full range.
#
# READ + WRITE: --apply calls gcs_copy_object (server-side copy, never deletes or
# mutates the source object) + record_captured. Safe-idempotent per CLAUDE.md's
# VM-launch carve-out: a re-run's per-object dst-exists check makes every already-
# copied object a no-op skip, and the tool's own day-level checkpoint
# (processed_candles/_index/_dex_swaps_source_correction_checkpoint.json) resumes
# from measured progress on a SPOT preemption relaunch rather than replaying from
# day one -- satisfies the PROGRESS-checkpoint HARD RULE without a separate
# mechanism.
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness VM shape as launch-backfill-candle-manifest-vm.sh,
# which this launcher is a direct sibling of -- same VM_BACKFILL_CMD dispatch
# mechanism, own VM_TASK branch per the no-dispatch-branch recurring-bug precedent).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing + per-object
# dst-check are fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md). ON_DEMAND=true
# opts out.
#
# The VM prefix MUST match the registered backfill-defi-dex-swaps- entry in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only, the
# tool's own day-checkpoint is its resumability mechanism, not a per-VM shard) +
# its launcher_registry.py twin.
#
# Singleton lock: prefix backfill-defi-dex-swaps- (defi-only tool, no asset_group arg).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Day-level checkpoint: gs://{defi-tick-bucket}/processed_candles/_index/_dex_swaps_source_correction_checkpoint.json
#   - Audit report (only written when --apply):
#       gs://{defi-tick-bucket}/_index/audit/defi_dex_pool_swaps_source_correction.parquet
#
# Usage:
#   bash launch-backfill-defi-dex-swaps-source-correction-vm.sh              # --apply, all days
#   bash launch-backfill-defi-dex-swaps-source-correction-vm.sh --dry-run    # classify + dst-check only, no writes
#   bash launch-backfill-defi-dex-swaps-source-correction-vm.sh --scope      # cheap count-only pass
#   bash launch-backfill-defi-dex-swaps-source-correction-vm.sh --force      # bypass singleton lock
#   bash launch-backfill-defi-dex-swaps-source-correction-vm.sh --env staging
#   ON_DEMAND=true bash launch-backfill-defi-dex-swaps-source-correction-vm.sh
#
# Cost: e2-highmem-8 SPOT + 250GB. Single pass, apply mode by default.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
MODE="--apply"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --dry-run) MODE="--dry-run"; shift ;;
        --scope) MODE="--scope"; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) echo "ERROR: unrecognised argument: $1" >&2; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

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

SINGLETON_PREFIX="backfill-defi-dex-swaps-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: backfill-defi-dex-swaps VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect above that it
is genuinely stale. It may be another dispatch's actively progressing VM.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

SCRIPTS="/home/ikennaigboaka/workspace/mdps/scripts"

BACKFILL_CMD="python ${SCRIPTS}/backfill_defi_dex_pool_swaps_source_correction.py ${MODE}"

METADATA="VM_TASK=backfill-defi-dex-swaps"
METADATA="${METADATA},VM_SERVICE=market_data_processing_service"
METADATA="${METADATA},VM_OPERATION=backfill-defi-dex-swaps"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# the tool logs "day=<d>: already_covered=... needs_copy=..." for every non-empty
# day and "checkpoint: N/M days done" every 20 days -- "checkpoint" recurs
# throughout a full --apply run, matching the launcher convention of keying the
# stall timer on a token that repeats, not just once at the top.
METADATA="${METADATA},STALL_PROGRESS_REGEX=checkpoint"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-backfill-defi-dex-swaps-source-correction-vm.sh" \
    DEPLOYMENT_ENV="$DEPLOYMENT_ENV" MODE="$MODE"

echo "Launching $VM_NAME: defi dex_pool_swaps source-correction backfill (mode=${MODE})"
echo "  Backfill: backfill_defi_dex_pool_swaps_source_correction.py ${MODE}"

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
    --labels=purpose=backfill-defi-dex-swaps,asset-group=defi,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'checkpoint: N/M days done' progress lines --"
echo "verify the VERDICT line + record_captured counts landing in the manifest at completion."
