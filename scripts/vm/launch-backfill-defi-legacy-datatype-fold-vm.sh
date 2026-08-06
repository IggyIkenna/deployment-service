#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: the dex_pools/dex_swaps/rate_indices legacy data_type fold prod-run
#   confirms 0 missing_source AND a --dry-run re-run over the full worklist shows the
#   steady-state needs_fold shard count has stopped shrinking (only genuinely new
#   pre-2026-06-02-shaped writes would remain, which should be none going forward).
#
# Launch a short-lived SPOT GCE VM that runs the legacy DeFi data_type fold
# (market-tick-data-service/scripts/fold_legacy_dex_pools_swaps_rate_indices_2026_08_04.py --apply)
# over the FULL manifest-driven worklist (~29k (venue,chain,instrument_type,day,
# pipeline_mode) shards / ~3.9M individual legacy objects measured 2026-08-04).
#
# Built for: unified-trading-pm/plans/active/issues/
#   defi_legacy_data_type_names_manifest_migration_scope_2026_08_04.md
# `dex_pools`/`dex_swaps` were fixed forward-only at market-tick-data-service@0a3a7071
# (2026-06-02) to emit canonical `dex_pool_state`/`dex_pool_swaps`; every capture BEFORE
# that commit is still sitting at the legacy hive path. `rate_indices`'s canonical target
# is `lending_indices` (a different, pre-existing alias, not a pools/swaps sibling).
# Content-verified 2026-08-04 (bounded sample, this issue's DIAG todo): well-formed,
# schema-identical to canonical rows for the same cell -- the divergence is coverage
# (0%/88.8%/100% of legacy cells have NO canonical twin for dex_pools/dex_swaps/
# rate_indices respectively), not corruption, so folding is a real coverage gain.
#
# READ + WRITE: --apply downloads each legacy object, rewrites its `data_type` column to
# the canonical value via the PRODUCTION write_defi_rows() path/schema builder, and
# uploads to the canonical path (COPY-not-move — never touches/deletes the legacy
# source) + registers a per-VM-sharded manifest row. Safe-idempotent per CLAUDE.md's
# VM-launch carve-out: every destination write is `blob_exists`-gated (already-folded
# cells are cheap no-op skips) and the script's own resume-log
# (fold_legacy_dex_pools_swaps_rate_indices_2026_08_04.resume.jsonl, staged to GCS by
# setup-data-pipeline-vm.sh's standard log/artifact upload) lets a relaunch skip
# already-applied shards without re-listing them.
#
# per_vm_shards=True (REQUIRED, wired into the script itself) — the default
# ManifestWriter direct-CAS path read-merge-writes the ENTIRE 38M+-row consolidated DeFi
# index on every flush and has already taken the agent-orchestrator fleet offline once
# for exactly this mistake (mtds_gas_fees_migration_script_unbounded_memory_2026_07_30.md).
#
# Run same-region (asia-northeast1-c) so the per-shard GCS list + download + upload is fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md). ON_DEMAND=true opts out.
#
# The VM prefix MUST match the registered backfill-defi-legacy-datatype-fold- entry in
# deployment_service/vm_prefix_registry.py (bucket=market-data-tick-defi-{env}-{pid} --
# per-VM-sharded manifest writer, NOT heartbeat-only) + its launcher_registry.py twin.
#
# Singleton lock: prefix backfill-defi-legacy-datatype-fold- (defi-only tool, no
# asset_group arg).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Manifest: per-VM shard at gs://{defi-tick-bucket}/_index/per_vm/{vm_name}.parquet,
#     merged into the consolidated index by the standing manifest consolidator.
#   - Resume log: fold_legacy_dex_pools_swaps_rate_indices_2026_08_04.resume.jsonl
#     (VM-local; the run.log carries the same progress info for post-hoc review).
#
# Usage:
#   bash launch-backfill-defi-legacy-datatype-fold-vm.sh                    # --apply, all 3 data_types
#   bash launch-backfill-defi-legacy-datatype-fold-vm.sh --dry-run          # plan + dst-check only, no writes
#   bash launch-backfill-defi-legacy-datatype-fold-vm.sh --only dex_swaps   # one data_type
#   bash launch-backfill-defi-legacy-datatype-fold-vm.sh --force           # bypass singleton lock
#   bash launch-backfill-defi-legacy-datatype-fold-vm.sh --env staging
#   ON_DEMAND=true bash launch-backfill-defi-legacy-datatype-fold-vm.sh
#
# Cost: e2-standard-4 SPOT + 100GB. Single pass, apply mode by default.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
MODE="--apply"
ONLY_ARG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --dry-run) MODE=""; shift ;;
        --only) ONLY_ARG="--only $2"; shift 2 ;;
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
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="backfill-defi-legacy-datatype-fold-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: backfill-defi-legacy-datatype-fold VM already running in $ZONE: $EXISTING
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

SCRIPTS="/home/ikennaigboaka/workspace/mtds/scripts"

BACKFILL_CMD="python ${SCRIPTS}/fold_legacy_dex_pools_swaps_rate_indices_2026_08_04.py ${MODE} --workers 24 ${ONLY_ARG}"

METADATA="VM_TASK=backfill-defi-legacy-datatype-fold"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=backfill-defi-legacy-datatype-fold"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
# The tool logs "progress: N/M shards done  totals=..." every 200 shards -- key the
# stall timer on that recurring token (mirrors the "day=" precedent in
# launch-backfill-defi-dex-swaps-source-correction-vm.sh; a per-shard log line would be
# noisier without adding real signal since shards complete out-of-order across 24 threads).
METADATA="${METADATA},STALL_PROGRESS_REGEX=progress:"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-backfill-defi-legacy-datatype-fold-vm.sh" \
    DEPLOYMENT_ENV="$DEPLOYMENT_ENV" MODE="$MODE" ONLY_ARG="$ONLY_ARG"

echo "Launching $VM_NAME: defi legacy data_type fold (mode=${MODE:-dry-run} ${ONLY_ARG})"
echo "  Backfill: fold_legacy_dex_pools_swaps_rate_indices_2026_08_04.py ${MODE} --workers 24 ${ONLY_ARG}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library \
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
    --labels=purpose=backfill-defi-legacy-datatype-fold,asset-group=defi,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'progress: N/M shards done' lines --"
echo "verify the final totals line + record_captured counts landing in the manifest at completion."
