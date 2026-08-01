#!/usr/bin/env bash
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + orphan-sweep class-E backfilled to 0 for cefi/defi/tradfi/prediction
#
# Launch a short-lived SPOT GCE VM that runs the GCS->manifest orphan sweep
# (migration_orphan_sweep.py --asset-group <ag>) for ONE asset_group. It is the
# INVERSE of the phantom-row reconciler: walks the AG's raw-tick bucket ONCE
# and classifies every object A_canonical_manifested / B_legacy_duplicate /
# C_manifest_infra / C2_non_data / D_junk / E_orphan_real, writing the
# actionable (class-B legacy twins + class-E orphans) rows to an audit parquet.
#
# READ-ONLY, confirmed by reading migration_orphan_sweep.py: main() only calls
# run_sweep() + _print_report() + optionally _write_report() — none mutate the
# source corpus (no delete, no patch of an existing object or manifest row).
# --dry-run is a NO-OP in this tool (the flag is parsed but never read inside
# main()) -- output is controlled solely by --report-out, which this launcher
# always passes so the audit lands somewhere discoverable afterward.
#
# Built for: unified-trading-pm/plans/active/issues/
# estate_orphan_assessment_2026_07_21.md todo 3 -- the defi/cefi/tradfi/
# prediction leg the in-session sweep could not complete (multi-GB
# availability-manifest download hit ChunkedEncodingError -- defi's index is
# ~1.8GB -- and the classification/join phase hung). Sports is EXCLUDED here
# -- it already ran in-session via migration_orphan_sweep_sports.py directly
# (sports paths aren't hive-keyed; canonical_path_templates('sports') == []).
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (the
# Tier-2 two-tier compute model whose SPOT/singleton-lock/tarball-freshness VM
# shape this launcher reuses -- this sweep itself sits at the cheaper census/G1
# tier, not the per-datapoint G2/G3 tier, but the launch machinery is the same).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing is fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): the tool is
# READ-ONLY and naturally idempotent (re-running just re-classifies and
# overwrites the same fixed report path -- no partial state to corrupt), so a
# preemption is a safe restart-from-scratch via the standard RelaunchPreemptedVm
# path. UNLIKE the Tier-2 datapoint-validator, this tool has NO per-shard/day
# checkpoint -- there is no measured-progress frontier to resume from, so a
# relaunch re-walks the whole corpus (a restart, not a resume).
# ON_DEMAND=true is the only opt-out.
#
# The VM prefix MUST match the registered orphan-sweep-{ag}- entry in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only,
# because this tool writes a FIXED per-AG report path, not a per-VM shard --
# pointing bucket at the tick bucket would make the fleet monitor look for a
# `_index/per_vm/{vm}.parquet` this tool never writes) + its launcher_registry.py
# twin.
#
# Singleton lock is per-asset-group (prefix orphan-sweep-{asset_group}-) so
# different asset_groups can sweep in parallel safely.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Audit report (class-B legacy twins + class-E orphans):
#       gs://{ag-tick-bucket}/_index/audit/orphan_sweep_{asset_group}.parquet
#     (mirrors the sports run's gs://{bucket}/_index/audit/orphan_sweep_sports.parquet)
#
# Usage:
#   bash launch-orphan-sweep-vm.sh cefi
#   bash launch-orphan-sweep-vm.sh defi
#   bash launch-orphan-sweep-vm.sh tradfi
#   bash launch-orphan-sweep-vm.sh prediction
#   bash launch-orphan-sweep-vm.sh --force defi          # bypass singleton lock
#   bash launch-orphan-sweep-vm.sh --env staging defi
#   ON_DEMAND=true bash launch-orphan-sweep-vm.sh defi    # opt out of SPOT
#
# Cost: e2-standard-4 SPOT + 100GB. Read-only single walk.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

_positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
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
        echo "ERROR: sports is EXCLUDED from this launcher -- sports paths aren't hive-keyed" >&2
        echo "(canonical_path_templates('sports') == []). Run migration_orphan_sweep_sports.py directly instead." >&2
        exit 2
        ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# cefi default bumped to e2-highmem-8 (2026-07-22): cefi's consolidated
# availability_index.parquet is the largest of any asset_group's (most venues,
# longest history) and `_load_manifested_cells()` reads it whole into memory
# (single `download_bytes` + `pd.read_parquet(BytesIO(...))`, no streaming) before
# the walk even starts. Two real launches on the default e2-standard-4 (16GB)
# produced ZERO log/heartbeat output for 30-40+ minutes each -- a signature that
# matches the confirmed OOM precedent in launch-expected-universe-v2-vm.sh (a
# 63.9M-row defi run OOM-killed on e2-standard-4, fixed via e2-standard-16) more
# than a GCS/network flake. tradfi/prediction measurably ran fine on e2-standard-4
# (the separate throughput-decay bug they hit is a workers-based concurrency fix,
# not a memory one -- see migration_orphan_sweep_performance_decay_2026_07_22.md).
# defi ALSO bumped 2026-07-23: ran fine on e2-standard-4 for 3 earlier same-session
# attempts (whose failures were 2x SPOT preemption + 1x an unrelated in-memory
# actionable-accumulator OOM, now separately fixed), but its 4th and 5th attempts
# BOTH hit the identical index-load freeze cefi had (system-wide silence, incl. the
# independent bash heartbeat loop -- confirmed via serial console, not just the
# Python log) -- defi's manifest had evidently grown enough since its earlier
# attempts (hours of ongoing unrelated production capture) to now also exceed
# e2-standard-4's headroom for this same one-shot whole-index load.
case "$ASSET_GROUP" in
    cefi|defi) MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-8}" ;;
    *)         MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}" ;;
esac
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) -- the
# sweep is read-only (never deletes/patches), so a preemption relaunch is a safe
# restart-from-scratch. ON_DEMAND=true is the only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

# Singleton check per-asset-group (different asset_groups may sweep in parallel).
SINGLETON_PREFIX="orphan-sweep-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: orphan-sweep VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP}

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" -- a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

# Env-tiered tick-bucket name (bucket_name_ssot shape -- mirrors
# resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=<ag>), verified
# against configs/cloud-providers.yaml; same construction as
# launch-canonical-migration-vm.sh's TRADFI_TICK_BUCKET). This is the SAME bucket
# migration_orphan_sweep.py's own _resolve_bucket() resolves internally, so the
# report path below lands where the tool's own docstring says it writes.
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
REPORT_OUT="gs://${TICK_BUCKET}/_index/audit/orphan_sweep_${ASSET_GROUP}.parquet"

# instruments-service-code.tar.gz is extracted to $WORKSPACE/instruments/ by
# setup-data-pipeline-vm.sh (VM_SERVICE=instruments_service), so the sweep lives at:
SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"

# Single `python ` token -> setup-data-pipeline-vm.sh substitutes it to $VENV/bin/python.
BACKFILL_CMD="python ${SCRIPTS}/migration_orphan_sweep.py"
BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
BACKFILL_CMD="${BACKFILL_CMD} --report-out ${REPORT_OUT}"

METADATA="VM_TASK=orphan-sweep"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=orphan-sweep"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# Stall-progress marker: migration_orphan_sweep.py logs "N objects swept (X/s)"
# every 50,000 objects (run_sweep()) -- reset the stall timer only on a genuine
# walk-progress line, not on generic noise. "swept" is the distinguishing token
# (no spaces, matching every other launcher's STALL_PROGRESS_REGEX convention).
# Generous timeout: multi-million-object cefi/tradfi buckets + the lazy footer
# reads for small class-E candidates can legitimately run long between markers.
METADATA="${METADATA},STALL_PROGRESS_REGEX=swept"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Persist the exact launch scope BEFORE create so a SPOT-preemption relaunch
# (RelaunchPreemptedVm) replays the same asset_group (a restart-from-scratch --
# this tool has no day/shard checkpoint to resume from). Best-effort; never
# fails the launch.
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-orphan-sweep-vm.sh" \
    ASSET_GROUP="$ASSET_GROUP" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: GCS->manifest orphan sweep for asset_group=${ASSET_GROUP}"
echo "  Sweep:   migration_orphan_sweep.py (READ-ONLY -- classifies, never deletes/patches)"
echo "  Report:  ${REPORT_OUT}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library \
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
    --labels=purpose=orphan-sweep,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Report:      gcloud storage ls ${REPORT_OUT}"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'objects swept' progress lines (this tool has"
echo "NO per-VM manifest shard to count rows against -- it's a single full-corpus walk"
echo "ending in ONE final report write, not incremental shard growth):"
echo "  - STARTED < 60s; require log growth + a new 'objects swept' line periodically;"
echo "    flat => STALL => diagnose; verify the report parquet exists at completion."
