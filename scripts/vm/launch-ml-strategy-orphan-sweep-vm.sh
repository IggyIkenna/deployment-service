#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + ml_orphan_sweep/strategy_orphan_sweep class-E
#   backfilled to 0 for both corpora
#
# Launch a short-lived SPOT GCE VM that runs the GCS->manifest orphan sweep for
# EITHER the ml-service ml_predictions corpus (ml_orphan_sweep.py) OR the
# strategy-service strategy_instructions corpus (strategy_orphan_sweep.py).
# Direct sibling of launch-feature-orphan-sweep-vm.sh -- same READ-ONLY A-E
# classification pattern, one launcher covering both corpora (each is a single
# global cell -- no per-asset_group axis, unlike features) via --target.
#
# READ-ONLY, confirmed by reading both scripts: main() only calls run_sweep() +
# _print_report() + optionally _write_report() -- neither mutates the source
# corpus (no delete, no patch of an existing object or manifest row; each
# tool's own checkpoint write is its own internal resume-state file).
#
# Built for: unified-trading-pm/plans/active/issues/
# mdps_features_ml_strategy_orphan_sweep_tooling_gap_2026_07_27.md todo 3b --
# the real-GCS validation leg for the 2 sweeps todo 3 built (but never ran
# against real data).
#
# SSOT: codex/02-data/orphan-object-detection.md;
# codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness launch shape).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing is fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): both tools
# are READ-ONLY and checkpoint-resumable on restart (see each tool's own
# _load_checkpoint/_write_checkpoint). ON_DEMAND=true is the only opt-out.
#
# Machine sizing: UNVERIFIED for either corpus as of first write (2026-08-03) --
# no ml_predictions/strategy_instructions manifest has been measured for this
# sweep's memory profile yet. Defaults to e2-standard-4; override via
# MACHINE_TYPE= if a real run OOMs (matching the raw-tick/candle sweeps'
# documented escalation path: bump machine size, don't silently retry).
#
# VM_SERVICE=ml_service had NO SERVICE_TARBALLS/TARBALL_DIRS entry before this
# dispatch (setup-data-pipeline-vm.sh) -- launch-ml-vm.sh has set
# VM_SERVICE=ml_service since before this launcher existed, but with no
# registry entry it silently fell through to the "install all available
# tarballs" WARNING branch, which itself iterates ONLY TARBALL_DIRS keys --
# and ml-service-code was ALSO missing from THAT table, so ml-service code was
# never actually extracted on ANY VM_SERVICE=ml_service VM. Fixed in the same
# commit as this launcher (setup-data-pipeline-vm.sh: added both entries) --
# same "no dispatch branch" bug class this codebase catches on first real
# launch repeatedly (see orphan-sweep/feature-orphan-sweep VM_TASK comments in
# that script).
#
# The VM prefix MUST match the registered ml-orph-/strat-orph- entries in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only,
# same reasoning as feat-orph-: each tool writes a FIXED report path staged
# under CODE_BUCKET, not a per-VM manifest shard) + their launcher_registry.py
# twins.
#
# Singleton lock is per target (ml-orph- / strat-orph-) since each is a single
# global cell -- only one sweep per corpus can usefully run at a time.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Audit report (class-E orphans): gs://{CODE_BUCKET}/ml-strategy-orphan-sweep/
#     {RUN_TS}/{vm_name}/orphan_sweep_{target}.parquet (staged under CODE_BUCKET,
#     mirrors the candle/feature-orphan-sweep launchers' own staging choice)
#
# Usage:
#   bash launch-ml-strategy-orphan-sweep-vm.sh --target ml
#   bash launch-ml-strategy-orphan-sweep-vm.sh --target strategy
#   bash launch-ml-strategy-orphan-sweep-vm.sh --force --target ml
#   bash launch-ml-strategy-orphan-sweep-vm.sh --env staging --target strategy
#   ON_DEMAND=true bash launch-ml-strategy-orphan-sweep-vm.sh --target ml
#
# Cost: e2-standard-4 SPOT + 250GB. Read-only single walk per target.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
TARGET=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --target) TARGET="$2"; shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

SINGLETON_PREFIX=""
VM_SERVICE=""
VM_OPERATION=""
SCRIPT_REL=""
REPO_FOR_TARBALL=""
WORKSPACE_SUBDIR=""
VM_TASK=""
case "$TARGET" in
    ml)
        SINGLETON_PREFIX="ml-orph-"
        VM_SERVICE="ml_service"
        VM_OPERATION="ml-orphan-sweep"
        VM_TASK="ml-orphan-sweep"
        SCRIPT_REL="scripts/ml_orphan_sweep.py"
        REPO_FOR_TARBALL="ml-service"
        WORKSPACE_SUBDIR="ml"
        ;;
    strategy)
        SINGLETON_PREFIX="strat-orph-"
        VM_SERVICE="strategy_service"
        VM_OPERATION="strategy-orphan-sweep"
        VM_TASK="strategy-orphan-sweep"
        SCRIPT_REL="scripts/strategy_orphan_sweep.py"
        REPO_FOR_TARBALL="strategy-service"
        WORKSPACE_SUBDIR="strategy"
        ;;
    *)
        echo "ERROR: --target must be one of ml/strategy (got: '$TARGET')" >&2
        exit 2
        ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
# 250GB minimum (not 100GB): GCP PD throughput scales with disk SIZE (~0.28 MB/s per GB),
# so a 100GB disk only sustains ~28 MB/s vs the ~70 MB/s this backfill-VM workload class
# needs -- see plans/active/issues/backfill_vm_disk_starvation_misdiagnosed_as_tardis_quota_2026_07_18.md.
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) -- both
# sweeps are read-only (never delete/patch), so a preemption relaunch is a safe restart
# (checkpoint-resumable per each tool's own checkpoint contract). ON_DEMAND=true is the
# only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: ml-strategy-orphan-sweep VM already running for target=${TARGET} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force --target ${TARGET}

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction"). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

REPORT_OUT="gs://${CODE_BUCKET}/ml-strategy-orphan-sweep/${RUN_TS}/${VM_NAME}/orphan_sweep_${TARGET}.parquet"

# {repo}-code.tar.gz is extracted to $WORKSPACE/{subdir}/ by setup-data-pipeline-vm.sh
# (VM_SERVICE=ml_service -> ml, VM_SERVICE=strategy_service -> strategy), so the sweep
# lives at:
SCRIPTS="/home/ikennaigboaka/workspace/${WORKSPACE_SUBDIR}/scripts"

# Single `python ` token -> setup-data-pipeline-vm.sh substitutes it to $VENV/bin/python.
BACKFILL_CMD="python ${SCRIPTS}/$(basename "$SCRIPT_REL")"
BACKFILL_CMD="${BACKFILL_CMD} --report-out ${REPORT_OUT}"

METADATA="VM_TASK=${VM_TASK}"
METADATA="${METADATA},VM_SERVICE=${VM_SERVICE}"
METADATA="${METADATA},VM_OPERATION=${VM_OPERATION}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# Stall-progress marker: both sweeps log "N ... objects swept (X/s)" every 50,000
# objects (run_sweep()) -- reset the stall timer only on a genuine walk-progress line.
# "swept" is the distinguishing token, matching the raw-tick/candle/feature sweeps'
# own STALL_PROGRESS_REGEX convention.
METADATA="${METADATA},STALL_PROGRESS_REGEX=swept"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Persist the exact launch scope BEFORE create so a SPOT-preemption relaunch
# (RelaunchPreemptedVm) replays the same target. Best-effort; never fails the launch.
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-ml-strategy-orphan-sweep-vm.sh" \
    TARGET="$TARGET" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: GCS->manifest orphan sweep for target=${TARGET}"
echo "  Sweep:   ${SCRIPT_REL} (READ-ONLY -- classifies, never deletes/patches)"
echo "  Report:  ${REPORT_OUT}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        "$REPO_FOR_TARBALL" unified-api-contracts unified-trading-library \
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
    --labels=purpose=ml-strategy-orphan-sweep,target="${TARGET}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Report:      gcloud storage ls ${REPORT_OUT}"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'objects swept' progress lines (this tool has NO"
echo "per-VM manifest shard to count rows against -- it's a single-target walk ending in ONE"
echo "final report write, not incremental shard growth):"
echo "  - STARTED < 60s; require log growth + a new 'objects swept' line periodically;"
echo "    flat => STALL => diagnose; verify the report parquet exists at completion."
