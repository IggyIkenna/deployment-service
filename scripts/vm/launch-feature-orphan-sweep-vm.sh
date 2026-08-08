#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + feature_orphan_sweep class-E backfilled to 0
#   for every (feature_family x asset_group) cell below
#
# Launch a short-lived SPOT GCE VM that runs the GCS->manifest orphan sweep
# (features-service/scripts/feature_orphan_sweep.py --feature-family <family>
# [--asset-group <ag>]) for ONE (feature_family x asset_group) cell. Direct
# sibling of launch-orphan-sweep-vm.sh (instruments-service raw-tick corpus) and
# launch-canonical-migration-vm.sh's *-candle-orphan-sweep category
# (market-data-processing-service candle corpus) -- same READ-ONLY A-E
# classification pattern, adapted here because EACH feature_family writes to
# its OWN bucket/prefix/partition shape (feature_orphan_sweep.py's own module
# docstring), so a walk is scoped to ONE family x ONE asset_group per
# invocation, not one asset_group covering every family the way the candle/
# raw-tick sweeps do.
#
# READ-ONLY, confirmed by reading feature_orphan_sweep.py: main() only calls
# run_sweep() + _print_report() + optionally _write_report() -- none mutate
# the source corpus (no delete, no patch of an existing object or manifest
# row; the tool's own checkpoint write is its own internal resume-state file,
# not manifest/source data).
#
# Built for: unified-trading-pm/plans/active/issues/
# mdps_features_ml_strategy_orphan_sweep_tooling_gap_2026_07_27.md todo 2b --
# the real-GCS validation leg for the 5 wired families the tool was built (but
# never run against real data) for in the same doc's todo 2.
#
# SSOT: codex/02-data/orphan-object-detection.md;
# codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness launch shape).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing is fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): the tool is
# READ-ONLY and idempotent-on-restart for 6 of 7 wired families (a fresh
# invocation re-walks from scratch, or resumes from its own checkpoint file if
# one exists -- see feature_orphan_sweep.py's _load_checkpoint/_write_checkpoint).
# ON_DEMAND=true is the only opt-out.
#
# Machine sizing: UNVERIFIED for this corpus as of first write (2026-08-03) --
# no feature-family manifest has been measured for this sweep's memory profile
# yet, unlike the raw-tick/candle sweeps which have confirmed multi-GB OOM
# history on e2-standard-4 for cefi/defi specifically (see
# launch-orphan-sweep-vm.sh's own sizing comment). Defaults to e2-standard-4;
# override via MACHINE_TYPE= if a real run OOMs (matching the exact escalation
# path the raw-tick sweep needed: bump machine size, don't silently retry).
#
# The VM prefix MUST match the registered feat-orph- entry in
# deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only,
# same reasoning as orphan-sweep-{ag}-: this tool writes a FIXED per-cell
# report path staged under CODE_BUCKET, not a per-VM manifest shard) + its
# launcher_registry.py twin. Prefix is deliberately short (`feat-orph-`, not
# `feature-orphan-sweep-`) to leave headroom for the family+asset_group
# abbreviation under GCE's 63-char instance-name cap (the exact class of bug
# that hit the candle-orphan-sweep prediction launch -- see
# mdps_features_ml_strategy_orphan_sweep_tooling_gap_2026_07_27.md's todo 1
# progress log).
#
# Singleton lock is per (family, asset_group) cell (prefix
# feat-orph-{family_abbrev}-{ag_abbrev}-) so different cells sweep in parallel
# safely.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Audit report (class-E orphans, class-B legacy-shape twins for
#     delta_one/volatility): gs://{CODE_BUCKET}/feature-orphan-sweep/{RUN_TS}/
#     {vm_name}/orphan_sweep_{family}_{ag_or_global}.parquet
#     (staged under CODE_BUCKET, not the family's own features bucket --
#     mirrors the candle-orphan-sweep launcher's own staging choice, since
#     several families share a folded per-asset_group `features-{ag}` bucket
#     and a fixed per-cell path there would collide across families)
#
# Usage:
#   bash launch-feature-orphan-sweep-vm.sh --feature-family delta_one --asset-group cefi
#   bash launch-feature-orphan-sweep-vm.sh --feature-family volatility --asset-group tradfi
#   bash launch-feature-orphan-sweep-vm.sh --feature-family onchain          # fixed: defi
#   bash launch-feature-orphan-sweep-vm.sh --feature-family sports           # fixed: sports
#   bash launch-feature-orphan-sweep-vm.sh --feature-family calendar         # global, no --asset-group
#   bash launch-feature-orphan-sweep-vm.sh --feature-family commodity       # global, no --asset-group
#   bash launch-feature-orphan-sweep-vm.sh --feature-family cross_instrument --asset-group prediction
#   bash launch-feature-orphan-sweep-vm.sh --feature-family multi_timeframe --asset-group defi
#   bash launch-feature-orphan-sweep-vm.sh --force --feature-family delta_one --asset-group cefi
#   bash launch-feature-orphan-sweep-vm.sh --env staging --feature-family sports
#   ON_DEMAND=true bash launch-feature-orphan-sweep-vm.sh --feature-family onchain
#
# Cost: e2-standard-4 SPOT + 250GB. Read-only single walk, one family prefix
# per invocation (never a whole-bucket or cross-family walk).
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
FEATURE_FAMILY=""
ASSET_GROUP=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --feature-family) FEATURE_FAMILY="$2"; shift 2 ;;
        --asset-group) ASSET_GROUP="$2"; shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$FEATURE_FAMILY" ]]; then
    echo "ERROR: --feature-family is required" >&2
    exit 2
fi

# Mirrors feature_orphan_sweep.py's own _FAMILY_CONFIGS / resolve_feature_group_asset_group
# validation (todo 2b: cross_instrument + multi_timeframe wired 2026-08-03; todo 2c: commodity
# wired 2026-08-03, features-service@fa18180b -- global bucket, no --asset-group, same shape
# as calendar below). The python side's _UNSUPPORTED_FAMILIES is now empty; this launcher's
# per-family case block must stay in lockstep so a bad launch fails BEFORE a VM boots.
ASSET_GROUP_ABBREV=""
FAMILY_ABBREV=""
case "$FEATURE_FAMILY" in
    delta_one)
        FAMILY_ABBREV="d1"
        case "$ASSET_GROUP" in
            cefi|tradfi|defi|prediction) ;;
            *) echo "ERROR: delta_one requires --asset-group in cefi/tradfi/defi/prediction (got: '$ASSET_GROUP')" >&2; exit 2 ;;
        esac
        ;;
    volatility)
        FAMILY_ABBREV="vol"
        case "$ASSET_GROUP" in
            cefi|tradfi|defi) ;;
            *) echo "ERROR: volatility requires --asset-group in cefi/tradfi/defi (got: '$ASSET_GROUP')" >&2; exit 2 ;;
        esac
        ;;
    cross_instrument)
        FAMILY_ABBREV="xi"
        case "$ASSET_GROUP" in
            cefi|tradfi|defi|prediction) ;;
            *) echo "ERROR: cross_instrument requires --asset-group in cefi/tradfi/defi/prediction (got: '$ASSET_GROUP')" >&2; exit 2 ;;
        esac
        ;;
    multi_timeframe)
        FAMILY_ABBREV="mtf"
        case "$ASSET_GROUP" in
            cefi|tradfi|defi|prediction) ;;
            *) echo "ERROR: multi_timeframe requires --asset-group in cefi/tradfi/defi/prediction (got: '$ASSET_GROUP')" >&2; exit 2 ;;
        esac
        ;;
    onchain)
        FAMILY_ABBREV="oc"
        if [[ -n "$ASSET_GROUP" && "$ASSET_GROUP" != "defi" ]]; then
            echo "ERROR: onchain is fixed to asset_group=defi (got: '$ASSET_GROUP')" >&2; exit 2
        fi
        ASSET_GROUP="defi"
        ;;
    sports)
        FAMILY_ABBREV="spt"
        if [[ -n "$ASSET_GROUP" && "$ASSET_GROUP" != "sports" ]]; then
            echo "ERROR: sports is fixed to asset_group=sports (got: '$ASSET_GROUP')" >&2; exit 2
        fi
        ASSET_GROUP="sports"
        ;;
    calendar)
        FAMILY_ABBREV="cal"
        if [[ -n "$ASSET_GROUP" ]]; then
            echo "ERROR: calendar is global -- omit --asset-group (got: '$ASSET_GROUP')" >&2; exit 2
        fi
        ;;
    commodity)
        FAMILY_ABBREV="cm"
        if [[ -n "$ASSET_GROUP" ]]; then
            echo "ERROR: commodity is global -- omit --asset-group (got: '$ASSET_GROUP')" >&2; exit 2
        fi
        ;;
    *)
        echo "ERROR: --feature-family must be one of delta_one/volatility/onchain/sports/calendar/commodity/cross_instrument/multi_timeframe (got: '$FEATURE_FAMILY')" >&2
        exit 2
        ;;
esac
ASSET_GROUP_ABBREV="${ASSET_GROUP:-gl}"
case "$ASSET_GROUP_ABBREV" in
    prediction) ASSET_GROUP_ABBREV="pred" ;;
    tradfi)     ASSET_GROUP_ABBREV="tfi" ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
# 250GB minimum (not 100GB): GCP PD throughput scales with disk SIZE (~0.28 MB/s per GB),
# so a 100GB disk only sustains ~28 MB/s vs the ~70 MB/s this backfill-VM workload class
# needs -- see plans/active/issues/backfill_vm_disk_starvation_misdiagnosed_as_tardis_quota_2026_07_18.md.
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) -- the
# sweep is read-only (never deletes/patches), so a preemption relaunch is a safe
# restart (checkpoint-resumable per feature_orphan_sweep.py's own checkpoint contract).
# ON_DEMAND=true is the only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

# Singleton check per (family, asset_group) cell (different cells may sweep in parallel).
SINGLETON_PREFIX="feat-orph-${FAMILY_ABBREV}-${ASSET_GROUP_ABBREV}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: feature-orphan-sweep VM already running for ${FEATURE_FAMILY}/${ASSET_GROUP:-global} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force --feature-family ${FEATURE_FAMILY} --asset-group ${ASSET_GROUP}

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction"). If, after that check, it is genuinely stale, construct the
delete command yourself — do not copy-paste one from this message — per
infra.md STEP 0.65's 3-signal staleness check (heartbeat age, run.log tail,
manifest mtime); when unsure, escalate instead of deleting.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

REPORT_OUT="gs://${CODE_BUCKET}/feature-orphan-sweep/${RUN_TS}/${VM_NAME}/orphan_sweep_${FEATURE_FAMILY}_${ASSET_GROUP:-global}.parquet"

# features-service-code.tar.gz is extracted to $WORKSPACE/features/ by
# setup-data-pipeline-vm.sh (VM_SERVICE=features_service), so the sweep lives at:
SCRIPTS="/home/ikennaigboaka/workspace/features/scripts"

# Single `python ` token -> setup-data-pipeline-vm.sh substitutes it to $VENV/bin/python.
BACKFILL_CMD="python ${SCRIPTS}/feature_orphan_sweep.py"
BACKFILL_CMD="${BACKFILL_CMD} --feature-family ${FEATURE_FAMILY}"
if [[ -n "$ASSET_GROUP" && "$FEATURE_FAMILY" != "calendar" && "$FEATURE_FAMILY" != "commodity" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
fi
BACKFILL_CMD="${BACKFILL_CMD} --report-out ${REPORT_OUT}"

METADATA="VM_TASK=feature-orphan-sweep"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=feature-orphan-sweep-${FAMILY_ABBREV}"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "${ASSET_GROUP:-GLOBAL}" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# Stall-progress marker: feature_orphan_sweep.py logs "N feature objects swept (X/s)"
# every 50,000 objects (run_sweep()) -- reset the stall timer only on a genuine
# walk-progress line. "swept" is the distinguishing token, matching the raw-tick/
# candle sweeps' own STALL_PROGRESS_REGEX convention.
METADATA="${METADATA},STALL_PROGRESS_REGEX=swept"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Persist the exact launch scope BEFORE create so a SPOT-preemption relaunch
# (RelaunchPreemptedVm) replays the same cell. Best-effort; never fails the launch.
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-feature-orphan-sweep-vm.sh" \
    FEATURE_FAMILY="$FEATURE_FAMILY" ASSET_GROUP="$ASSET_GROUP" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: GCS->manifest orphan sweep for feature_family=${FEATURE_FAMILY} asset_group=${ASSET_GROUP:-global}"
echo "  Sweep:   feature_orphan_sweep.py (READ-ONLY -- classifies, never deletes/patches)"
echo "  Report:  ${REPORT_OUT}"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service unified-api-contracts unified-trading-library \
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
    --labels=purpose=feature-orphan-sweep,family="${FAMILY_ABBREV}",category="${ASSET_GROUP_ABBREV}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Report:      gcloud storage ls ${REPORT_OUT}"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'feature objects swept' progress lines (this tool"
echo "has NO per-VM manifest shard to count rows against -- it's a single family-prefix walk"
echo "ending in ONE final report write, not incremental shard growth):"
echo "  - STARTED < 60s; require log growth + a new 'objects swept' line periodically;"
echo "    flat => STALL => diagnose; verify the report parquet exists at completion."
