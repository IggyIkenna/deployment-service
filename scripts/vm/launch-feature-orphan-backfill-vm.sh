#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed for every (feature_family x asset_group) cell
#   feature_orphan_sweep.py found real orphans in (currently onchain/defi + sports/sports)
#
# Launch a short-lived SPOT GCE VM that runs the features-corpus class-E orphan ->
# record_captured backfill (features-service/scripts/backfill_feature_orphan_class_e.py
# --feature-family <family> [--asset-group <ag>] --report-uri <uri> --apply) for ONE
# (feature_family x asset_group) cell, reading the durable
# orphan_sweep_{family}_{ag}.parquet report feature_orphan_sweep.py already wrote.
#
# Built for: unified-trading-pm/plans/active/issues/
# features_service_manifest_coverage_gap_2026_08_03.md todo 2 -- sports/sports's 67,077
# real orphan objects (13.1GB total, all footer-only reads) need this backfill; the
# volume is well past the in-session bound (STEP 0.56 memory-bounding guardrail), so it
# must run on a dedicated VM, not the shared planning host. Sibling of
# launch-feature-orphan-sweep-vm.sh (read-only classify) and
# launch-backfill-orphan-e-vm.sh (the instruments-service raw-tick analogue this mirrors).
#
# READ + WRITE, NOT read-only (unlike the sweep): --apply calls record_captured
# (ManifestWriter.add(), per-VM shard) for every still-orphan cell with row_count>0.
# NEVER deletes or mutates the source object (record-only by construction -- see that
# script's own module docstring: features orphans are never re-shaped/converted).
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3 (same Tier-2
# SPOT/singleton-lock/tarball-freshness VM shape as the sibling launchers above).
#
# Run same-region (asia-northeast1-c) so the corpus GCS listing + manifest join are fast.
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): idempotent re-run
# (reverify_against_index() drops any cell the LIVE index already covers, so a
# preemption relaunch just re-verifies + skips already-recorded cells -- safe, simple
# restart-from-scratch behavior, same reasoning as launch-backfill-orphan-e-vm.sh).
# ON_DEMAND=true opts out.
#
# Machine sizing: e2-standard-4 default (unlike the instruments-service raw-tick
# backfill's e2-highmem-8) -- this tool is RECORD-ONLY, never retains a full converted
# DataFrame per object (backfill_feature_orphan_class_e.py's CellRecord holds only
# scalars + URI strings), so its footprint is far smaller even at sports' 67k-object
# scale. Bump via MACHINE_TYPE=... if a real run OOMs.
#
# The VM prefix MUST match the registered feat-orph-bf-{family_abbrev}-{ag_abbrev}-
# entry in deployment_service/vm_prefix_registry.py (bucket=None -- heartbeat-only,
# fixed per-cell report path under the family's own bucket, not a per-VM manifest
# shard path) + its launcher_registry.py twin.
#
# Singleton lock is per (family, asset_group) cell (prefix
# feat-orph-bf-{family_abbrev}-{ag_abbrev}-), mirroring the sweep launcher.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Backfill report (only written when --apply + recorded>0, per
#     backfill_feature_orphan_class_e.py::write_report):
#       gs://{family bucket}/_index/audit/feature_orphan_backfill_{family}_{ag_or_global}.parquet
#
# Usage:
#   bash launch-feature-orphan-backfill-vm.sh --feature-family sports \
#       --report-uri gs://deployment-scripts-central-element-323112/feature-orphan-sweep/20260803-104314/feat-orph-spt-sports-20260803-104314/orphan_sweep_sports_sports.parquet
#   bash launch-feature-orphan-backfill-vm.sh --feature-family onchain --asset-group defi \
#       --report-uri gs://.../orphan_sweep_onchain_defi.parquet --dry-run
#   bash launch-feature-orphan-backfill-vm.sh --force --feature-family sports --report-uri gs://...
#   ON_DEMAND=true bash launch-feature-orphan-backfill-vm.sh --feature-family sports --report-uri gs://...
#
# Cost: e2-standard-4 SPOT + 250GB. Single pass, apply mode by default.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN_MODE=false
FEATURE_FAMILY=""
ASSET_GROUP=""
REPORT_URI=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN_MODE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --feature-family) FEATURE_FAMILY="$2"; shift 2 ;;
        --asset-group) ASSET_GROUP="$2"; shift 2 ;;
        --report-uri) REPORT_URI="$2"; shift 2 ;;
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
if [[ -z "$REPORT_URI" ]]; then
    echo "ERROR: --report-uri is required (the feature_orphan_sweep.py --report-out parquet)" >&2
    exit 2
fi

# Mirrors launch-feature-orphan-sweep-vm.sh's own --feature-family validation +
# family_abbrev scheme (same abbreviations, so the two launchers' VM names read
# consistently for the same family). commodity is excluded -- feature_orphan_sweep.py
# itself refuses that family (no verified bucket/prefix config).
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
    *)
        echo "ERROR: --feature-family must be one of delta_one/volatility/onchain/sports/calendar/cross_instrument/multi_timeframe (got: '$FEATURE_FAMILY')" >&2
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

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="feat-orph-bf-${FAMILY_ABBREV}-${ASSET_GROUP_ABBREV}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: feature-orphan-backfill VM already running for ${FEATURE_FAMILY}/${ASSET_GROUP:-global} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force --feature-family ${FEATURE_FAMILY} --asset-group ${ASSET_GROUP} --report-uri ${REPORT_URI}

CAUTION -- do NOT delete $EXISTING unless you have confirmed via Inspect above that it
is genuinely stale. It may be another dispatch's actively progressing VM.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

SCRIPTS="/home/ikennaigboaka/workspace/features/scripts"

BACKFILL_CMD="python ${SCRIPTS}/backfill_feature_orphan_class_e.py"
BACKFILL_CMD="${BACKFILL_CMD} --feature-family ${FEATURE_FAMILY}"
if [[ -n "$ASSET_GROUP" && "$FEATURE_FAMILY" != "calendar" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
fi
BACKFILL_CMD="${BACKFILL_CMD} --report-uri ${REPORT_URI}"
if $DRY_RUN_MODE; then
    BACKFILL_CMD="${BACKFILL_CMD} --dry-run"
else
    BACKFILL_CMD="${BACKFILL_CMD} --apply"
fi

METADATA="VM_TASK=feature-orphan-backfill"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=feature-orphan-backfill-${FAMILY_ABBREV}"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "${ASSET_GROUP:-GLOBAL}" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# backfill_feature_orphan_class_e.py logs "footer-read N/M cells" every 200 objects
# (main()) -- "footer-read" is unique among this tool's recurring log lines, matching
# the launcher convention of keying the stall timer on a token that repeats throughout
# the run, not just once at the top.
METADATA="${METADATA},STALL_PROGRESS_REGEX=footer-read"
METADATA="${METADATA},STALL_TIMEOUT_SEC=3600"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-feature-orphan-backfill-vm.sh" \
    FEATURE_FAMILY="$FEATURE_FAMILY" ASSET_GROUP="$ASSET_GROUP" REPORT_URI="$REPORT_URI" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: features-corpus class-E orphan record_captured backfill for feature_family=${FEATURE_FAMILY} asset_group=${ASSET_GROUP:-global}"
echo "  Backfill: backfill_feature_orphan_class_e.py --feature-family ${FEATURE_FAMILY} $([ -n "$ASSET_GROUP" ] && [ "$FEATURE_FAMILY" != "calendar" ] && echo "--asset-group ${ASSET_GROUP} ")--report-uri ${REPORT_URI} $([ "$DRY_RUN_MODE" == "true" ] && echo "--dry-run" || echo "--apply")"

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
    --labels=purpose=feature-orphan-backfill,family="${FAMILY_ABBREV}",category="${ASSET_GROUP_ABBREV}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + the periodic 'footer-read N/M cells' progress lines; verify"
echo "the VERDICT line + record_captured counts landing in the manifest at completion."
