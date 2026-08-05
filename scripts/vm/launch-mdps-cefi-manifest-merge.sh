#!/usr/bin/env bash
# Epic: cefi_satellite_ao_dispatch_batch5
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed for cefi (manifest row count + r20251225's 7 rows verified)
#
# Launch a short-lived SPOT GCE VM that runs the additive MDPS manifest
# reconciliation for the cefi trades gap-fill campaign.
#
# Runs merge_manifest_from_canonical_paths (unified-trading-library,
# manifest_writer/_maintenance.py) with:
#   bucket="market-data-tick-cefi-prd-central-element-323112"
#   service_name="market-data-processing-service"
#   prefix="processed_candles/by_date"
#
# Registers per-VM manifest shards that never flushed during the 15-VM campaign
# (notably r20251225's final 7 rows — the underlying parquet data is confirmed
# present on GCS, only its manifest registration is missing).
#
# ADDITIVE ONLY — computes discovered - existing, uploads existing + new_only.
# Every row outside prefix (co-located MTDS raw_tick_data) survives untouched.
# Re-running is a no-op. Two live regression tests cover both properties.
#
# SSOT: unified-trading-pm/plans/active/cefi_satellite_ao_dispatch_batch5_2026_08_02.md todo 3
#
# Run same-region (asia-northeast1-c) so GCS manifest reads are fast.
#
# SPOT by default (idempotent — safe to re-run on preemption).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#
# Usage:
#   bash launch-mdps-cefi-manifest-merge.sh
#   bash launch-mdps-cefi-manifest-merge.sh --dry-run
#   bash launch-mdps-cefi-manifest-merge.sh --force
#
# Cost: e2-standard-4 SPOT + 50GB. ~30-90 min (full corpus GCS walk under prefix).
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

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="mdps-cefi-manifest-merge-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: mdps-cefi-manifest-merge VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect above that it
is genuinely stale. It may be another dispatch's actively progressing VM.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

# UTL scripts are deployed to workspace/utl/scripts/ on the VM
# (TARBALL_DIRS["unified-trading-library-code"]="utl")
SCRIPTS="/home/ikennaigboaka/workspace/utl/scripts"

BACKFILL_CMD="python ${SCRIPTS}/merge_mdps_cefi_manifest.py"

METADATA="VM_TASK=mdps-cefi-manifest-merge"
METADATA="${METADATA},VM_SERVICE=market_data_processing_service"
METADATA="${METADATA},VM_OPERATION=mdps-cefi-manifest-merge"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# merge_manifest_from_canonical_paths logs "merge_manifest_from_canonical_paths: wrote"
# on success; the GCS listing under prefix can take a while with no stdout, so use a
# generous stall timeout.
METADATA="${METADATA},STALL_PROGRESS_REGEX=merge_manifest_from_canonical_paths"
METADATA="${METADATA},STALL_TIMEOUT_SEC=7200"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-mdps-cefi-manifest-merge.sh" \
    DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: additive MDPS manifest merge for cefi processed_candles/by_date"
echo "  Script:  merge_mdps_cefi_manifest.py"
echo "  Bucket:  market-data-tick-cefi-prd-central-element-323112"
echo "  Prefix:  processed_candles/by_date"
echo "  Mode:    $([ "$DRY_RUN_MODE" == "true" ] && echo "DRY RUN" || echo "APPLY")"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        unified-trading-library unified-api-contracts \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
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
    --labels=purpose=mdps-cefi-manifest-merge,asset-group=cefi,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (<=30-min, kill -0 liveness, no self-match)"
echo "keyed on run.log mtime + exit_code. Verify before/after manifest row count +"
echo "r20251225's 7 rows confirmed registered before calling /done."
