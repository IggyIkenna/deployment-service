#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11.
#
# Launch a short-lived SPOT GCE VM that runs the Tier-2 per-datapoint validator
# (validate_datapoint_schema_id.py) for ONE asset_group — the ONE sanctioned
# single-walk of the reconciliation two-tier compute model. It enumerates the
# market-data corpus for the asset_group EXACTLY ONCE and, per parquet, runs
# G2 (id-canonical) + G3 (schema) bundled onto that one snapshot walk, writing
# ONE results row per validated object to the flat datapoint-validation results
# bucket as a per-VM shard (_index/per_vm/{VM_NAME}.parquet).
#
# SSOT: codex/02-data/reconciliation-census-and-compute-tiers.md § 3.
#
# Run same-region (asia-northeast1-c) so the corpus GCS reads are fast
# (cross-region listing is ~18× slower).
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): the job is
# presence-skip idempotent (a shard whose results row already exists is skipped),
# so a preemption RelaunchPreemptedVm resumes from measured progress — NOT a
# --force replay-from-start, so the force-PAGE guard never fires. ON_DEMAND=true
# is the only opt-out.
#
# The VM prefix MUST match the registered datapoint-validation-{ag}- entry in
# deployment_service/vm_prefix_registry.py (VmPrefixSpec → the datapoint-validation
# results bucket) + its launcher_registry.py twin — an unregistered name is
# silently invisible in deployment-ui/cockpit/Slack.
#
# Singleton lock is per-asset-group (prefix datapoint-validation-{asset_group}-)
# so different asset_groups can validate in parallel safely.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Preemption-resume checkpoint: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/PROGRESS.json
#     (emitted by the tee-wrapper from the validator's [[VM_PROGRESS]] day-frontier markers)
#   - Results shard: gs://{pid}-datapoint-validation/_index/per_vm/{vm_name}.parquet
#
# Usage:
#   bash launch-datapoint-validation-vm.sh cefi
#   bash launch-datapoint-validation-vm.sh defi
#   bash launch-datapoint-validation-vm.sh tradfi
#   bash launch-datapoint-validation-vm.sh sports
#   bash launch-datapoint-validation-vm.sh prediction
#   bash launch-datapoint-validation-vm.sh --force defi          # bypass singleton lock
#   bash launch-datapoint-validation-vm.sh --env staging defi
#   CAMPAIGN_ID=20260721-101500 bash launch-datapoint-validation-vm.sh defi  # reuse a campaign
#   ON_DEMAND=true bash launch-datapoint-validation-vm.sh defi    # opt out of SPOT
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
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-100}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) — the
# validator is presence-skip idempotent (already-validated objects skip), so a
# standard preemption relaunch resumes. ON_DEMAND=true is the only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

# Singleton check per-asset-group (different asset_groups may run in parallel).
SINGLETON_PREFIX="datapoint-validation-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: datapoint-validation VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"
# Reuse an existing campaign (CAMPAIGN_ID env) to avoid a redundant walk (§ 3.4);
# else this launch opens a fresh campaign stamped with the run timestamp.
CAMPAIGN_ID="${CAMPAIGN_ID:-${RUN_TS}}"

# instruments-service-code.tar.gz is extracted to $WORKSPACE/instruments/ by
# setup-data-pipeline-vm.sh (VM_SERVICE=instruments_service), so the validator lives at:
SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"

# Single `python ` token → setup-data-pipeline-vm.sh substitutes it to $VENV/bin/python.
BACKFILL_CMD="python ${SCRIPTS}/validate_datapoint_schema_id.py"
BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
BACKFILL_CMD="${BACKFILL_CMD} --campaign-id ${CAMPAIGN_ID}"
BACKFILL_CMD="${BACKFILL_CMD} --vm-name ${VM_NAME}"
BACKFILL_CMD="${BACKFILL_CMD} --env ${DEPLOYMENT_ENV}"

METADATA="VM_TASK=datapoint-validation"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=datapoint-validation"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
# Per-VM results shards (_index/per_vm/{VM_NAME}.parquet) so the fleet monitor
# keys on results-row write-progress (§ 3.2, the 2026-07-18 entity-agnostic blind spot).
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},CAMPAIGN_ID=${CAMPAIGN_ID}"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Persist the exact launch scope BEFORE create so a SPOT-preemption relaunch
# (RelaunchPreemptedVm) replays the same (asset_group, campaign) — presence-skip
# then resumes from the frontier (§ 3.2). Best-effort; never fails the launch.
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-datapoint-validation-vm.sh" \
    ASSET_GROUP="$ASSET_GROUP" CAMPAIGN_ID="$CAMPAIGN_ID" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: Tier-2 datapoint validation (id + schema) for asset_group=${ASSET_GROUP}"
echo "  Campaign:   ${CAMPAIGN_ID}"
echo "  Validator:  validate_datapoint_schema_id.py (ONE sanctioned single-walk)"
echo "  Results:    gs://${PROJECT}-datapoint-validation/_index/per_vm/${VM_NAME}.parquet"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
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
    --labels=purpose=datapoint-validation,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Progress:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/PROGRESS.json"
echo "Results:     gcloud storage ls gs://${PROJECT}-datapoint-validation/_index/per_vm/${VM_NAME}.parquet"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (§ 3.2 / async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (≤30-min, kill -0 liveness, no self-match) keyed on"
echo "the results-shard row count (entity-scoped to (asset_group, campaign_id) on time_created):"
echo "  - STARTED < 60s; require ≥1 progress/hr; flat ⇒ STALL ⇒ diagnose; verify T+10min."
