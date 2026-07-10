#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that runs the per-instrument-grain v2 expected-
# universe enumerator (`instruments-service/scripts/enumerate_expected_universe.py
# --enumerator-version v2`) for a given asset_group.
#
# v2 differs from v1 in that it cross-joins the instruments-service catalog
# (per-instrument lifecycle dates) with the date axis, emitting one manifest
# row per (instrument_id, date, data_type) triple where the instrument is NOT
# alive on that date. This enables honest per-instrument drilldown denominators
# in the data-status UI.
#
# Gate G3 of manifest_evolution_SUPERSEDED_2026_05_21. Plan Phase 2.A.
#
# Usage:
#   bash launch-expected-universe-v2-vm.sh <asset_group>
#   bash launch-expected-universe-v2-vm.sh <asset_group> --apply-write
#   bash launch-expected-universe-v2-vm.sh <asset_group> --apply-write <max_writes>
#   bash launch-expected-universe-v2-vm.sh <asset_group> <catalog_gs_path>
#   bash launch-expected-universe-v2-vm.sh <asset_group> --apply-write <max_writes> <catalog_gs_path>
#   bash launch-expected-universe-v2-vm.sh --force <asset_group> --apply-write
#   bash launch-expected-universe-v2-vm.sh --env staging <asset_group>
#
# Positional args (after optional --force / --env):
#   1. asset_group: cefi | defi | tradfi | prediction | sports (required)
#   2. mode flag:   --scan-only (default) | --apply-write (optional)
#   3. max writes:  integer (optional; only valid with --apply-write)
#   4. catalog_path: gs:// URI or local path to instruments-service catalog parquet
#                   Defaults to the canonical instruments-service catalog GCS path
#                   for the given asset_group + env tier.
#
# Singleton lock: refuses to launch if another expected-universe-v2-* VM is
# RUNNING in the zone. Pass --force to bypass.
#
# Env overrides:
#   ON_DEMAND=true   opt out of the SPOT default (backfill/idempotent VMs → SPOT per HARD RULE)
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true are always
# passed so the enumerator's runtime guards are exercised on every run.
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - CSV report       → temp dir on VM (uploaded to GCS at run end)
#   - Per-VM manifest shard (only with --apply-write):
#       gs://market-data-tick-{asset_group}-{pid}/_index/per_vm/{vm_name}.parquet
#
# Execution ownership (Runbook SSOT):
#   execution:
#     owner: Cloud Scheduler (recurring) — this VM launcher is the manual/backfill fallback
#     cadence: RECURRING daily 01:30 UTC via Cloud Scheduler (expected-universe-v2-<ag>-daily,
#              terraform/gcp/expected_universe_v2_scheduler.tf, tofu apply 2026-06-19) —
#              the per-AG Cloud Run Job runs the enumerator --apply-write on a bounded window.
#              This VM launcher remains for ad-hoc one-shot / full-history backfills only.
#     verifier: gcloud scheduler jobs list --location=asia-northeast1 | grep expected-universe-v2 (ENABLED)
#               + gcloud run jobs executions list --job expected-universe-v2-<ag> (Succeeded)
#               + ENUMERATOR_COMPLETED event in gs://{pid}-events/.../
#     last_executed: 2026-06-19 (scheduler+jobs created prod via tofu apply; first scheduled
#                    materialisation fires 2026-06-20 01:30 UTC)
#
# VM naming: expected-universe-v2-{asset_group}-{timestamp}
# Watchdog: registered under prefix "expected-universe-v2-" (None bucket — per-VM shards)
#
# Bucket-naming SSOT: env-aware shape per
# `bucket_name_ssot_canonicalisation_2026_05_10.md`. DEPLOYMENT_ENV propagated
# to VM metadata.

set -euo pipefail

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse named flags before positional args.
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) break ;;
    esac
done

ASSET_GROUP="${1:-}"
if [[ -z "$ASSET_GROUP" ]]; then
    echo "ERROR: asset_group is required (cefi | defi | tradfi | prediction | sports)" >&2
    exit 2
fi
shift

APPLY_FLAG="${1:---scan-only}"
MAX_WRITES=""
CATALOG_PATH_OVERRIDE=""

# Optional: parse remaining positional/flag args
if [[ $# -gt 0 ]]; then
    case "$1" in
        --scan-only|--apply-write)
            APPLY_FLAG="$1"; shift
            ;;
    esac
fi
if [[ $# -gt 0 ]]; then
    if [[ "$1" =~ ^[0-9]+$ ]]; then
        MAX_WRITES="$1"; shift
    fi
fi
if [[ $# -gt 0 ]]; then
    CATALOG_PATH_OVERRIDE="$1"; shift
fi

# Validate.
case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$APPLY_FLAG" in
    --scan-only|--apply-write) ;;
    *) echo "ERROR: mode flag must be --scan-only (default) or --apply-write (got: $APPLY_FLAG)" >&2; exit 2 ;;
esac
case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac
case "$DEPLOYMENT_ENV" in
    prod)    DEPLOYMENT_ENV_SHORT="prd" ;;
    staging) DEPLOYMENT_ENV_SHORT="staging" ;;
    dev)     DEPLOYMENT_ENV_SHORT="dev" ;;
esac
# instruments-store uses "pred" shortform for prediction asset group.
case "$ASSET_GROUP" in
    prediction) CATALOG_AG_SHORT="pred" ;;
    *)          CATALOG_AG_SHORT="$ASSET_GROUP" ;;
esac
if [[ -n "$MAX_WRITES" ]]; then
    if ! [[ "$MAX_WRITES" =~ ^[0-9]+$ ]]; then
        echo "ERROR: max_writes must be a positive integer (got: $MAX_WRITES)" >&2; exit 2
    fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) — the
# enumerator writes to a per-VM manifest shard (MANIFEST_PER_VM_SHARDS=true, always
# on), so preemption just resumes on restart without double-counting.
# ON_DEMAND=true is the only opt-out (matches the fleet backfill convention).
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

# Singleton lock: only one expected-universe-v2-* per zone at a time.
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^expected-universe-v2-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: expected-universe-v2 VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate without --force. The v2 enumerator reads the
instruments-service catalog and the canonical manifest once at startup; concurrent
runs for different asset_groups write to different per-VM shards and are safe in
principle, but we run them sequentially so the operator can inspect each scan-only
CSV first.

Options:
  Inspect:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Stop:     gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:    bash $0 --force ${ASSET_GROUP} ${APPLY_FLAG}
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="expected-universe-v2-${ASSET_GROUP}-${RUN_TS}"

# Default catalog path: canonical instruments-service catalog in env-aware bucket.
# Shape: gs://instruments-store-{asset_group}-{project}/{env}/catalog.parquet
# Override with CATALOG_PATH_OVERRIDE arg or $CATALOG_PATH env var.
if [[ -n "$CATALOG_PATH_OVERRIDE" ]]; then
    CATALOG_PATH="$CATALOG_PATH_OVERRIDE"
elif [[ -n "${CATALOG_PATH:-}" ]]; then
    CATALOG_PATH="${CATALOG_PATH}"
else
    # Canonical default: instruments-service catalog GCS path per env tier.
    CATALOG_BUCKET="instruments-store-${CATALOG_AG_SHORT}-${DEPLOYMENT_ENV_SHORT}-${PROJECT}"
    CATALOG_PATH="gs://${CATALOG_BUCKET}/${DEPLOYMENT_ENV}/catalog.parquet"
fi

echo "Launching $VM_NAME: v2 expected-universe enumerator"
echo "  asset_group: $ASSET_GROUP"
echo "  mode:        $APPLY_FLAG"
echo "  catalog:     $CATALOG_PATH"
if [[ -n "$MAX_WRITES" ]]; then
    echo "  max_writes:  $MAX_WRITES"
fi

# Build the Python command.
ENUM_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/enumerate_expected_universe.py"
BACKFILL_CMD="python ${ENUM_SCRIPT}"
BACKFILL_CMD="${BACKFILL_CMD} --asset-group ${ASSET_GROUP}"
BACKFILL_CMD="${BACKFILL_CMD} --enumerator-version v2"
BACKFILL_CMD="${BACKFILL_CMD} --catalog-path ${CATALOG_PATH}"
if [[ "$APPLY_FLAG" == "--apply-write" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --apply-write"
fi
if [[ -n "$MAX_WRITES" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --max-writes-per-run ${MAX_WRITES}"
fi

METADATA="VM_TASK=expected-universe-v2"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=expected-universe-v2"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# Per-VM shard isolation: always pass these even in scan-only mode so the
# runtime guards are exercised.
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      "${PROVISIONING_ARGS[@]}" \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=expected-universe-v2,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:    gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/expected-universe-v2.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:  gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify event stream (required — no fire-and-forget):"
echo "  STARTED within 60s, >= 1 progress event/hr, STOPPED/FAILED on exit."
