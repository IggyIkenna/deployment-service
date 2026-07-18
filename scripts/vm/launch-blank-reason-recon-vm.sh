#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Launch a short-lived GCE VM that runs the blank-error_reason migration
# (`instruments-service/scripts/reconcile_blank_error_reason_rows.py`) for a
# given asset_group, in same-region (asia-northeast1-c) so the manifest
# read + per-VM shard write are fast.
#
# Why this exists: parallel-agent finding 2026-05-07 — 5 CeFi VMs wrote
# 96-100% empty rows with all blank error_reason (bitfinex 2020/2024 spot,
# bitget futures 2025, kraken 2020/2023 spot). The blank reason violates
# the writegate Phase 2.E taxonomy AND silently masks real fetch failures.
# Migration script reclassifies blank-reason rows per the asset-group-
# specific empty_confirmed legitimacy rule (operator directive 2026-05-07
# msg 6):
#   * sports / prediction → keep empty_confirmed with classified reason
#   * cefi / defi / tradfi → flip to attempted_failed (force re-attempt)
#     unless venue-level EXPECTED_* rule fires
#
# Default: SCAN-ONLY (CSV report). Pass `--apply-flips` to actually mutate
# the manifest. Per-VM shard isolation guards apply automatically per the
# launcher metadata.
#
# Usage:
#   bash launch-blank-reason-recon-vm.sh                          # cefi --scan-only (default)
#   bash launch-blank-reason-recon-vm.sh cefi                     # cefi --scan-only
#   bash launch-blank-reason-recon-vm.sh cefi --apply-flips       # cefi, write back
#   bash launch-blank-reason-recon-vm.sh defi --apply-flips
#   bash launch-blank-reason-recon-vm.sh sports
#   bash launch-blank-reason-recon-vm.sh tradfi --apply-flips
#   bash launch-blank-reason-recon-vm.sh prediction
#   bash launch-blank-reason-recon-vm.sh --force defi             # bypass singleton
#
# Cost: e2-standard-4 + 50GB. CeFi (~2.4M rows) classifier walk ~30-90s.
#
# Singleton lock: refuses to launch if another blank-reason-recon-* VM is
# RUNNING in the zone.
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - CSV report       → /tmp/recon-blank-{asset_group}-{ts}.csv on the VM
#   - Per-VM manifest shard (only with --apply-flips):
#       gs://market-data-tick-{asset_group}-{pid}/_index/per_vm/{vm_name}.parquet
#       (consolidator daemon merges into _index/availability_index.parquet
#        within ~5 min)
#   - Auto-shutdown when the script exits (VM_SHUTDOWN_ON_COMPLETION=true)
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse named flags (--force / --dry-run / --env <val>) in any order before positional args.
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) break ;;
    esac
done

ASSET_GROUP="${1:-cefi}"
APPLY_FLAG="${2:---scan-only}"
MAX_FLIPS="${3:-}"  # optional integer override for --max-flips-per-run (default in script: 100000)

case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$APPLY_FLAG" in
    --scan-only|--apply-flips) ;;
    *) echo "ERROR: second arg must be --scan-only (default) or --apply-flips (got: $APPLY_FLAG)" >&2; exit 2 ;;
esac
case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac
if [[ -n "$MAX_FLIPS" ]]; then
    if ! [[ "$MAX_FLIPS" =~ ^[0-9]+$ ]]; then
        echo "ERROR: third arg (max-flips-per-run) must be a positive integer (got: $MAX_FLIPS)" >&2
        exit 2
    fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^blank-reason-recon-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: blank-reason-recon VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate without --force.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP} ${APPLY_FLAG}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
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

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="blank-reason-recon-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: blank-reason migration for asset_group=${ASSET_GROUP} (${APPLY_FLAG})"

# Map launcher flag to script flag.
SCRIPT_FLAG=""
if [[ "$APPLY_FLAG" == "--apply-flips" ]]; then
    SCRIPT_FLAG="--apply-flips"
fi

# Route via VM_TASK=phantom-recon (the generic BACKFILL_CMD route — supports
# any one-off-script launcher per the setup-data-pipeline-vm.sh contract).
RECON_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/reconcile_blank_error_reason_rows.py"
BACKFILL_CMD="python ${RECON_SCRIPT} --asset-group ${ASSET_GROUP}"
if [[ -n "$SCRIPT_FLAG" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} ${SCRIPT_FLAG}"
fi
if [[ -n "$MAX_FLIPS" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --max-flips-per-run ${MAX_FLIPS}"
    echo "Using --max-flips-per-run=${MAX_FLIPS} (overrides script default 100000)"
fi

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=blank-reason-recon"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Per-VM shard isolation guards (CLAUDE.md "Per-VM shard isolation for
# concurrent backfills"). Required for --apply-flips writes.
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=blank-reason-recon,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
