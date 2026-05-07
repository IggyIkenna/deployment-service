#!/usr/bin/env bash
# Launch a short-lived GCE VM that runs the multi-asset-group phantom audit
# (`instruments-service/scripts/reconcile_phantom_manifest_rows_all.py`) for a
# given asset_group, in same-region (asia-northeast1-c) so the GCS listing
# walk is fast (CLAUDE.md "Manifest phantom audit" — cross-region listing is
# 18× slower).
#
# Why this exists: phantom rows accumulate in the availability manifest when
# adapters record `captured` for a shard but the parquet doesn't exist at the
# canonical GCS path (stale rescan output, schema migration churn, broken
# denorm). The orchestrator's `_should_skip_shard` trusts the manifest, so
# phantoms cause permanent skip — every backfill VM exits doing nothing for
# the affected shards. Periodic recon catches this.
#
# Default: DRY-RUN only (read-only, no manifest writes). Pass --apply to
# actually flip phantom rows to attempted_failed (see script docstring).
#
# Usage:
#   bash launch-defi-phantom-recon-vm.sh                          # defi --dry-run
#   bash launch-defi-phantom-recon-vm.sh defi --apply             # defi, write back
#   bash launch-defi-phantom-recon-vm.sh cefi                     # cefi --dry-run
#   bash launch-defi-phantom-recon-vm.sh sports                   # sports --dry-run
#   bash launch-defi-phantom-recon-vm.sh --force defi             # bypass singleton
#
# Cost: e2-standard-4 + 50GB. ~13 min for full DEFI per CLAUDE.md
# (~222 prefixes/sec on same-region GCE). Larger asset_groups (CEFI ~313k
# rows) take 30-60 min.
#
# Singleton lock: refuses to launch if another defi-phantom-recon-* VM is
# RUNNING in the zone. The audit script reads the canonical manifest +
# bulk-lists GCS prefixes; concurrent runs duplicate the GCS list-blob
# budget without speedup. --force bypasses for legitimate parallel runs
# across asset_groups (the singleton check matches the prefix not the
# asset_group, so launching defi + cefi back-to-back needs --force on the
# second; in practice run them sequentially).
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - Auto-shutdown when the script exits (VM_SHUTDOWN_ON_COMPLETION=true)
set -euo pipefail

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
    FORCE=true
    shift
fi

ASSET_GROUP="${1:-defi}"
APPLY_FLAG="${2:---dry-run}"

# Validate asset_group + flag.
case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$APPLY_FLAG" in
    --dry-run|--apply) ;;
    *) echo "ERROR: second arg must be --dry-run (default) or --apply (got: $APPLY_FLAG)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^defi-phantom-recon-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: phantom-recon VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — concurrent GCS list-blob budget shared,
no speedup. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ${ASSET_GROUP} ${APPLY_FLAG}
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="defi-phantom-recon-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: phantom recon for asset_group=${ASSET_GROUP} (${APPLY_FLAG})"

# Route via VM_TASK=phantom-recon (added to setup-data-pipeline-vm.sh
# 2026-05-07): the setup script pulls the instruments-service tarball
# (because VM_SERVICE=instruments_service) + sets up the venv, then runs
# VM_BACKFILL_CMD verbatim with `python` rewritten to `$VENV/bin/python`.
# setup-data-pipeline-vm.sh extracts instruments-service-code tarball to
# $WORKSPACE/instruments (not /instruments-service) per the TARBALL_DIRS
# alias in that script. $WORKSPACE = /home/ikennaigboaka/workspace.
RECON_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/reconcile_phantom_manifest_rows_all.py"
BACKFILL_CMD="python ${RECON_SCRIPT} --asset-group ${ASSET_GROUP} ${APPLY_FLAG}"

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=phantom-recon"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=defi-phantom-recon,asset-group="${ASSET_GROUP}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/phantom-recon.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
