#!/usr/bin/env bash
# Launch a short-lived GCE VM that runs the per-asset-group expected-universe
# enumerator (`instruments-service/scripts/enumerate_expected_universe.py`)
# for a given asset_group, in same-region (asia-northeast1-c) so the manifest
# read + per-VM shard write are fast.
#
# Why this exists: the availability manifest's rollup totals (top-of-page
# percentage in the data-status UI) and per-shard drilldown derive their
# denominators from different code paths. The drilldown enumerates the
# expected universe (calendar pre-skip, chain pre-genesis, source coverage
# windows) and counts every (shard_key, day) tuple. The rollup just reads the
# manifest. Today the manifest only has rows for tuples we *attempted* —
# calendar / chain pre-genesis / pre-coverage cases were silently pre-skipped
# without writing rows. That makes rollup and drilldown disagree on the
# denominator. Phase 3.D.4 of the writegate plan closes the gap by
# back-filling `record_expected_empty(reason=EXPECTED_*)` rows for every
# tuple in the expected universe that has no manifest row.
#
# Default: SCAN-ONLY (read-only — emits a CSV report, no manifest writes).
# Pass `--apply-write` to actually write per-VM shard rows. Per-VM shard
# isolation (CLAUDE.md "Per-VM shard isolation for concurrent backfills")
# is automatically wired in `--apply-write` mode so concurrent writers from
# other VMs do not race on the canonical manifest.
#
# Usage:
#   bash launch-expected-universe-enumerator-vm.sh                          # tradfi --scan-only
#   bash launch-expected-universe-enumerator-vm.sh tradfi                    # tradfi --scan-only
#   bash launch-expected-universe-enumerator-vm.sh tradfi --apply-write      # tradfi, write back
#   bash launch-expected-universe-enumerator-vm.sh defi --apply-write
#   bash launch-expected-universe-enumerator-vm.sh sports
#   bash launch-expected-universe-enumerator-vm.sh cefi                       # stub — instant exit
#   bash launch-expected-universe-enumerator-vm.sh prediction                 # stub — instant exit
#   bash launch-expected-universe-enumerator-vm.sh --force defi               # bypass singleton
#
# Cost: e2-standard-4 + 50GB. tradfi/defi/sports ~5–30 min depending on
# expected-universe cardinality; cefi/prediction stubs exit in seconds.
#
# Singleton lock: refuses to launch if another expected-universe-enum-* VM
# is RUNNING in the zone. The enumerator reads the canonical manifest once
# at startup; concurrent runs across asset_groups are safe in principle
# (they write to different per-VM shards) but the singleton lock matches
# the prefix only — pass --force to launch a second asset_group while the
# first is still running. In practice run them sequentially.
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - CSV report       → /tmp/enum-universe-{asset_group}-{ts}.csv on the VM
#   - Per-VM manifest shard (only with --apply-write):
#       gs://market-data-tick-{asset_group}-{pid}/_index/per_vm/{vm_name}.parquet
#       (consolidator daemon merges into _index/availability_index.parquet
#        within ~5 min)
#   - Auto-shutdown when the script exits (VM_SHUTDOWN_ON_COMPLETION=true)
set -euo pipefail

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
    FORCE=true
    shift
fi

ASSET_GROUP="${1:-tradfi}"
APPLY_FLAG="${2:---scan-only}"

# Validate asset_group + flag.
case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$APPLY_FLAG" in
    --scan-only|--apply-write) ;;
    *) echo "ERROR: second arg must be --scan-only (default) or --apply-write (got: $APPLY_FLAG)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^expected-universe-enum-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: expected-universe-enum VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate without --force. Concurrent enumerator
runs across asset_groups write to different per-VM shards and so are
safe in principle, but in practice we run them sequentially so the
operator can inspect each scan-only CSV before the apply-write.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ${ASSET_GROUP} ${APPLY_FLAG}
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="expected-universe-enum-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: expected-universe enumerator for asset_group=${ASSET_GROUP} (${APPLY_FLAG})"

# Map our launcher flag to the script's flag: the launcher takes
# --scan-only (default) / --apply-write to mirror phantom-recon's UX, but
# the underlying script understands a presence-flag --apply-write only
# (default = scan-only).
SCRIPT_FLAG=""
if [[ "$APPLY_FLAG" == "--apply-write" ]]; then
    SCRIPT_FLAG="--apply-write"
fi

# Route via VM_TASK=expected-universe-enum: see setup-data-pipeline-vm.sh
# elif branch alongside mdps-backfill / features-backfill / phantom-recon
# — same generic VM_BACKFILL_CMD pull-and-exec shape. setup-data-pipeline-vm.sh
# extracts instruments-service-code tarball to $WORKSPACE/instruments
# (not /instruments-service) per the TARBALL_DIRS alias in that script.
# $WORKSPACE = /home/ikennaigboaka/workspace.
ENUM_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/enumerate_expected_universe.py"
BACKFILL_CMD="python ${ENUM_SCRIPT} --asset-group ${ASSET_GROUP}"
if [[ -n "$SCRIPT_FLAG" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} ${SCRIPT_FLAG}"
fi

METADATA="VM_TASK=expected-universe-enum"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=expected-universe-enum"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Per-VM shard isolation guards (CLAUDE.md "Per-VM shard isolation for
# concurrent backfills"). The enumerator script also runtime-guards:
# --apply-write without these env vars exits 4 with
# ENUMERATOR_FAILED reason=missing_per_vm_shards_env / missing_vm_name_env.
# Pass them in scan-only too so the env shape is identical and the script's
# env-validation path is exercised on every run.
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=expected-universe-enum,asset-group="${ASSET_GROUP}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/expected-universe-enum.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
