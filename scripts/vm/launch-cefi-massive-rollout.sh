#!/usr/bin/env bash
# Massive CeFi backfill launcher — 364 week-VMs (one per ISO week x 7 years)
# OR 364 day-VMs on a single day for concurrency probing. Inherits the
# 100-VM-per-batch rate-limit pattern from the legacy unified-trading-deployment-v2
# fan-out (commit history: deployment-ui v1 used to throttle to 100/batch
# because GCE quota exhaustion in asia-northeast1 starts triggering at
# ~150 concurrent inserts).
#
# Probe usage (concurrency test — 364 VMs all on same day, skip-existing
# means most are no-ops if data is already there):
#   bash launch-cefi-massive-rollout.sh probe 2026-04-22
#
# Real backfill usage (364 week-VMs across 7 years, each handles 1 ISO week):
#   bash launch-cefi-massive-rollout.sh full 2020-01-01 2026-12-31
#
# What it does:
#   1. Computes the shard list (week-ranges in 'full' mode; day-replicas in 'probe').
#   2. Launches in batches of $BATCH_SIZE (default 100) with $BATCH_PAUSE_SEC
#      pause between batches.
#   3. Retries gcloud-create on transient quota / IP-exhaustion errors.
#   4. All VMs in asia-northeast1-c; same machine type; per-VM unique name
#      via shard-index suffix (avoids the same-second RUN_TS collision the
#      cefi-week-test launcher hit on 2026-04-29).
#   5. Skip-existing is automatic (orchestrator.py:1015 reads manifest and
#      bypasses CAPTURED shards). Re-running this script after a partial
#      run only does the missing work.
#
# Quota notes (asia-northeast1):
#   - Default in-use IP addresses: ~64. We only need 1 per VM (internal NAT
#     by default). External-IP VMs cap at the public IP quota.
#   - CPUs: e2-standard-8 = 8 CPUs/VM. 100 VMs = 800 CPUs.
#   - Persistent-disk: 50 GB/VM × 100 = 5 TB per batch.
#   - In-use addresses ~ 100 per batch (each VM gets 1 ephemeral external IP
#     by default unless --no-address used).
# Run ``gcloud compute project-info describe --project=$PROJECT --format='value(quotas)'``
# pre-launch to verify headroom.
set -euo pipefail

MODE="${1:-}"
shift || true

# Region asia-northeast1 (Tardis IP-allowlist locked to this region — VMs in
# different regions get rate-limited even at low concurrency). Round-robin
# across zones a/b/c so per-zone quota exhaustion (typical at ~100-150
# concurrent VMs in one zone) falls through to siblings. Per-zone CPU /
# in-use-IP / persistent-disk quotas are independent so 3 zones triples
# the headroom without changing Tardis's view of where the calls come from.
REGION="asia-northeast1"
ZONES=("asia-northeast1-a" "asia-northeast1-b" "asia-northeast1-c")
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"
BATCH_SIZE="${BATCH_SIZE:-100}"            # VMs per batch
BATCH_PAUSE_SEC="${BATCH_PAUSE_SEC:-30}"   # pause between batches for quota recovery
RETRY_MAX="${RETRY_MAX:-3}"                # retries per VM on transient gcloud errors
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

usage() {
    cat <<EOF
Usage:
  $0 probe <YYYY-MM-DD>                     # 364 day-VMs all on same day (concurrency test)
  $0 full <START_YYYY-MM-DD> <END_YYYY-MM-DD>  # 1 VM per ISO week in window

Env overrides:
  MACHINE_TYPE    default e2-standard-8 (book_snapshot_5 days may need e2-standard-16)
  BATCH_SIZE      default 100 VMs per batch
  BATCH_PAUSE_SEC default 30s between batches
  RETRY_MAX       default 3 retries per VM on quota / transient error

Examples:
  $0 probe 2026-04-22
  BATCH_SIZE=50 $0 probe 2026-04-22
  $0 full 2020-01-01 2026-12-31
  MACHINE_TYPE=e2-standard-16 $0 full 2020-01-01 2026-12-31
EOF
    exit 2
}

case "$MODE" in
    probe|full) ;;
    *) usage ;;
esac

# Build shard list (one shard per VM): each shard is a (start_date, end_date)
# pair the VM will pass to launch-cefi-forward-poll.sh.
SHARDS_FILE="$(mktemp -t cefi-shards-XXXXXX)"
trap 'rm -f "$SHARDS_FILE"' EXIT

if [[ "$MODE" == "probe" ]]; then
    PROBE_DATE="${1:-}"
    if [[ -z "$PROBE_DATE" ]]; then usage; fi
    REPLICA_COUNT="${REPLICA_COUNT:-364}"
    for i in $(seq 1 "$REPLICA_COUNT"); do
        echo "$PROBE_DATE $PROBE_DATE $i" >> "$SHARDS_FILE"
    done
    echo "Mode: probe — $REPLICA_COUNT day-VMs all targeting $PROBE_DATE (skip-existing makes most no-ops)"
elif [[ "$MODE" == "full" ]]; then
    START_DATE="${1:-}"
    END_DATE="${2:-}"
    if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then usage; fi
    # Compute ISO weeks in window. Each VM handles one Mon-Sun stretch.
    python3 - <<PYEOF >> "$SHARDS_FILE"
from datetime import date, timedelta
s = date.fromisoformat("$START_DATE")
e = date.fromisoformat("$END_DATE")
# Snap start to Monday of its ISO week.
s -= timedelta(days=s.weekday())
i = 0
while s <= e:
    end_of_week = min(s + timedelta(days=6), e)
    i += 1
    print(f"{s.isoformat()} {end_of_week.isoformat()} {i}")
    s += timedelta(days=7)
PYEOF
    echo "Mode: full — $(wc -l < "$SHARDS_FILE" | tr -d ' ') week-VMs across ${START_DATE}..${END_DATE}"
fi

TOTAL=$(wc -l < "$SHARDS_FILE" | tr -d ' ')
RUN_TS="$(date +%Y%m%d-%H%M%S)"
echo "Run timestamp: $RUN_TS"
echo "Region: $REGION  Zones: ${ZONES[*]}  Machine: $MACHINE_TYPE  Disk: ${BOOT_DISK_GB}G"
echo "Batch size: $BATCH_SIZE  Pause: ${BATCH_PAUSE_SEC}s  Retries: $RETRY_MAX"
echo "Total VMs: $TOTAL"
echo ""

# Common metadata fragment — only VM_START_DATE / VM_END_DATE / vm_name vary.
_common_meta() {
    echo -n "VM_TASK=cefi-backfill"
    echo -n ",VM_SERVICE=market_tick_data_service"
    echo -n ",VM_OPERATION=download"
    echo -n ",VM_ASSET_GROUP=CEFI"
    echo -n ",VM_SHUTDOWN_ON_COMPLETION=true"
    echo -n ",MANIFEST_PER_VM_SHARDS=true"
}

# Launch one VM with retry across all zones in the region. Exits non-zero
# only after every zone fails RETRY_MAX times. The shard index is used as
# a round-robin offset so the first batch spreads across a/b/c rather than
# piling onto -a and waiting for it to fail.
_launch_one() {
    local vm_name="$1" start_date="$2" end_date="$3" shard_idx="$4"
    local meta
    meta="startup-script-url=${STARTUP},$(_common_meta),VM_START_DATE=${start_date},VM_END_DATE=${end_date}"
    local zone_count=${#ZONES[@]}
    local attempt=0
    local zone_offset=$(( (shard_idx - 1) % zone_count ))
    while (( attempt < RETRY_MAX * zone_count )); do
        local zone="${ZONES[$(( (zone_offset + attempt) % zone_count ))]}"
        if gcloud compute instances create "$vm_name" \
                --project="$PROJECT" --zone="$zone" \
                --machine-type="$MACHINE_TYPE" \
                --boot-disk-size="${BOOT_DISK_GB}GB" \
                --image-family=ubuntu-2404-lts-amd64 \
                --image-project=ubuntu-os-cloud \
                --scopes=cloud-platform \
                --metadata="$meta" \
                --labels=purpose=cefi-massive-rollout,run-ts="$RUN_TS",mode="$MODE" \
                > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        # Quota / IP exhaustion errors are zone-local and typically
        # transient; same-zone retry rarely helps but cross-zone retry
        # routes around the exhausted one. Linear backoff scaled by
        # zone-cycle progress so early retries are fast (5s) and later
        # attempts space out (up to 15s).
        sleep $(( ((attempt / zone_count) + 1) * 5 ))
    done
    echo "  FAILED after $((RETRY_MAX * zone_count)) attempts across $zone_count zones: $vm_name"
    return 1
}

LAUNCHED=0
FAILED=0
BATCH_NUM=0
PIDS=()

while IFS=' ' read -r start end idx; do
    BATCH_POS=$((LAUNCHED % BATCH_SIZE))
    if [[ $BATCH_POS -eq 0 ]] && [[ $LAUNCHED -gt 0 ]]; then
        # Wait for the previous batch to finish launching before pausing.
        wait
        BATCH_NUM=$((BATCH_NUM + 1))
        echo "[batch $BATCH_NUM] launched $LAUNCHED / $TOTAL — pausing ${BATCH_PAUSE_SEC}s for quota recovery"
        sleep "$BATCH_PAUSE_SEC"
    fi
    vm_name="cefi-mr-${RUN_TS}-${idx}"
    _launch_one "$vm_name" "$start" "$end" "$idx" &
    PIDS+=($!)
    LAUNCHED=$((LAUNCHED + 1))
done < "$SHARDS_FILE"

# Wait for the final batch.
wait

# Count failures from background processes.
for pid in "${PIDS[@]}"; do
    if ! wait "$pid" 2>/dev/null; then
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Launch complete: $LAUNCHED launched, $FAILED failed"
echo "Run ts: $RUN_TS"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter='labels.run-ts=$RUN_TS' --format='table(name,status)' | head -20"
echo "Count by status:"
echo "  gcloud compute instances list --filter='labels.run-ts=$RUN_TS' --format='value(status)' | sort | uniq -c"
echo "Verify GCS captures (probe mode):"
echo "  gsutil du -sh gs://market-data-tick-cefi-central-element-323112/day=YYYY-MM-DD/"
echo "Verify manifest entries reach GCS:"
echo "  gsutil ls gs://market-data-tick-cefi-central-element-323112/_index/per_vm/cefi-mr-${RUN_TS}-*.parquet | wc -l"
