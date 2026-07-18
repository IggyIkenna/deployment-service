#!/usr/bin/env bash
# Epic: cefi backfill throughput
# Lifecycle: PERMANENT — supervises the cap-1 CeFi backfill and rotates the VM before the
#            Tardis per-session throughput cliff.
#
# WHY THIS EXISTS (measured 2026-07-18, network-RX counter):
#   A backfill VM sustains ~12 MB/s for its first ~7-8 GB, then collapses to ~2 MB/s and never
#   recovers — enforced by connection starvation (ConnectionTimeoutError to Wasabi), NOT by any
#   HTTP 429, so nothing in the fetch path can detect or back off from it. Raising client
#   concurrency 6x changed nothing (8 slots = 7.67 MB/s vs 48 slots = 7.58 MB/s, both cliffing
#   at ~7 GB in the same ~16 min).
#
#   The throttle is VM/SESSION-scoped, not account-scoped: a FRESH VM launched ~15 min after the
#   previous one was throttled immediately regained full speed (1.16 boot -> 6.03 -> 13.30 MB/s).
#
#   So rotating the VM converts "~12 MB/s for 7 GB then ~2 MB/s forever" (~2-2.7 MB/s effective)
#   into "~7 GB per ~20 min cycle" (~5.8 MB/s effective) — a ~2.5-3x win from scheduling alone.
#   Exactly ONE VM is ever live, so the cap-1 Tardis rule is never violated.
#
# Usage: rotate-cefi-backfill-vm.sh [--cycles N] [--rotate-gb 6.5] [--floor-mbps 4] [--dry-run]
set -uo pipefail
P="${GCP_PROJECT_ID:-central-element-323112}"; Z=asia-northeast1-c
DS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CYCLES=0; ROTATE_GB=6.5; FLOOR=4.0; LOW_TICKS=3; DRY=0
while [[ $# -gt 0 ]]; do case "$1" in
  --cycles) CYCLES="$2"; shift 2;;
  --rotate-gb) ROTATE_GB="$2"; shift 2;;
  --floor-mbps) FLOOR="$2"; shift 2;;
  --dry-run) DRY=1; shift;;
  *) echo "unknown arg $1" >&2; exit 2;;
esac; done

_running_vm() { gcloud compute instances list --project="$P" \
  --filter='(status=RUNNING OR status=PROVISIONING OR status=STAGING)' --format='value(name)' 2>/dev/null \
  | grep -iE '^cefi-queue' | head -1; }

_rx_mbps() {  # $1=instance_id -> MB/s over the last aligned minute
  local iid="$1" tok e s
  tok=$(gcloud auth print-access-token 2>/dev/null)
  e=$(date -u +%Y-%m-%dT%H:%M:%SZ); s=$(date -u -d '3 minutes ago' +%Y-%m-%dT%H:%M:%SZ)
  curl -sG "https://monitoring.googleapis.com/v3/projects/$P/timeSeries" -H "Authorization: Bearer $tok" \
    --data-urlencode "filter=metric.type=\"compute.googleapis.com/instance/network/received_bytes_count\" AND resource.labels.instance_id=\"$iid\"" \
    --data-urlencode "interval.startTime=$s" --data-urlencode "interval.endTime=$e" \
    --data-urlencode "aggregation.alignmentPeriod=60s" --data-urlencode "aggregation.perSeriesAligner=ALIGN_RATE" 2>/dev/null \
  | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(0); raise SystemExit
p=[float(x['value'].get('doubleValue',x['value'].get('int64Value',0))) for s in d.get('timeSeries',[]) for x in s.get('points',[])]
print(f'{p[-1]/1e6:.2f}' if p else 0)"
}

cycle=0
while :; do
  cycle=$((cycle+1))
  [ "$CYCLES" -gt 0 ] && [ "$cycle" -gt "$CYCLES" ] && { echo "reached --cycles $CYCLES; done"; break; }
  VM=$(_running_vm)
  if [ -z "$VM" ]; then
    echo "[cycle $cycle] launching backfill VM..."
    [ "$DRY" = 1 ] || ( cd "$DS" && YEARS="${YEARS:-2026}" START_DATE="${START_DATE:-2026-02-05}" \
        SINGLE_VM_QUEUE=1 LAUNCH_GROUPS="${LAUNCH_GROUPS:-heavy}" STALL_TIMEOUT_SEC=9000 \
        bash scripts/vm/launch-cefi-sharded-backfill.sh >/dev/null 2>&1 )
    for _ in $(seq 1 30); do VM=$(_running_vm); [ -n "$VM" ] && break; sleep 10; done
    [ -z "$VM" ] && { echo "  launch failed; retrying next cycle"; sleep 60; continue; }
  fi
  IID=$(gcloud compute instances describe "$VM" --zone=$Z --project="$P" --format='value(id)' 2>/dev/null)
  echo "[cycle $cycle] supervising $VM (rotate at ${ROTATE_GB}GB or ${FLOOR}MB/s x${LOW_TICKS})"
  cum=0; low=0
  while :; do
    sleep 60
    ST=$(gcloud compute instances describe "$VM" --zone=$Z --project="$P" --format='value(status)' 2>/dev/null || echo GONE)
    if [ "$ST" != "RUNNING" ]; then echo "  $VM status=$ST (SPOT preemption?) — relaunching"; break; fi
    rx=$(_rx_mbps "$IID"); rx=${rx:-0}
    cum=$(python3 -c "print(f'{$cum + $rx*60/1000:.2f}')")
    echo "  $(date -u +%H:%M:%S) rx=${rx}MB/s cum=${cum}GB"
    if python3 -c "import sys; sys.exit(0 if $cum >= $ROTATE_GB else 1)"; then
      echo "  -> reached ${cum}GB (>= ${ROTATE_GB}) — ROTATING before the cliff"; break; fi
    if python3 -c "import sys; sys.exit(0 if $rx < $FLOOR else 1)"; then
      low=$((low+1)); [ "$low" -ge "$LOW_TICKS" ] && { echo "  -> rx<${FLOOR}MB/s x${low} — throttled, ROTATING"; break; }
    else low=0; fi
  done
  if [ "$DRY" = 1 ]; then echo "  (dry-run: would delete $VM)"; else
    gcloud compute instances delete "$VM" --zone=$Z --project="$P" --quiet >/dev/null 2>&1
    for _ in $(seq 1 20); do [ -z "$(_running_vm)" ] && break; sleep 10; done
    echo "  deleted $VM"
  fi
done
