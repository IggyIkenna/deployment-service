#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is forwarded to each underlying launch-cefi-forward-poll.sh call so the
# spawned VMs target the right env tier.
#
# O-1 β remediation 2026-05-12: this script is a thin orchestrator — it
# fans out N calls to launch-cefi-forward-poll.sh, which is the actual
# `gcloud compute instances create` caller. Observability invariants
# (`VM_NAME=` / `MANIFEST_PER_VM_SHARDS=true` / `VM_SHUTDOWN_ON_COMPLETION=true`)
# are injected at that downstream launcher, not here. See
# launch-cefi-forward-poll.sh METADATA accumulator (lines 104-113).
#
# Throughput probe — fan out 7 parallel CeFi raw-tick VMs, one per day in
# a 7-day window, to measure Tardis API parallelism scaling before
# committing to the full 7-year sharded backfill.
#
# Why: a single VM does one CeFi day in ~37 min wallclock (measured
# 2026-04-29 with cefi-fwd-20260429-031023). Naive serial backfill of
# 2,555 days takes ~58 days, impractical. Sharded across 7 year-VMs
# gives ~8 days wallclock IF Tardis doesn't throttle concurrent calls
# per API key. This launcher tests that assumption: if 7 parallel day-VMs
# all finish in ~37 min wallclock, parallelism is linear and we can fan
# out 365 week-VMs (or 2,555 day-VMs) for the full window in <1 day. If
# they take 2x or more, we hit shared-key throttling and need either a
# different parallelism unit or multiple Tardis keys.
#
# Skip-existing: MTDS orchestrator.py:1015 reads the availability manifest
# at preflight and skips shards already CAPTURED — automatic, no CLI flag.
# So a re-run of this launcher post-cefi-fwd's first day just no-ops on
# 2026-04-28 and processes 2026-04-22..2026-04-27 fresh.
#
# Singleton lock: uses ``--force`` on each underlying launch-cefi-forward-poll.sh
# call so all 7 VMs spawn concurrently with the same ``cefi-fwd-*`` prefix
# (the lock is per-prefix-per-zone and we deliberately bypass it for the
# probe).
#
# Usage:
#   bash launch-cefi-week-test.sh                       # last 7 days
#   bash launch-cefi-week-test.sh 2026-04-22 2026-04-28 # explicit window
#
# After completion: check wallclock via
#   gcloud compute instances list --filter='labels.purpose=cefi-week-test'
# and the deployment-api manifest endpoint to confirm 7 days x N venues
# captured.
set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

_positional=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) _positional+=("$1"); shift ;;
    esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ $# -eq 2 ]]; then
    START_DATE="$1"
    END_DATE="$2"
else
    # Default: last 7 days (T-7..T-1).
    END_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
    START_DATE="$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d "7 days ago" +%Y-%m-%d)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

echo "═══════════════════════════════════════════════════════════════"
echo " CeFi week-throughput probe"
echo " Window:  $START_DATE → $END_DATE"
echo " Run ts:  $RUN_TS"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Build day list. Use python for portability — date arithmetic in bash 3.2
# (macOS default) is fragile.
DAY_LIST="$(python3 -c "
from datetime import date, timedelta
s = date.fromisoformat('$START_DATE')
e = date.fromisoformat('$END_DATE')
d = s
while d <= e:
    print(d.isoformat())
    d += timedelta(days=1)
")"

DAY_COUNT="$(echo "$DAY_LIST" | wc -l | tr -d ' ')"
echo "Launching $DAY_COUNT parallel day-VMs (one per day):"

LAUNCH_T0="$(date +%s)"
DRY_RUN_ARG=()
if [[ "${DRY_RUN:-false}" == "true" ]]; then
    DRY_RUN_ARG=("--dry-run")
    echo "[DRY-RUN] Propagating --dry-run to per-day launch-cefi-forward-poll.sh invocations."
fi
for d in $DAY_LIST; do
    echo "  → cefi-fwd $d $d --force ${DRY_RUN_ARG[*]:-}"
    bash "$SCRIPT_DIR/launch-cefi-forward-poll.sh" "${DRY_RUN_ARG[@]:+${DRY_RUN_ARG[@]}}" --force --env "$DEPLOYMENT_ENV" "$d" "$d" \
        > "/tmp/cefi-week-test-${RUN_TS}-${d}.log" 2>&1 &
    # Stagger 2s between launches: VM names use RUN_TS=YYYYMMDD-HHMMSS so
    # same-second launches collide on gcloud create. 2s gap > 1s timestamp
    # resolution + leaves headroom.
    sleep 2
done
wait
LAUNCH_T1="$(date +%s)"
LAUNCH_DURATION="$((LAUNCH_T1 - LAUNCH_T0))"

echo ""
echo "All $DAY_COUNT VMs launched in ${LAUNCH_DURATION}s."
echo ""
echo "Active fleet:"
gcloud compute instances list --filter='name~"^cefi-fwd-"' \
    --format='table(name,status,creationTimestamp.date(format="%H:%M"))'
echo ""
echo "Monitor (polls every 60s, exits when all done):"
echo "  while gcloud compute instances list --filter='name~\"^cefi-fwd-\"' --format='value(name)' | grep -q .; do"
echo "    sleep 60; gcloud compute instances list --filter='name~\"^cefi-fwd-\"' --format='table(name,status)'"
echo "  done"
echo ""
echo "Then count GCS captures:"
echo "  gsutil du -sh gs://market-data-tick-cefi-central-element-323112/day=2026-04-{22..28}/"
echo "  gsutil ls gs://market-data-tick-cefi-central-element-323112/_index/per_vm/cefi-fwd-${RUN_TS%-*}* | wc -l"
