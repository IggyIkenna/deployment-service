#!/usr/bin/env bash
# Launch a GCE VM that backfills sports FIXTURES from API-Football. Same code
# path as the live adapter + Cloud Run T+1 recon — just dispatched to a VM so
# multi-year runs don't block a laptop.
#
# Two invocation shapes:
#
#   1. Rolling forward-poll (preferred for FIXTURES schedule-polling):
#        bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7
#        bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
#
#      Resolves to --lookback-days / --lookahead-days / --force-window on the VM,
#      so the instruments-service CLI computes [today-N, today+M] at VM boot
#      time (UTC). This matches the rolling-window contract in
#      codex/02-data/sports-scheduling-and-sharding.md §4 and avoids stale
#      start/end dates frozen at launcher-invocation time.
#
#   2. Explicit historical range (use for pre-deployment backfill):
#        bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15
#        bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31
#
# Writes to gs://instruments-store-sports-central-element-323112/
#   sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet
# per day in the resolved window (inclusive).
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   # Rolling mode:
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider API_FOOTBALL \
#     --lookback-days $VM_LOOKBACK_DAYS --lookahead-days $VM_LOOKAHEAD_DAYS \
#     [--force-window]
#
#   # Explicit-date mode:
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider API_FOOTBALL \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - api-football-api-key in Secret Manager
#
# Usage:
#   bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15                          # historical backfill
#   bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31        # explicit forward range
#   bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7                     # rolling forward-poll
#   bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
#   bash launch-api-football-backfill-vm.sh --force <start> <end>                          # bypass singleton lock
#
# --entity FIXTURES | INJURIES | FIXTURE_STATS | FIXTURE_EVENTS | FIXTURE_LINEUPS | PLAYER_STATS
#   Restricts the VM to a single manifest entity. Use FIXTURES for forward-poll
#   (API-Football publishes schedules weeks/months ahead; per-fixture
#   enrichments only exist post-match).
#
# Cost: e2-standard-2 for ~5-30 min depending on range size. API-Football
# fixtures-by-date returns all leagues in one call per date, so the wall clock
# is dominated by rate-limit pacing (one call per date in the range).
#
# Singleton lock: refuses to launch if any af-backfill-* OR af-audit-* VM is
# already running in the zone. API-Football shares one API key across all VMs
# and enforces per-key rate limits — multiple concurrent VMs produce 429s
# without useful throughput. Reference incident: 2026-04-19 SFI thundering
# herd (10 VMs / 6h / ~4 useful writes), same shape. Pass --force only when
# you have a genuinely disjoint reason (e.g. testing on a different date
# window while an unrelated run is live).
set -euo pipefail

FORCE=false
ENTITY=""
LOOKBACK=""
LOOKAHEAD=""
FORCE_WINDOW=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --force-window) FORCE_WINDOW=true; shift ;;
    *) break ;;
  esac
done

# Mode resolution: rolling (--lookback/--lookahead) vs explicit (<start> <end>).
USE_ROLLING=false
if [[ -n "$LOOKBACK" || -n "$LOOKAHEAD" ]]; then
  USE_ROLLING=true
fi

if $USE_ROLLING; then
  if [[ $# -ne 0 ]]; then
    cat >&2 <<EOF
ERROR: cannot combine --lookback/--lookahead with positional <start> <end> dates.
Pick one mode: rolling (--lookback N --lookahead M) OR explicit (<start> <end>).
EOF
    exit 1
  fi
  # Default missing side to 0 so --lookback 1 alone works (end = today).
  [[ -z "$LOOKBACK" ]] && LOOKBACK=0
  [[ -z "$LOOKAHEAD" ]] && LOOKAHEAD=0
  if ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]] || ! [[ "$LOOKAHEAD" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --lookback / --lookahead must be non-negative integers (got lookback=$LOOKBACK, lookahead=$LOOKAHEAD)" >&2
    exit 1
  fi
  RANGE_DESC="rolling [today-${LOOKBACK}..today+${LOOKAHEAD}] UTC"
else
  if [[ $# -ne 2 ]]; then
    cat >&2 <<EOF
Usage: bash launch-api-football-backfill-vm.sh [--force] [--entity ENTITY] \\
         ( <START_DATE> <END_DATE> | --lookback N --lookahead M [--force-window] )

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  --lookback N / --lookahead M: rolling window resolved to [today-N..today+M] on the VM at boot (UTC).
  --force-window: disable skip-if-exists for the rolling window (forward-poll overwrite contract).
  ENTITY (optional): FIXTURES | INJURIES | FIXTURE_STATS | FIXTURE_EVENTS |
                     FIXTURE_LINEUPS | PLAYER_STATS. Omit for all entities.

Examples:
  bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15
  bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31
  bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7
  bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
EOF
    exit 1
  fi

  START_DATE="$1"
  END_DATE="$2"

  # Sanity-check the date format up front — typos turn into silent VM boots.
  if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
    exit 1
  fi
  RANGE_DESC="${START_DATE}..${END_DATE}"

  if $FORCE_WINDOW; then
    echo "WARNING: --force-window is only meaningful with --lookback/--lookahead; ignored in explicit-date mode." >&2
    FORCE_WINDOW=false
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock: API-Football rate-limits per-key ──
# Catches both af-backfill-* (this script) and af-audit-* (truth-set audit
# launcher) — they share the api_football key and must be mutually exclusive.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^af-audit-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: API-Football VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — API-Football rate-limits per-key; concurrent
VMs thrash on 429s without producing useful data (see 2026-04-19 SFI incident).

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ...
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="af-backfill-${RUN_TS}"

ENTITY_DESC="all entities"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY only"
echo "Launching $VM_NAME: API_FOOTBALL backfill ${RANGE_DESC} ($ENTITY_DESC)"

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
if $USE_ROLLING; then
  METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
  METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
  $FORCE_WINDOW && METADATA="${METADATA},VM_FORCE_WINDOW=true"
else
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
fi
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Machine type override via env (default e2-standard-4 — bumped from -2 after
# 2026-05-06 OOM during per-VM shard merge: 51 shards × pandas concat blew
# past 8GB on e2-standard-2). Operator can override with MACHINE_TYPE=e2-...
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type="$MACHINE_TYPE" \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=api-football-backfill,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
