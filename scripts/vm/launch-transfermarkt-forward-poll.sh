#!/usr/bin/env bash
# Launch a short-lived GCE VM that forward-polls Transfermarkt PLAYER_VALUES and
# TRANSFERMARKT_LEAGUES for the current week so valuations keep flowing forward
# between historical backfill runs.
#
# Transfer-window gate: PLAYER_VALUES / TRANSFERMARKT_LEAGUES change meaningfully
# only during active transfer windows (summer: Jun-Aug, winter: Jan-Feb). Outside
# these windows the script exits 0 without launching a VM; use --skip-window-check
# to override (e.g. to refresh league metadata year-round).
#
# Scrape hang prevention: the instruments-service caller MUST wrap each per-shard
# scrape in asyncio.wait_for(coro, timeout=N) (per the 2026-06-22 incident where
# Transfermarkt + FootyStats hung 6.5h on unbounded HTTP calls). The per-shard
# timeout prevents a single stalled request from blocking the entire VM. Fix lives
# in instruments-service; this launcher enforces VM_SHUTDOWN_ON_COMPLETION so a
# hung VM is caught by the fleet monitor reading the persisted GCS run.log.
#
# Singleton lock: refuses to launch if any tm-fwd-* VM is already running in the
# zone (shared API key; ~1 req/sec pacing). Pass --force to override.
#
# Default mode: rolling [today-7..today] (last 7 days) with VM_FORCE_WINDOW=true
# so stale / missed values are re-fetched each run.
#
# SSOT cadence: unified-trading-pm/codex/02-data/sports-scheduling-and-sharding.md
# §2.2 (Tier-2 daily; window-aware PLAYER_VALUES/TRANSFERMARKT_LEAGUES cadence).
# OOM fix (e2-standard-8 default): plans/active/sports_reference_backfill_oom_2026_06_22.md
# Bucket-naming SSOT: bucket_name_ssot_canonicalisation_2026_05_10.md Phase 0f.
#
# Usage:
#   bash launch-transfermarkt-forward-poll.sh
#   bash launch-transfermarkt-forward-poll.sh --entity PLAYER_VALUES
#   bash launch-transfermarkt-forward-poll.sh --entity TRANSFERMARKT_LEAGUES
#   bash launch-transfermarkt-forward-poll.sh --lookback 14 --lookahead 0
#   bash launch-transfermarkt-forward-poll.sh --skip-window-check
#   bash launch-transfermarkt-forward-poll.sh --dry-run
#   bash launch-transfermarkt-forward-poll.sh --force   # override singleton lock
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - transfermarkt API key in Secret Manager
# Epic: sports_master
# Lifecycle: permanent
# Delete-when: N/A — permanent recurring launcher
set -euo pipefail

# e2-standard-8 (32 GB): safe default for sports backfills — the 6.5 GB sports
# availability index OOM-killed e2-standard-2 (fixed instruments-service@505dcd9).
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

FORCE=false
ENTITY=""
LOOKBACK=7
LOOKAHEAD=0
SKIP_WINDOW_CHECK=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --skip-window-check) SKIP_WINDOW_CHECK=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]] || ! [[ "$LOOKAHEAD" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --lookback / --lookahead must be non-negative integers (got lookback=$LOOKBACK, lookahead=$LOOKAHEAD)" >&2
  exit 1
fi

# Transfer-window gate: PLAYER_VALUES / TRANSFERMARKT_LEAGUES update meaningfully
# only during active windows. Outside windows the run is harmless but wastes quota.
# Windows: summer Jun 1 – Aug 31, winter Jan 1 – Feb 15 (bash-level approximation;
# instruments-service has the authoritative is_transfer_window_open() check per-shard).
_is_in_transfer_window() {
  local month day
  month="$(date -u +%-m)"   # 1-12
  day="$(date -u +%-d)"     # 1-31
  # Summer window: June (6), July (7), August (8)
  if [[ "$month" -ge 6 && "$month" -le 8 ]]; then return 0; fi
  # Winter window: January (1), first half of February (2, day ≤ 15)
  if [[ "$month" -eq 1 ]]; then return 0; fi
  if [[ "$month" -eq 2 && "$day" -le 15 ]]; then return 0; fi
  return 1
}

if ! $SKIP_WINDOW_CHECK; then
  if ! _is_in_transfer_window; then
    CURRENT_MONTH="$(date -u +%B)"
    cat >&2 <<EOF
INFO: Skipping Transfermarkt forward-poll — not in an active transfer window (${CURRENT_MONTH}).
  Transfer windows: June–August (summer), January–15 February (winter).
  PLAYER_VALUES / TRANSFERMARKT_LEAGUES data is static outside these windows.
  Use --skip-window-check to force launch anyway (e.g. for TRANSFERMARKT_LEAGUES metadata).
EOF
    exit 0
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^tm-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Transfermarkt forward-poll VM already running in $ZONE: $EXISTING
Refusing duplicate launch — shared API key rate limits.

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
VM_NAME="tm-fwd-${RUN_TS}"

ENTITY_DESC="PLAYER_VALUES + TRANSFERMARKT_LEAGUES"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=${ENTITY}"
RANGE_DESC="rolling [today-${LOOKBACK}..today+${LOOKAHEAD}] UTC (force-window)"
echo "Launching $VM_NAME: TRANSFERMARKT forward-poll ${RANGE_DESC} (${ENTITY_DESC})"

METADATA="VM_TASK=sports-forward-poll"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
METADATA="${METADATA},VM_FORCE_WINDOW=true"
METADATA="${METADATA},VM_SPORTS_PROVIDER=TRANSFERMARKT"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=transfermarkt-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
