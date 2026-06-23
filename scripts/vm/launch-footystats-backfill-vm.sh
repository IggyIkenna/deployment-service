#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that backfills FootyStats data (fixtures, match stats, odds
# snapshots, predictions, player performance). Sibling to
# launch-footystats-forward-poll.sh but supports explicit historical ranges and
# rolling windows like launch-api-football-backfill-vm.sh — for pre-deployment
# backfills without laptop runs.
#
# Historical window: coverage varies by league subscription; ~2017 for most
# subscribed leagues (confirm against your FootyStats plan).
#
# Cadence + publication: codex/02-data/sports-scheduling-and-sharding.md §2.3 —
# MATCH_STATS / PLAYER_PERFORMANCE finalise with 6–24h post-match lag; odds
# snapshots are pre-match market captures.
#
# Two invocation shapes:
#
#   1. Rolling:
#        bash launch-footystats-backfill-vm.sh --lookback 1 --lookahead 7
#        bash launch-footystats-backfill-vm.sh --entity ODDS_SNAPSHOTS --lookback 1 --lookahead 7 --force-window
#
#   2. Explicit historical range:
#        bash launch-footystats-backfill-vm.sh 2018-01-01 2019-01-15
#        bash launch-footystats-backfill-vm.sh --entity MATCH_STATS 2024-09-01 2024-09-01
#
# Invocation inside the VM (metadata → setup-data-pipeline-vm.sh):
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider FOOTYSTATS \
#     --start-date ... --end-date ...
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - footystats-api-key in Secret Manager
#
# Usage:
#   bash launch-footystats-backfill-vm.sh 2018-01-01 2019-01-15
#   bash launch-footystats-backfill-vm.sh --entity ODDS_SNAPSHOTS 2024-09-01 2024-09-01
#   bash launch-footystats-backfill-vm.sh --lookback 1 --lookahead 7
#   bash launch-footystats-backfill-vm.sh --force <start> <end>
#
# --entity FIXTURES | MATCH_STATS | ODDS_SNAPSHOTS | PREDICTIONS | PLAYER_PERFORMANCE
#
# Singleton lock: refuses if any fs-backfill-* VM is running (per-key rate limits).
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# Machine type override (default e2-standard-2). The per-league skip-check used
# to re-read the 6.5 GB sports availability index, OOM-killing e2-standard-2;
# fixed in instruments-service@505dcd9 (single index read). e2-standard-8
# (32 GB) is the safe default for large full-range sports backfills.
# SSOT: plans/active/sports_reference_backfill_oom_2026_06_22.md
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

FORCE=false
ENTITY=""
LOOKBACK=""
LOOKAHEAD=""
FORCE_WINDOW=false
RECOVERY_FIXTURE_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --force-window) FORCE_WINDOW=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

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
Usage: bash launch-footystats-backfill-vm.sh [--force] [--entity ENTITY] \\
         ( <START_DATE> <END_DATE> | --lookback N --lookahead M [--force-window] )

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  ENTITY (optional): FIXTURES | MATCH_STATS | ODDS_SNAPSHOTS | PREDICTIONS | PLAYER_PERFORMANCE

Examples:
  bash launch-footystats-backfill-vm.sh 2018-01-01 2019-01-15
  bash launch-footystats-backfill-vm.sh --entity ODDS_SNAPSHOTS 2024-09-01 2024-09-01
  bash launch-footystats-backfill-vm.sh --lookback 1 --lookahead 7
EOF
    exit 1
  fi

  START_DATE="$1"
  END_DATE="$2"

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
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^fs-backfill-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: FootyStats backfill VM already running in $ZONE: $EXISTING
Refusing duplicate — per-key rate limits.

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
VM_NAME="fs-backfill-${RUN_TS}"

ENTITY_DESC="all entities"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY only"
echo "Launching $VM_NAME: FOOTYSTATS backfill ${RANGE_DESC} ($ENTITY_DESC)"

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
METADATA="${METADATA},VM_SPORTS_PROVIDER=FOOTYSTATS"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
[[ -n "$RECOVERY_FIXTURE_IDS" ]] && METADATA="${METADATA},VM_RECOVERY_FIXTURE_IDS=${RECOVERY_FIXTURE_IDS}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

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
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=footystats-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
