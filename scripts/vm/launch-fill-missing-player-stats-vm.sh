#!/usr/bin/env bash
# Launch a GCE VM that runs ``instruments-service/scripts/fill_missing_player_stats.py``
# in asia-northeast1-c (close to api_football, close to GCS sports buckets).
#
# Why this exists rather than just relaunching ``af-backfill-*`` again:
# the orchestrator iterates dates chronologically and spends most of its
# wall-clock on ``_should_skip_shard`` checks for the 80%+ of dates already
# complete. The targeted script reads the canonical manifest, computes the
# (date, league_id) gap as ``FIXTURES-captured ∩ Prediction-tier-api_football
# −  PLAYER_STATS-attempted``, and fires api_football calls ONLY at the
# missing shards. Mirrors the ``/tmp/fill_missing_ohlcv.py`` Databento
# OHLCV pattern; SSOT for the script body lives at
# ``instruments-service/scripts/fill_missing_player_stats.py``.
#
# Singleton lock: refuses to launch if any af-backfill-* OR
# fill-missing-player-stats-* VM is already running in the zone — both
# share the same api_football API key and per-key rate limit, so
# concurrent runs split the quota and produce 429s without throughput
# gain (see 2026-04-19 SFI thundering-herd incident). Use --force only
# when the running VM is doing a genuinely disjoint task (e.g. forward-poll
# of a different entity).
#
# Prerequisites:
#   - Tarballs refreshed (so the new script ships):
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - api-football-api-key in Secret Manager
#   - Watchdog dict updated (``fill-missing-player-stats-`` prefix added to
#     VM_PREFIX_TO_BUCKET in vm_zombie_watchdog.py + watchdog VM relaunched)
#
# Usage:
#   bash launch-fill-missing-player-stats-vm.sh                            # full sweep, default concurrency
#   bash launch-fill-missing-player-stats-vm.sh --concurrency 8            # bump concurrency
#   bash launch-fill-missing-player-stats-vm.sh --start-date 2024-01-01    # restrict window
#   bash launch-fill-missing-player-stats-vm.sh --limit 50                 # smoke (50 dates)
#   bash launch-fill-missing-player-stats-vm.sh --force                    # bypass singleton
#
# Cost: e2-standard-2 for ~2-4h depending on rate-limit ceiling
# (api_football Mega plan ≈ 900 req/min; ~117k calls expected for the
# full PLAYER_STATS gap = ~2.2h at full quota).

set -euo pipefail

FORCE=false
CONCURRENCY=4
START_DATE="2020-06-06"
END_DATE=""
LIMIT=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date) END_DATE="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,40p' "$0"
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$END_DATE" ]]; then
  END_DATE="$(date -u +%Y-%m-%d)"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock: api_football rate-limits per-key ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^fill-missing-player-stats-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: an api_football VM is already running in $ZONE: $EXISTING
Refusing to launch a duplicate — api_football rate-limits per-key;
concurrent VMs thrash on 429s without producing useful data.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ...
EOF
    exit 1
  fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="fill-missing-player-stats-${RUN_TS}"

# Build the python invocation. The ``python`` literal here gets rewritten
# to ``$VENV/bin/python`` by setup-data-pipeline-vm.sh's sports-gap-fill
# dispatch (same substitution as sports-manifest-rescan / canonical-migration).
SCRIPT_ARGS="--start-date ${START_DATE} --end-date ${END_DATE} --concurrency ${CONCURRENCY}"
if (( LIMIT > 0 )); then
  SCRIPT_ARGS="${SCRIPT_ARGS} --limit ${LIMIT}"
fi
MIGRATION_CMD="python scripts/fill_missing_player_stats.py ${SCRIPT_ARGS}"

echo "Launching $VM_NAME"
echo "  Command: ${MIGRATION_CMD}"
echo "  Window:  ${START_DATE} → ${END_DATE}"
echo "  Concurrency: ${CONCURRENCY}"
(( LIMIT > 0 )) && echo "  Limit:   ${LIMIT} dates"

METADATA="VM_TASK=sports-gap-fill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
METADATA="${METADATA},VM_SPORTS_ENTITY=PLAYER_STATS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_MIGRATION_CMD=${MIGRATION_CMD}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=fill-missing-player-stats,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-gap-fill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Manifest will absorb new rows on the consolidator's next cron tick (~1min)."
