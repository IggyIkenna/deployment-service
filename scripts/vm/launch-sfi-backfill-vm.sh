#!/usr/bin/env bash
# Launch a GCE VM that backfills SoccerFootball.info (SFI / SharpAPI) reference
# data — leagues + progressive stats — for a multi-year historical range.
# Sibling to `launch-sfi-forward-poll.sh` (which is T-1 / forward-poll only).
#
# **SFI has NO standings endpoint** — confirmed in
# `instruments-service/instruments_service/engine/orchestrator.py` L4365-4367
# (`_want_sfi_standings = False`). Pre-match league position comes from
# API-Football `STANDINGS`; do not pass `--entity SFI_STANDINGS` here, the
# instruments-service path will skip it.
#
# Cadence + publication rules: unified-trading-pm/codex/02-data/
# sports-scheduling-and-sharding.md §2.4 (Tier-1 every 6h for SFI_LEAGUES;
# Tier-4 T+24h for SFI_PROGRESSIVE_STATS — needs completed-matchday state).
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m instruments_service \
#     --operation instruments --mode batch --category SPORTS \
#     --sports-provider SOCCER_FOOTBALL_INFO \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#   (or --sports-entity SFI_LEAGUES | SFI_PROGRESSIVE_STATS to scope)
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --category SPORTS
#   - `soccer-football-info-api-key` in Secret Manager
#
# Usage:
#   bash launch-sfi-backfill-vm.sh 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --entity SFI_LEAGUES 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --entity SFI_PROGRESSIVE_STATS 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --force 2020-01-01 2026-04-21
#
# --entity SFI_LEAGUES | SFI_PROGRESSIVE_STATS
#   Restricts the VM to a single manifest entity. Omit for all (default).
#
# Singleton lock: refuses to launch if ANY sfi-* VM is already running in the
# zone (both sfi-backfill-* and sfi-fwd-* share the `soccer-football-info-api-key`
# Secret Manager entry; 10 concurrent SFI VMs thrashed on 429 during the
# 2026-04-19 incident — reference: codex §2.4 + singleton-locked-launchers
# memory). Pass --force to bypass for legitimate parallel investigations.
set -euo pipefail

FORCE=false
ENTITY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    *) break ;;
  esac
done

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
Usage: bash launch-sfi-backfill-vm.sh [--force] [--entity ENTITY] <START_DATE> <END_DATE>

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  ENTITY (optional): SFI_LEAGUES | SFI_PROGRESSIVE_STATS (default: both).
  --force: bypass the sfi-* singleton lock (rate-limit hazard).

Examples:
  bash launch-sfi-backfill-vm.sh 2020-01-01 2026-04-21
  bash launch-sfi-backfill-vm.sh --entity SFI_LEAGUES 2020-01-01 2026-04-21
  bash launch-sfi-backfill-vm.sh --entity SFI_PROGRESSIVE_STATS 2020-01-01 2026-04-21
EOF
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
  exit 1
fi

if [[ -n "$ENTITY" && "$ENTITY" != "SFI_LEAGUES" && "$ENTITY" != "SFI_PROGRESSIVE_STATS" ]]; then
  echo "ERROR: --entity must be SFI_LEAGUES or SFI_PROGRESSIVE_STATS (got $ENTITY)" >&2
  echo "       Note: SFI has no standings endpoint — do not pass SFI_STANDINGS." >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# Singleton lock: lock against ALL `sfi-*` VMs (backfill + forward-poll) since
# they share one Secret Manager API key. The 2026-04-19 thundering-herd incident
# landed 10 sfi-fwd-* VMs concurrently — ~4 useful writes across 60h. Never
# again.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sfi-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: SFI VM already running in $ZONE: $EXISTING
Refusing duplicate launch — shared API key rate limits
(reference: 2026-04-19 thundering-herd incident).

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
VM_NAME="sfi-backfill-${RUN_TS}"

ENTITY_DESC="all SFI entities"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY only"
echo "Launching $VM_NAME: SOCCER_FOOTBALL_INFO backfill ${START_DATE}..${END_DATE} ($ENTITY_DESC)"

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_CATEGORY=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SPORTS_PROVIDER=SOCCER_FOOTBALL_INFO"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
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
  --labels=purpose=sfi-backfill,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
