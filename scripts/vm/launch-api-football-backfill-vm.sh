#!/usr/bin/env bash
# Launch a GCE VM that backfills sports FIXTURES from API-Football for an
# explicit date range. Same code path as the live adapter + Cloud Run T+1 recon
# — just dispatched to a VM so multi-year runs don't block a laptop.
#
# Purpose: fill genuine "no parquet at all" gaps in the manifest. The rescan
# script handles empty_confirmed for dates where the adapter ran and got zero;
# this launcher handles the other direction (adapter never ran).
#
# Writes to gs://instruments-store-sports-central-element-323112/
#   sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet
# per day in [START_DATE..END_DATE] inclusive.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m instruments_service \
#     --operation instruments --mode batch --category SPORTS \
#     --sports-provider API_FOOTBALL \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --category SPORTS
#   - api-football-api-key in Secret Manager
#
# Usage:
#   bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15   # pre-deployment backfill
#   bash launch-api-football-backfill-vm.sh 2026-04-21 2026-05-01   # forward-poll ~10 days
#   bash launch-api-football-backfill-vm.sh --force <start> <end>   # bypass singleton lock
#
# Cost: e2-standard-2 for ~5-30 min depending on range size. API-Football
# fixtures-by-date returns all leagues in one call per date, so the wall clock
# is dominated by rate-limit pacing (one call per date in the range).
#
# Singleton lock: refuses to launch if any af-backfill-* VM is already running
# in the zone. API-Football shares one API key across all VMs and enforces
# per-key rate limits — multiple concurrent VMs produce 429s without useful
# throughput. Reference incident: 2026-04-19 SFI thundering herd (10 VMs /
# 6h / ~4 useful writes), same shape. Pass --force only when you have a
# genuinely disjoint reason (e.g. testing on a different date window while an
# unrelated run is live).
set -euo pipefail

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
  shift
fi

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
Usage: bash launch-api-football-backfill-vm.sh [--force] <START_DATE> <END_DATE>

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).

Examples:
  bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15
  bash launch-api-football-backfill-vm.sh 2026-04-21 2026-05-01
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

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock: API-Football rate-limits per-key ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^af-backfill-" AND status=RUNNING' \
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
  Force:     bash $0 --force ${START_DATE} ${END_DATE}
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="af-backfill-${RUN_TS}"

echo "Launching $VM_NAME: API_FOOTBALL backfill ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_CATEGORY=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
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
  --labels=purpose=api-football-backfill,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
