#!/usr/bin/env bash
# Launch a short-lived GCE VM that forward-polls FootyStats for the next 14 days.
#
# Purpose: capture odds + predictions evolution over time. Each VM boot writes
# a single `fetched_at_hour=YYYY-MM-DDTHH` snapshot per (kickoff date, entity)
# under `gs://instruments-store-sports-.../sports_reference/by_date/day={D}/entity={E}/`.
# Running this VM hourly (or daily) accumulates multiple snapshots per fixture,
# enabling retrospective analysis of "when did FT first publish" and "do odds move".
#
# Default: polls `today` through `today + 14 days` and shuts itself down on success.
#
# Prerequisites:
#   - Tarballs already uploaded to gs://deployment-scripts-central-element-323112/code/
#   - FOOTYSTATS_API_KEY in Secret Manager under 'footystats-api-key'
#
# Usage:
#   bash launch-footystats-forward-poll.sh                      # today..+14d (default)
#   bash launch-footystats-forward-poll.sh 0 30                 # today..+30d
#   bash launch-footystats-forward-poll.sh 0 14 ODDS            # only entity=ODDS
#   bash launch-footystats-forward-poll.sh 0 14 PREDICTIONS     # only entity=PREDICTIONS
#
# Cost: e2-small for ~5 min per run = ~$0.004 per fire. Delete on completion.
set -euo pipefail

DAYS_FROM="${1:-0}"      # offset from today (negative for backfill; positive not used — future==today is fine)
DAYS_TO="${2:-14}"       # how far forward
SPORTS_ENTITY="${3:-}"   # optional: ODDS | PREDICTIONS | MATCHES (empty = all)
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

START_DATE="$(date -v+"${DAYS_FROM}"d +%Y-%m-%d 2>/dev/null || date -d "+${DAYS_FROM} days" +%Y-%m-%d)"
END_DATE="$(date -v+"${DAYS_TO}"d +%Y-%m-%d 2>/dev/null || date -d "+${DAYS_TO} days" +%Y-%m-%d)"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_TS_LABEL="$(date +%Y%m%d-%H%M%S)"
VM_NAME="footystats-fwd-${RUN_TS}"

echo "Launching $VM_NAME: FOOTYSTATS ${START_DATE}..${END_DATE} entity=${SPORTS_ENTITY:-ALL}"

METADATA="VM_TASK=sports-forward-poll"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_CATEGORY=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SPORTS_PROVIDER=FOOTYSTATS"
[[ -n "$SPORTS_ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${SPORTS_ENTITY}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-small \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=footystats-forward-poll,run-ts="${RUN_TS_LABEL}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
