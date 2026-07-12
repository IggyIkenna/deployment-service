#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that forward-polls FootyStats for the next N days.
#
# Purpose: capture odds + predictions evolution over time. Each VM boot writes
# a single `fetched_at_hour=YYYY-MM-DDTHH` snapshot per (kickoff date, entity)
# under `gs://instruments-store-sports-.../sports_reference/by_date/day={D}/entity={E}/`.
# Running this VM hourly (or daily) accumulates multiple snapshots per fixture,
# enabling retrospective analysis of "when did FT first publish" and "do odds move".
#
# Default: rolling window [today..today+14] with `--force-window` so every run
# re-fetches the window (required to accumulate snapshots). The VM resolves the
# window at boot time (UTC) via the instruments-service rolling-window CLI
# (`--lookback-days 0 --lookahead-days N --force-window`), per
# codex/02-data/sports-scheduling-and-sharding.md §4.
#
# Prerequisites:
#   - Tarballs uploaded to gs://deployment-scripts-central-element-323112/code/
#     (refresh with: deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS)
#   - FOOTYSTATS_API_KEY in Secret Manager under 'footystats-api-key'
#
# Usage:
#   bash launch-footystats-forward-poll.sh                       # today..today+14
#   bash launch-footystats-forward-poll.sh 30                    # today..today+30
#   bash launch-footystats-forward-poll.sh 14 ODDS               # today..today+14, entity=ODDS
#   bash launch-footystats-forward-poll.sh 14 PREDICTIONS        # today..today+14, entity=PREDICTIONS
#   bash launch-footystats-forward-poll.sh --explicit 2026-04-15 2026-04-21 [ENTITY]
#                                                                # legacy explicit-date mode
#
# Cost: e2-small for ~5 min per run = ~$0.004 per fire. Delete on completion.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Strip --env <val> from anywhere in the leading args without disturbing positional shape.
NEW_ARGS=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) NEW_ARGS+=("$1"); shift ;;
  esac
done
set -- "${NEW_ARGS[@]+"${NEW_ARGS[@]}"}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

USE_EXPLICIT=false
if [[ "${1:-}" == "--explicit" ]]; then
  USE_EXPLICIT=true
  shift
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if $USE_EXPLICIT; then
  if [[ $# -lt 2 ]]; then
    echo "ERROR: --explicit requires <START_DATE> <END_DATE> [ENTITY]" >&2
    exit 1
  fi
  START_DATE="$1"
  END_DATE="$2"
  SPORTS_ENTITY="${3:-}"
  if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
    exit 1
  fi
  RANGE_DESC="${START_DATE}..${END_DATE}"
else
  LOOKAHEAD="${1:-14}"
  SPORTS_ENTITY="${2:-}"
  if ! [[ "$LOOKAHEAD" =~ ^[0-9]+$ ]]; then
    echo "ERROR: lookahead must be non-negative integer (got $LOOKAHEAD)" >&2
    exit 1
  fi
  RANGE_DESC="rolling [today..today+${LOOKAHEAD}] UTC (force-window)"
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="footystats-fwd-${RUN_TS}"

echo "Launching $VM_NAME: FOOTYSTATS ${RANGE_DESC} entity=${SPORTS_ENTITY:-ALL}"

METADATA="VM_TASK=sports-forward-poll"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=sports"
if $USE_EXPLICIT; then
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
else
  METADATA="${METADATA},VM_LOOKBACK_DAYS=0"
  METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
  METADATA="${METADATA},VM_FORCE_WINDOW=true"
fi
METADATA="${METADATA},VM_SPORTS_PROVIDER=FOOTYSTATS"
[[ -n "$SPORTS_ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${SPORTS_ENTITY}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-small \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=footystats-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
