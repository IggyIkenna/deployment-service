#!/usr/bin/env bash
# Epic: data_completion_to_100_all_ag
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that forward-polls Understat xG / shot data for a short
# rolling window. Intended for the weekly cron (or manual re-enrichment) that
# keeps XG current as match dates roll forward. Understat is scrape-only with
# FT+2h latency; the forward poll uses lookback=2 by default so freshly
# completed matches have time to be enriched before the shard is attempted.
#
# Forward-poll uses a lighter machine (e2-standard-2) and always enables
# --force-window so the rolling window re-fetches even if rows already exist.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group sports \
#     --sports-provider UNDERSTAT --sports-entity XG \
#     --lookback-days $VM_LOOKBACK_DAYS --lookahead-days $VM_LOOKAHEAD_DAYS \
#     [--force-window]
#
# Leagues covered: EPL, La Liga, Bundesliga, Serie A, Ligue 1 (RFPL optional).
# SSOT: codex/02-data/sports-scheduling-and-sharding.md §2.6 (Tier-4 T+24h).
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#
# Usage:
#   bash launch-understat-forward-poll.sh
#   bash launch-understat-forward-poll.sh --lookback 3 --lookahead 2
#   bash launch-understat-forward-poll.sh --entity XG
#   bash launch-understat-forward-poll.sh --dry-run
#
# --entity XG
#   Restricts the VM to xG / shot data. Currently XG is the only valid entity
#   for understat forward polls. Omit to use the default (XG).
#
# Singleton lock: refuses to launch if any us-forward-poll-* VM is already
# running in the zone (AJAX scrape; per-IP rate limits). Pass --force to
# override.
# Bucket-naming SSOT: env-aware shape per bucket_name_ssot_canonicalisation_2026_05_10.md.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

# e2-standard-2 (8 GB) is sufficient for incremental forward polling.
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"

FORCE=false
ENTITY=""
# Default rolling window: 2 days lookback (covers FT+2h enrichment lag),
# 1 day lookahead — keeps current xG populated without over-fetching.
LOOKBACK="${LOOKBACK:-2}"
LOOKAHEAD="${LOOKAHEAD:-1}"
# Force-window is ON by default for forward polls: re-fetches even if rows exist.
FORCE_WINDOW="${FORCE_WINDOW:-true}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --no-force-window) FORCE_WINDOW=false; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "ERROR: Unknown argument: $1" >&2; exit 1 ;;
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

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^us-forward-poll-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Understat forward-poll VM already running in $ZONE: $EXISTING
Refusing duplicate launch — AJAX scrape per-IP rate limits.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ...

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="us-forward-poll-${RUN_TS}"

ENTITY_DESC="entity=XG (default)"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY only"
RANGE_DESC="rolling [today-${LOOKBACK}..today+${LOOKAHEAD}] UTC"
echo "Launching $VM_NAME: UNDERSTAT forward-poll ${RANGE_DESC} ($ENTITY_DESC)"

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=sports"
METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
[[ "$FORCE_WINDOW" == "true" ]] && METADATA="${METADATA},VM_FORCE_WINDOW=true"
METADATA="${METADATA},VM_SPORTS_PROVIDER=UNDERSTAT"
METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY:-XG}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: ${VM_NAME}"
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
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=understat-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
