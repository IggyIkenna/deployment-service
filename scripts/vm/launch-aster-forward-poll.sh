#!/usr/bin/env bash
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a short-lived GCE VM that forward-polls Aster Futures perp data.
#
# Purpose: ingest one day (T-1 by default) of Aster derivative_ticker (funding
# rates + mark price) and trades (aggTrades) for BTC + ETH USDT-margined perps.
# Writes to:
#   gs://market-data-tick-cefi-central-element-323112/raw_tick_data/by_date/
#     day={D}/asset_group=cefi/venue=ASTER/instrument_type=perpetual/
#       data_type={trades,derivative_ticker}/{symbol}.parquet
#
# Runs the unified MTDS CLI — same code path as the historical Aster backfill,
# just scoped to a one-day window. No standalone script; no ad-hoc code.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m market_tick_data_service \
#     --operation download --mode batch --asset-group CEFI \
#     --venues ASTER \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE \
#     --data-types trades book_snapshot_5 derivative_ticker liquidations \
#     --instrument-ids BTC ETH \
#     --force-window
#
# Default: polls T-1 (yesterday). aggTrades only return ~1k rows per
# fromId-paged window, so a calendar day is fully covered in a single VM run
# typically <2 minutes for BTC + ETH on Aster's current volume.
#
# Usage:
#   bash launch-aster-forward-poll.sh                       # yesterday only (T-1)
#   bash launch-aster-forward-poll.sh 2026-04-15 2026-04-18  # explicit window
#   bash launch-aster-forward-poll.sh --force 2026-04-15 2026-04-18  # bypass singleton
#
# Cost: e2-standard-2 for ~3-10 min per run.
#
# Singleton lock: refuses to launch if any aster-fwd-* VM is already running
# in the zone. Aster's REST API has a public 1200 req/min rate limit per IP,
# and two concurrent VMs from the same project share an external IP pool that
# can collide on 429s. Pass --force for legitimate parallel investigations.
set -euo pipefail

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

# Parse optional flags (--force / --env) while preserving positional dates.
_positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Default: yesterday only (T-1). Pass two dates for an explicit window.
if [[ $# -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
  END_DATE="$START_DATE"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# ── Singleton lock ──────────────────────────────────────────────────────────
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^aster-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: Aster forward-poll VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Aster's REST API rate-limits per-IP and two
concurrent VMs sharing a project's external IP pool may collide on 429s
without producing useful additional data.

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
VM_NAME="aster-fwd-${RUN_TS}"

echo "Launching $VM_NAME: ASTER ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_VENUE=ASTER"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_DATA_TYPES=trades;derivative_ticker"
METADATA="${METADATA},VM_INSTRUMENT_IDS=BTC;ETH"
METADATA="${METADATA},VM_FORCE_WINDOW=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
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
  --labels=purpose=aster-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:    gcloud compute ssh $VM_NAME --zone=$ZONE --command 'sudo tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete:  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "For daily scheduling: wire to Cloud Scheduler with target=Compute Engine"
echo "  → /v1/projects/${PROJECT}/zones/${ZONE}/instances (POST) using this same metadata."
