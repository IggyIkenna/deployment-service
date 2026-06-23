#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that forward-polls SoccerFootball.info (SFI / SharpAPI).
#
# Purpose: ingest SFI reference data (sfi_leagues + progressive_stats) for a given
# date window. Writes to:
#   gs://instruments-store-sports-central-element-323112/sports_reference/
#     by_date/day={D}/entity=sfi_leagues/sfi_leagues.parquet
#     by_date/day={D}/entity=progressive_stats/progressive_stats.parquet
#
# Runs the unified instruments-service CLI — same code path as Cloud Run T+1
# recon, just dispatched to a GCE VM. No standalone script; no ad-hoc code.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider SOCCER_FOOTBALL_INFO \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Default: polls yesterday (T-1) only. Passes no future dates because SFI
# progressive stats require completed matches.
#
# Prerequisites:
#   - Tarballs uploaded to gs://deployment-scripts-central-element-323112/code/
#   - soccer-football-info-api-key in Secret Manager
#
# Usage:
#   bash launch-sfi-forward-poll.sh                      # yesterday only (T-1)
#   bash launch-sfi-forward-poll.sh 2026-04-15 2026-04-18  # explicit window
#   bash launch-sfi-forward-poll.sh --force 2026-04-15 2026-04-18  # bypass singleton lock
#
# Cost: e2-standard-2 for ~5-15 min per run.
#
# Singleton lock: refuses to launch if any sfi-fwd-* VM is already running in
# the zone. SFI/RapidAPI rate-limits per-API-key, so 10 concurrent VMs sharing
# `soccer-football-info-api-key` thrash on 429 (proven in 2026-04-19 incident:
# 10 VMs ran for ~6h producing ~4 successful writes total). Pass --force to
# bypass for legitimate parallel investigations.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Strip --force / --env <val> from anywhere in args without disturbing positional shape.
NEW_ARGS=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) NEW_ARGS+=("$1"); shift ;;
  esac
done
set -- "${NEW_ARGS[@]+"${NEW_ARGS[@]}"}"

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

# ── Singleton lock: 10 concurrent VMs sharing one API key thrash on 429s ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sfi-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: SFI VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — SFI/RapidAPI rate-limits per-key and
multiple concurrent VMs thrash on 429s without producing useful data.

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
VM_NAME="sfi-fwd-${RUN_TS}"

echo "Launching $VM_NAME: SFI (SOCCER_FOOTBALL_INFO) ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=sports-forward-poll"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SPORTS_PROVIDER=SOCCER_FOOTBALL_INFO"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-2 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=sfi-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
