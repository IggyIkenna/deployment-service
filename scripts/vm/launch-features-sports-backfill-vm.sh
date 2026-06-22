#!/usr/bin/env bash
# DEPRECATION NOTE (2026-05-08, Phase 8A of features_repo_consolidation_2026_05_08):
# For NEW single-cell features-sports backfills use the consolidated launcher:
#   bash launch-features-vm.sh --feature-family sports --asset-group SPORTS \
#       --start-date YYYY-MM-DD --end-date YYYY-MM-DD --launch-mode full
# This launcher is preserved for the singleton-lock + --skip-existing semantics
# the consolidated launcher does not model. Will be archived alongside the
# legacy features-sports-service repo when Phase 7 lands.
#
# Launch a GCE VM that backfills FIXTURE_FEATURES (per-fixture denormalised
# join of Transfermarkt team value, pre-match standings, kickoff-hour
# weather) via features-sports-service batch compute. Same code path as the
# daily Cloud Run job + per-fixture Tier-3 trigger — dispatched to a VM so
# multi-year runs don't block a laptop.
#
# SSOT:
#   plans/active/features_sports_pipeline_deployment_2026_04_21.plan
#   plans/active/features_sports_denormalisation_pipeline_2026_04_21.plan
#
# Invocation inside the VM (VM_BACKFILL_CMD metadata; see
# setup-data-pipeline-vm.sh line 455 branch for features-backfill):
#
#   python -m features_sports_service \
#     --operation compute --mode batch --asset-group SPORTS \
#     --tables fixture_features \
#     --start-date $START_DATE --end-date $END_DATE
#
# Writes to
#   gs://features-sports-central-element-323112/features/by_date/day={D}/feature_group=fixture_features/...
# and records manifest rows (capture_status ∈ {captured, empty_confirmed,
# attempted_failed}) via ManifestWriter from batch_handler.
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - All upstream providers (API_FOOTBALL / TRANSFERMARKT / SFI / OPEN_METEO)
#     must already have data for the target window, otherwise the rows will
#     materialise as empty_confirmed (honest-coverage — not attempted_failed).
#
# Usage:
#   bash launch-features-sports-backfill-vm.sh 2018-01-01 2026-04-20
#   bash launch-features-sports-backfill-vm.sh --skip-existing 2018-01-01 2026-04-20
#   bash launch-features-sports-backfill-vm.sh --force 2018-01-01 2026-04-20
#
# --skip-existing: skip dates where fixture_features parquet already exists
#                  in GCS (safe resume after VM restart).
# --force:         bypass singleton lock (same-prefix fs-backfill-* VM lock).
#
# Singleton lock: refuses to launch if any fs-backfill-* VM is already running
# in the zone. features-sports-service batch_handler reads from several GCS
# buckets concurrently per-date and can pressure the network when many VMs
# race. One VM per window is sufficient — the bottleneck is pandas joins per
# day, not network fanout. Pass --force only when you have a genuinely
# disjoint reason (e.g. two different feature groups / two different
# non-overlapping date windows).
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
SKIP_EXISTING=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --skip-existing) SKIP_EXISTING=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
Usage: bash launch-features-sports-backfill-vm.sh [--force] [--skip-existing] <START_DATE> <END_DATE>

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).

Examples:
  bash launch-features-sports-backfill-vm.sh 2018-01-01 2026-04-20
  bash launch-features-sports-backfill-vm.sh --skip-existing 2018-01-01 2026-04-20
EOF
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# Singleton lock — prevent duplicate fs-backfill-* VMs in the zone.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^fs-backfill-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: features-sports backfill VM already running in $ZONE: $EXISTING

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

# Compose the in-VM command. features-backfill branch of setup-data-pipeline-vm.sh
# substitutes `python ` → the per-tarball venv python.
BACKFILL_CMD="python -m features_service.sports"
BACKFILL_CMD="${BACKFILL_CMD} --operation compute --mode batch --asset-group SPORTS"
BACKFILL_CMD="${BACKFILL_CMD} --tables fixture_features"
BACKFILL_CMD="${BACKFILL_CMD} --start-date ${START_DATE} --end-date ${END_DATE}"
if $SKIP_EXISTING; then
  BACKFILL_CMD="${BACKFILL_CMD} --skip-existing"
fi

echo "Launching $VM_NAME: features-sports FIXTURE_FEATURES backfill ${START_DATE}..${END_DATE}"
echo "  cmd: ${BACKFILL_CMD}"

METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=compute"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=features-sports-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/features-backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
