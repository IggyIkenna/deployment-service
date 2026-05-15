#!/usr/bin/env bash
# Launch a same-region GCE VM that runs the cross-asset honest-coverage
# measurement script (instruments-service/scripts/measure_honest_coverage.py)
# and writes the result to gs://central-element-323112-honest-coverage/{date}/coverage.json.
#
# The resulting JSON is consumed by the deployment-api
# GET /api/data-status/honest-coverage endpoint (Phase 2C) and surfaced in
# the deployment-ui data-status tab (Phase 2D).
#
# SSOT: codex/03-deployment/data-status-ui-surface.md
# Plan: cross_asset_group_catalogue_audit_2026_05_10.md Phase 2B
#
# Singleton lock: refuses to launch if a measure-honest-coverage-* VM is
# already running (the measurement is cheap but concurrent runs race on the
# GCS output object, producing interleaved JSON). Use --force to bypass.
#
# VM_PREFIX_TO_BUCKET registration:
#   "measure-honest-coverage-" prefix is registered in
#   deployment-service/scripts/vm/vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET
#   pointing to None (no per-vm shard writes; output is project-level).
#
# Usage:
#   bash launch-measure-honest-coverage-vm.sh                        # all, prod
#   bash launch-measure-honest-coverage-vm.sh cefi                   # cefi only
#   bash launch-measure-honest-coverage-vm.sh --env staging          # staging manifests
#   bash launch-measure-honest-coverage-vm.sh --force                # bypass singleton lock
#
# Cost: e2-standard-2 for ~5-15 minutes depending on manifest sizes.
set -euo pipefail

FORCE=false
ASSET_GROUP="all"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=true
      shift
      ;;
    --env)
      DEPLOYMENT_ENV="$2"
      shift 2
      ;;
    cefi|defi|tradfi|sports|prediction|all)
      ASSET_GROUP="$1"
      shift
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      echo "Usage: $0 [--force] [--env prod|staging|dev] [cefi|defi|tradfi|sports|prediction|all]" >&2
      exit 1
      ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock ────────────────────────────────────────────────────────────
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^measure-honest-coverage-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: measure-honest-coverage VM already running in $ZONE: $EXISTING
Concurrent runs race on the GCS output JSON. Use --force to bypass.

  Inspect:  gcloud compute ssh $EXISTING --zone=$ZONE
  Stop:     gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:    bash $0 --force [args]
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="measure-honest-coverage-${RUN_TS}"

MEASURE_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/measure_honest_coverage.py"
BACKFILL_CMD="python ${MEASURE_SCRIPT} --asset-group ${ASSET_GROUP}"

# VM_TASK=features-backfill: setup script reads VM_BACKFILL_CMD verbatim (no CLI dispatch).
# VM_TASK=measure-honest-coverage was not registered in setup-data-pipeline-vm.sh and
# fell through to the default instruments-service CLI path which rejects --asset-group=all.
METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching $VM_NAME: measure-honest-coverage asset_group=${ASSET_GROUP} env=${DEPLOYMENT_ENV}"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=measure-honest-coverage,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Asset group: $ASSET_GROUP"
echo ""
OUTPUT_DATE="$(date +%Y-%m-%d)"
echo "Output (when complete): gsutil cat gs://${PROJECT}-honest-coverage/${OUTPUT_DATE}/coverage.json"
echo "Events: gsutil ls gs://${PROJECT}-events/events/instruments-service/$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
