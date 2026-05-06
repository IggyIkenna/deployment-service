#!/usr/bin/env bash
# Launch a single GCE VM that backfills sfi_progressive halftime features
# for the entire SFI coverage window via features-sports-service.
#
# Phase 0.6 of features_sports_honest_coverage_2026_05_05.plan.md.
#
# Why standalone (not part of full derived_features):
#   sfi_progressive features are computed from already-captured SFI
#   progressive_stats parquets — no upstream backfill needed. Running just
#   this calculator across 2020-01-01 → today on one e2-standard-4 VM
#   finishes in ~30-45 min vs ~37 hours for full derived_features. Get
#   halftime data unblocked before Phase 1 starts.
#
# Invocation inside the VM (VM_BACKFILL_CMD metadata; setup-data-pipeline-vm.sh
# features-backfill branch substitutes `python ` → venv python):
#
#   python -m features_sports_service.scripts.compute_sfi_progressive_only \
#     --start-date $START_DATE --end-date $END_DATE \
#     --bucket $BUCKET
#
# Output:
#   gs://features-sports-{pid}/sports_features/by_date/day={D}/
#     feature_group=sfi_progressive/sfi_progressive.parquet
#
# Manifest:
#   per-VM shards under
#     gs://features-sports-{pid}/_catalogue/_index/per_vm/{VM_NAME}.parquet
#   merged by manifest-consolidator into canonical
#     _catalogue/_index/availability_index.parquet.
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - SFI progressive_stats already captured (96.5% as of 2026-05-05).
#
# Usage:
#   bash launch-sfi-progressive-features-backfill-vm.sh                          # full SFI window
#   bash launch-sfi-progressive-features-backfill-vm.sh 2024-01-01 2024-12-31    # explicit range
#   bash launch-sfi-progressive-features-backfill-vm.sh --force 2020-01-01 2026-05-05
#
# Singleton lock: refuses to launch if any features-sfi-progressive-* VM is
# already running in the zone (the SFI progressive_stats parquets are read
# concurrently per-day; one VM per window is sufficient and avoids manifest-
# CAS contention even with per-VM shards). --force bypasses for legitimate
# parallel investigations.
#
# VM naming: features-sfi-progressive-{TS} — covered by the heartbeat-only
# `features-` prefix entry in vm_zombie_watchdog.VM_PREFIX_TO_BUCKET.
set -euo pipefail

FORCE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    *) break ;;
  esac
done

DEFAULT_START="2020-01-01"
DEFAULT_END="$(date -u +%Y-%m-%d)"

if [[ $# -eq 0 ]]; then
  START_DATE="$DEFAULT_START"
  END_DATE="$DEFAULT_END"
elif [[ $# -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  cat >&2 <<EOF
Usage: bash launch-sfi-progressive-features-backfill-vm.sh [--force] [<START_DATE> <END_DATE>]

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  Defaults to ${DEFAULT_START} .. today (full SFI coverage window).
EOF
  exit 1
fi

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
BUCKET="features-sports-${PROJECT}"

# Singleton lock — prevent duplicate features-sfi-progressive-* VMs in the zone.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^features-sfi-progressive-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: features-sfi-progressive backfill VM already running in $ZONE: $EXISTING

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Events:    gcloud storage ls gs://${PROJECT}-events/events/features-sports-service/
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ...
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="features-sfi-progressive-${RUN_TS}"

# Compose the in-VM command. features-backfill branch of setup-data-pipeline-vm.sh
# substitutes `python ` → the per-tarball venv python.
# RECOMPUTE_FORCE (env var) toggles the script's ``--force`` flag — set when
# the operator wants to overwrite prior captured manifest rows after a
# calculator fix. Default off so the script's read-once + TTL skip-cache
# works for safe resumes after crash.
BACKFILL_CMD="python -m features_sports_service.scripts.compute_sfi_progressive_only"
BACKFILL_CMD="${BACKFILL_CMD} --start-date ${START_DATE} --end-date ${END_DATE}"
BACKFILL_CMD="${BACKFILL_CMD} --bucket ${BUCKET}"
if [[ "${RECOMPUTE_FORCE:-false}" == "true" ]]; then
  BACKFILL_CMD="${BACKFILL_CMD} --force"
fi

echo "Launching $VM_NAME: features-sports SFI progressive halftime backfill ${START_DATE}..${END_DATE}"
echo "  bucket: ${BUCKET}"
echo "  cmd:    ${BACKFILL_CMD}"

METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=features_sports_service"
METADATA="${METADATA},VM_OPERATION=compute"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=features-sfi-progressive,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:   gcloud storage ls gs://${PROJECT}-events/events/features-sports-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "SSH:      gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verification protocol (no fire-and-forget):"
echo "  1. Wait 90s, then: gcloud storage ls gs://${PROJECT}-events/events/features-sports-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "  2. Confirm STARTED event in first JSONL"
echo "  3. Every 10-15min: confirm new PROGRESSIVE_DAY_* events"
echo "  4. At completion: confirm STOPPED event with detection-rate stats"
