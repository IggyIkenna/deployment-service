#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Migrated 2026-06-22 (features_repo_consolidation finalisation): this launcher
# now invokes the CONSOLIDATED entry-point
# `features_service.sports.scripts.compute_sfi_progressive_only` (moved into the
# `features_service` package so it is `-m`-runnable against the installed wheel —
# the top-level repo `scripts/` dir is NOT shipped in hatch's wheel). The prior
# `features_sports_service.scripts.compute_sfi_progressive_only` module path
# pulled the STALE `features-sports-service-code` tarball (archived repo, predates
# the feature_family=sports fix) → MissingFeatureFamilyError. VM_SERVICE is now
# `features_service` so the FRESH `features-service-code` tarball is installed.
#
# Launch a single GCE VM that backfills sfi_progressive halftime features
# for the entire SFI coverage window via features-sports-service.
#
# Phase 0.6 of features_sports_honest_coverage_2026_05_05.plan.
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
#   python -m features_service.sports.scripts.compute_sfi_progressive_only \
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
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND="${ON_DEMAND:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

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
# Canonical bucket (the legacy flat `features-sports-${PROJECT}` twin was
# deleted 2026-07-21 by the bucket_estate_consolidation_to_sub100 migration —
# see migrate_features_sports_flat_bucket_gap_2026_07_15.py in features-service,
# which migrated this launcher's only real prior output, the day=2020-01-01
# sfi_progressive cell, into this bucket). Matches the sibling
# launch-features-sports-parallel-backfill-vm.sh's GCS_BUCKET default.
BUCKET="features-sports-prd-${PROJECT}"

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
VM_NAME="features-sfi-progressive-${RUN_TS}"

# Compose the in-VM command. features-backfill branch of setup-data-pipeline-vm.sh
# substitutes `python ` → the per-tarball venv python.
# RECOMPUTE_FORCE (env var) toggles the script's ``--force`` flag — set when
# the operator wants to overwrite prior captured manifest rows after a
# calculator fix. Default off so the script's read-once + TTL skip-cache
# works for safe resumes after crash.
BACKFILL_CMD="python -m features_service.sports.scripts.compute_sfi_progressive_only"
BACKFILL_CMD="${BACKFILL_CMD} --start-date ${START_DATE} --end-date ${END_DATE}"
BACKFILL_CMD="${BACKFILL_CMD} --bucket ${BUCKET}"
if [[ "${RECOMPUTE_FORCE:-false}" == "true" ]]; then
  BACKFILL_CMD="${BACKFILL_CMD} --force"
fi

METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=compute"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME: features-sports SFI progressive halftime backfill ${START_DATE}..${END_DATE} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  bucket: ${BUCKET}"
echo "  cmd:    ${BACKFILL_CMD}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=features-sfi-progressive,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

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
