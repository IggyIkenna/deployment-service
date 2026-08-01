#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
#   bash launch-measure-honest-coverage-vm.sh --machine-type e2-standard-4  # override machine type
#                                                                     # (default below; used for the
#                                                                     # 16GB-vs-32GB right-sizing verification —
#                                                                     # see honest_coverage_nightly_cron_undersized_and_launcher_ssot_drift_2026_07_16.md)
#   bash launch-measure-honest-coverage-vm.sh --oom-monitor           # opt-in ps/free/dmesg peak-RSS
#                                                                     # capture (oom-hang-monitor.sh) for a
#                                                                     # right-sizing verification run
#
# Cost: e2-highmem-4 (4 vCPU / 32 GiB) for ~5-15 minutes depending on manifest sizes.
# THIS is the nightly-cron launcher — Cloud Scheduler `honest-coverage-daily` (00:30 UTC)
# → Cloud Run Job `honest-coverage-daily-launcher` fetches THIS file from
# gs://deployment-scripts-central-element-323112/vm/ and runs it (NOT
# launch-honest-coverage-vm.sh). Re-upload via create-code-tarballs.sh after any edit.
#
# Right-sizing history + CURRENT rationale (2026-07-16, plan
# data_status_page_ux_and_canonicalisation_2026_07_16 P1): the per-AG manifest loads
# (cefi availability_index ~35.8M rows) OOM-killed even a 32 GiB box PRE the eu-only
# secondary read; the eu-only pushdown (measure_honest_coverage._read_parquet_eu_only)
# now bounds the oracle read to ~4.1M eu rows, and a manual e2-highmem-4 (32 GiB) run
# measured ALL 5 asset groups on 2026-07-16. The 2026-06-16 downsize to e2-standard-4
# (16 GiB) cited a column-pruned reader that was NEVER shipped (the writer still reads
# instrument_id/instrument_type) — 16 GiB empirically OOM'd most AGs, so the nightly
# wrote 1-AG partial coverage.json for weeks. Reverting to the PROVEN 32 GiB. A real
# column-prune (plan DATA P2) would let this drop back to 16 GiB.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
ASSET_GROUP="all"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
MACHINE_TYPE="e2-highmem-4"
OOM_MONITOR=false

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force)
      FORCE=true
      shift
      ;;
    --env)
      DEPLOYMENT_ENV="$2"
      shift 2
      ;;
    --machine-type)
      MACHINE_TYPE="$2"
      shift 2
      ;;
    --oom-monitor)
      OOM_MONITOR=true
      shift
      ;;
    cefi|defi|tradfi|sports|prediction|all)
      ASSET_GROUP="$1"
      shift
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      echo "Usage: $0 [--force] [--env prod|staging|dev] [--machine-type TYPE] [--oom-monitor] [cefi|defi|tradfi|sports|prediction|all]" >&2
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
CODE_BUCKET="deployment-scripts-${PROJECT}"

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
  Force:    bash $0 --force [args]

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
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
if $OOM_MONITOR; then
  METADATA="${METADATA},VM_OOM_MONITOR=true"
fi

echo "Launching $VM_NAME: measure-honest-coverage asset_group=${ASSET_GROUP} env=${DEPLOYMENT_ENV} machine_type=${MACHINE_TYPE}"

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
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=measure-honest-coverage,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service,machine-type="${MACHINE_TYPE//./-}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Asset group: $ASSET_GROUP"
echo ""
OUTPUT_DATE="$(date +%Y-%m-%d)"
echo "Output (when complete): gsutil cat gs://${PROJECT}-honest-coverage/${OUTPUT_DATE}/coverage.json"
echo "Events: gsutil ls gs://${PROJECT}-events/events/instruments-service/$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
