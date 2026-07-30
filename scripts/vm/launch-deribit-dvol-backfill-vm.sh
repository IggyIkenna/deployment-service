#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: temporary
# Delete-when: after the full 2021-03-24->now DVOL historical pull lands (one-off)
# Launch GCE VM for the Deribit DVOL (BTC/ETH implied-vol index) FULL historical
# batch backfill, via the `collect-deribit-volatility-index` MTDS operation
# (handler mtds@77ff475a, registered under vol_dvol_backtestable_engines_2026_07_13.md
# Todo 2). Feeds VOL_CARRY / VOL_ARB_RV_IV DVOL-backtestable strategy engines
# (Todo 3 of the same plan).
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Why e2-highmem-4 (not e2-standard-4): the handler's ManifestWriter.flush() reads +
# rewrites the shared cefi availability_index.parquet (~7.5M rows) on EVERY captured
# day — confirmed via a live 3-day connectivity test to spike to ~17GB RSS on this
# host. This is the SAME OOM signature already root-caused for other cefi MTDS
# backfills (mtds_backfill_vm_memory_hang_large_chunk_2026_07_22.md: anon-rss up to
# ~14.6GB, e2-standard-4's 16GB ceiling too tight) — e2-highmem-4 (32GB) is the
# established fix for this exact class, not a DVOL-specific workaround.
#
# ~1954 days (2021-03-24 -> today) x 2 currencies. DVOL data volume itself is tiny
# (hourly OHLC candles); the per-day manifest CAS-write is the real cost driver, and
# is expected to see retries under fleet-wide write contention on the shared index —
# per the plan's own connectivity-test note, that is NOT a reason to shrink scope.
# --batch-date-concurrency defaults to 3 here (vs the CLI's serial default of 1) to
# bound wall-clock without materially raising simultaneous manifest-flush pressure.
#
# Idempotent — the CLI's orchestrator skip-if-exists check means a SPOT-preempted
# relaunch of this exact command resumes cleanly (already-captured days are skipped).
#
# Usage:
#   bash launch-deribit-dvol-backfill-vm.sh
#   bash launch-deribit-dvol-backfill-vm.sh --dry-run
#   bash launch-deribit-dvol-backfill-vm.sh --start 2021-03-24 --end 2026-07-30
#   bash launch-deribit-dvol-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-4}"
DRY_RUN=false
# DVOL history starts 2021-03-24 (Deribit product launch) per the handler's
# _DVOL_COVERAGE_START constant.
START_DATE="${START_DATE:-2021-03-24}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
BATCH_DATE_CONCURRENCY="${BATCH_DATE_CONCURRENCY:-3}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper). --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --project)   PROJECT_ID="$2"; shift 2 ;;
    --zone)      ZONE="$2"; shift 2 ;;
    --start)     START_DATE="$2"; shift 2 ;;
    --end)       END_DATE="$2"; shift 2 ;;
    --batch-date-concurrency) BATCH_DATE_CONCURRENCY="$2"; shift 2 ;;
    --force)     FORCE=true; shift ;;
    --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand) ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="dvol-deribit-backfill"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "Deribit DVOL Full-History Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} -> ${END_DATE}"
echo "  Concurrency: ${BATCH_DATE_CONCURRENCY}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^dvol-deribit-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: DVOL backfill VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=deribit-dvol-backfill  VM_OPERATION=collect-deribit-volatility-index"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=deribit-dvol-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-deribit-volatility-index"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_BATCH_DATE_CONCURRENCY=${BATCH_DATE_CONCURRENCY}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
lc_verify_tarball_freshness "$CODE_BUCKET" \
    market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

lc_write_preemption_signal_file

# shellcheck disable=SC2086
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="250GB" --boot-disk-type="pd-balanced" \
  --labels="purpose=dvol-deribit-backfill,env=${DEPLOYMENT_ENV},managed-by=deployment-service" \
  --metadata="${METADATA}" \
  --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}"

lc_write_launch_params "$VM_NAME" "$PROJECT_ID" "launch-deribit-dvol-backfill-vm.sh" \
  "START_DATE=${START_DATE}" "END_DATE=${END_DATE}" "BATCH_DATE_CONCURRENCY=${BATCH_DATE_CONCURRENCY}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
