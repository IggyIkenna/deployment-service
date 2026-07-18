#!/usr/bin/env bash
# Epic: predictions_master
# Lifecycle: campaign
# Delete-when: Kalshi deep-history (2021→2026-02-05 bulk snapshot) is seeded to canonical + manifest v9
#   (the one-off seed VM has no recurring role afterwards).
#
# Launch a GCE VM that seeds Kalshi deep history into the CANONICAL prediction store from the free
# Jon-Becker bulk corpus (operator 2026-06-20, option-b path). Reuses VM_TASK=canonical-migration so the
# generic setup-data-pipeline-vm.sh installs the venv + UTL/UAC + env, then runs the on-VM runner
# market_tick_data_service/scripts/run_kalshi_bulk_seed.sh which: download+extract the kalshi subset →
# PARITY-GATE one day (bulk vs the live /historical API) → full-range convert → rebuild v9 manifest.
#
# Big boot disk: the corpus is a single ~33.5GB tar.zst; extracted kalshi subset + working space → 250GB.
#
# Usage:
#   bash launch-kalshi-bulk-seed-vm.sh [PARITY_DAY] [--env prod] [--dry-run]
#     PARITY_DAY (default 2026-01-15) must be in BOTH the bulk snapshot (≤2026-02-05) and the API
#     /historical tier (<2026-04-21 cutoff) so the parity gate can compare both paths.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

PARITY_DAY="2026-01-15"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN="false"
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN="true" ;;
    --env) :;;  # handled below
    --env=*) DEPLOYMENT_ENV="${arg#*=}" ;;
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) PARITY_DAY="$arg" ;;
  esac
done
# support "--env prod" (two-token form)
prev=""
for arg in "$@"; do [[ "$prev" == "--env" ]] && DEPLOYMENT_ENV="$arg"; prev="$arg"; done

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-8"   # bulk decompress + pyarrow scans over the chunk glob
BOOT_DISK_GB="250"             # ~33.5GB tar.zst + extracted kalshi subset + headroom
RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="mtds-prediction-kalshibulk-${RUN_TS}"   # stays under the known mtds-prediction- prefix

RUNNER="bash market_tick_data_service/scripts/run_kalshi_bulk_seed.sh ${PARITY_DAY}"

echo "Launching $VM_NAME: Kalshi bulk→canonical seed (parity day ${PARITY_DAY}, env ${DEPLOYMENT_ENV})"

# canonical-migration route: setup-data-pipeline-vm.sh runs $VM_MIGRATION_CMD cd'd into $WORKSPACE/mtds
# via _launch_with_tee (gcs-tee log + heartbeat + STOPPED-on-exit). The runner owns venv via $VENV.
METADATA="VM_TASK=canonical-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=PREDICTION"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_MIGRATION_CMD=${RUNNER}"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY-RUN] Would create $VM_NAME (machine=$MACHINE_TYPE disk=${BOOT_DISK_GB}GB)"
  echo "[DRY-RUN] VM_MIGRATION_CMD=${RUNNER}"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=kalshi-bulk-seed,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Inspect:  gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "Delete:   gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
