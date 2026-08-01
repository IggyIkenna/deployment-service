#!/usr/bin/env bash
# Epic: sports_manifest_canonicalisation
# Lifecycle: oneoff
# Delete-when: after sports v9 migration (E4-E8) is C-GREEN verified and legacy
#   buckets decommissioned (plan: sports_manifest_canonicalisation_2026_06_01.md § E4).
#
# E4: Year-sharded sports v9 migration VM launcher.
#
# Runs migrate_sports_canonical_v9.py (path re-partition) for ONE year-slice on
# ONE surface. Run once per (surface, year) — per-VM shard isolation prevents
# manifest race conditions.
#
# Two-phase execution on the VM:
#   Phase 1: migrate_sports_canonical_v9.py  -- re-partitions GCS objects (category→asset_group,
#             adds pipeline_mode= hive partition, lifts source path→col for instruments).
#   Phase 2: rebuild_sports_manifest_v9.py   -- rebuilds the availability _index with v9 schema,
#             typed empty reasons, available_at, source column.
#
# Idempotent backfill: SPOT VMs by default (~60-91% cheaper); preempted VMs just rerun
# the same year-shard cleanly (idempotent gcs_copy_object + manifest rebuild).
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
#
# Per-VM shard isolation: VM_NAME + MANIFEST_PER_VM_SHARDS=true ensures the manifest
# consolidator merges per-VM shards correctly (no concurrent _index writes).
#
# Gates: E3 drain must have run first (sports-scheduler stopped + _index snapshotted).
#        Only --apply after the E3 drain is confirmed.
#
# Usage (single VM):
#   bash launch-sports-v9-migration-vm.sh --surface mdps --year 2024 --dry-run
#   bash launch-sports-v9-migration-vm.sh --surface mdps --year 2024 --apply
#   bash launch-sports-v9-migration-vm.sh --surface instruments --year 2022 --apply
#
# Fleet invocation (all years, both surfaces — run AFTER E3 drain):
#   for YEAR in 2019 2020 2021 2022 2023 2024 2025 2026; do
#     bash launch-sports-v9-migration-vm.sh --surface mdps --year $YEAR --apply
#   done
#   for YEAR in 2019 2020 2021 2022 2023 2024 2025 2026; do
#     bash launch-sports-v9-migration-vm.sh --surface instruments --year $YEAR --apply
#   done
#
# Workers tuning: MDPS ~16k objects/30-day (sparse). Instruments ~119k objects/3-day
# (instrument_availability dominates). Start at --workers 32; tune up to 64 if 429
# rate stays <1% over the first year-shard dry-run timing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-highcpu-16}"
WORKERS="${WORKERS:-32}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ON_DEMAND="${ON_DEMAND:-false}"

SURFACE=""
YEAR=""
APPLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --surface)     SURFACE="$2"; shift 2 ;;
    --year)        YEAR="$2"; shift 2 ;;
    --workers)     WORKERS="$2"; shift 2 ;;
    --apply)       APPLY=true; shift ;;
    --dry-run)     APPLY=false; shift ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    --project-id)  PROJECT_ID="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${SURFACE}" ]]; then
  echo "ERROR: --surface required (mdps|instruments)" >&2; exit 1
fi
if [[ -z "${YEAR}" ]]; then
  echo "ERROR: --year required (e.g. 2024)" >&2; exit 1
fi

case "${SURFACE}" in
  mdps|instruments) ;;
  *) echo "ERROR: --surface must be mdps or instruments (got: ${SURFACE})" >&2; exit 1 ;;
esac

lc_validate_env "${DEPLOYMENT_ENV}"

START_DATE="${YEAR}-01-01"
END_DATE="${YEAR}-12-31"
RUN_TS=$(date -u +%Y%m%d-%H%M%S)
VM_NAME="sports-v9-migration-${SURFACE}-${YEAR}-${RUN_TS}"
CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

MIGRATOR_FLAGS="--surface ${SURFACE} --project-id ${PROJECT_ID} --start-date ${START_DATE} --end-date ${END_DATE} --workers ${WORKERS}"
REBUILDER_FLAGS="--surface ${SURFACE} --project-id ${PROJECT_ID}"
if $APPLY; then
  MIGRATOR_FLAGS="${MIGRATOR_FLAGS} --apply"
  REBUILDER_FLAGS="${REBUILDER_FLAGS} --no-dry-run"
fi

# instruments surface: also pass --legacy-bucket for legacy→prd reconciliation gate
if [[ "${SURFACE}" == "instruments" ]]; then
  MIGRATOR_FLAGS="${MIGRATOR_FLAGS} --legacy-bucket instruments-store-sports-${PROJECT_ID}"
fi

# MDPS rebuilder: cross-load FIXTURES truthset from instruments-store for step 6.5
if [[ "${SURFACE}" == "mdps" ]]; then
  REBUILDER_FLAGS="${REBUILDER_FLAGS} --fixtures-index-bucket instruments-store-sports-prd-${PROJECT_ID}"
fi

MIGRATE_CMD="python -u -m market_tick_data_service.scripts.migrate_sports_canonical_v9 ${MIGRATOR_FLAGS}"
REBUILD_CMD="python -u -m market_tick_data_service.scripts.rebuild_sports_manifest_v9 ${REBUILDER_FLAGS}"

MODE_LABEL=$($APPLY && echo "APPLY" || echo "DRY-RUN")

echo "============================================================"
echo "Sports v9 Migration VM — E4"
echo "  Surface:   ${SURFACE}"
echo "  Year:      ${YEAR} (${START_DATE}..${END_DATE})"
echo "  Mode:      ${MODE_LABEL}"
echo "  VM name:   ${VM_NAME}"
echo "  Workers:   ${WORKERS}"
echo "  Spot:      $([[ "${ON_DEMAND}" == "false" ]] && echo "yes (default)" || echo "no (on-demand)")"
echo "  Phase 1:   ${MIGRATE_CMD}"
echo "  Phase 2:   ${REBUILD_CMD}"
echo "============================================================"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if [[ "${ON_DEMAND}" == "true" ]]; then PROVISIONING_FLAGS=""; fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_TASK=sports-v9-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=migrate-sports-v9-${SURFACE}"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SURFACE=${SURFACE}"
METADATA="${METADATA},VM_MIGRATION_MODE=${MODE_LABEL}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# Two-phase execution: Phase 1 migrator, then Phase 2 rebuilder (sequential on VM)
METADATA="${METADATA},VM_MIGRATION_CMD=${MIGRATE_CMD} && ${REBUILD_CMD}"

# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT_ID}")" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  ${PROVISIONING_FLAGS} \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=sports-v9-migration,surface=${SURFACE},year=${YEAR},mode=$(echo "${MODE_LABEL}" | tr '[:upper:]' '[:lower:]'),env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "VM launched: ${VM_NAME}"
echo "  T+10 verify: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:        gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/syslog"
echo "  Delete:      gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet"
echo ""
echo "Fleet invocation pattern (all years, one surface):"
echo "  for YEAR in 2019 2020 2021 2022 2023 2024 2025 2026; do"
echo "    bash $0 --surface ${SURFACE} --year \$YEAR --apply"
echo "  done"
echo "============================================================"
