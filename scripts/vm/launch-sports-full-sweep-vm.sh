#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Full API Football sweep — covers ALL entities for ALL dates 2019-01-01 → today
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first.
#
# 8 year-chunk VMs in parallel (one per calendar year).
# Each VM: entity-level manifest checking → skips already-done API calls.
# Covers ALL API_FOOTBALL entities: fixtures, leagues, teams, standings,
# injuries, fixture_stats, fixture_events, fixture_lineups, player_stats.
#
# Usage:
#   bash launch-sports-full-sweep-vm.sh                # Launch all 8 VMs
#   bash launch-sports-full-sweep-vm.sh --dry-run      # Print plan only
#   bash launch-sports-full-sweep-vm.sh --env staging  # Staging env tier
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
# 32GB default: the IS fixtures catalogue + per-fixture footprint OOM-kills
# e2-standard-2 (8GB). SSOT: plans/active/sports_reference_backfill_oom_2026_06_22.md
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
CHUNK_DAYS="${CHUNK_DAYS:-30}"
# Optional: restrict the sweep to ONE sports entity (e.g. FIXTURES). When set,
# only that entity is fetched per date — used for the fixtures-first phase so the
# catalogue (and the odds expected-universe it gates) fills WITHOUT the heavy
# per-fixture enrichment fan-out (player-stats/lineups) that rate-limits the API.
# Empty (default) = all entities per date. Phase-2 enrichment entities read
# fixture IDs from GCS (_per_fixture_gcs_fast_path), so fixtures-first composes.
ENTITY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)    DRY_RUN=true; shift ;;
    --project)    PROJECT_ID="$2"; shift 2 ;;
    --zone)       ZONE="$2"; shift 2 ;;
    --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
    --chunk-days) CHUNK_DAYS="$2"; shift 2 ;;
    --entity)     ENTITY="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

# Year-chunked ranges (one VM per year)
RANGES=(
  "sports-full-sweep-2019|2019-01-01|2019-12-31"
  "sports-full-sweep-2020|2020-01-01|2020-12-31"
  "sports-full-sweep-2021|2021-01-01|2021-12-31"
  "sports-full-sweep-2022|2022-01-01|2022-12-31"
  "sports-full-sweep-2023|2023-01-01|2023-12-31"
  "sports-full-sweep-2024|2024-01-01|2024-12-31"
  "sports-full-sweep-2025|2025-01-01|2025-12-31"
  "sports-full-sweep-2026|2026-01-01|2026-04-10"
)

echo "============================================================"
echo "Sports Full Sweep VM Fleet (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VMs:      ${#RANGES[@]} (one per year, parallel)"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  DryRun:   ${DRY_RUN}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

launch_vm() {
  local VM_NAME="$1"
  local START_DATE="$2"
  local END_DATE="$3"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME} (${START_DATE} → ${END_DATE})"
    return 0
  fi

  # Delete existing VM (idempotent fleet pattern)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  local METADATA
  METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  METADATA="${METADATA},VM_TASK=instruments-backfill"
  METADATA="${METADATA},VM_SERVICE=instruments_service"
  METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
  METADATA="${METADATA},VM_VENUE=API_FOOTBALL"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
  [[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"

  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --labels="purpose=sports-full-sweep,env=${DEPLOYMENT_ENV}" \
    --metadata="${METADATA}"
  echo "  Created: ${VM_NAME} (${START_DATE} → ${END_DATE})"
  echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
}

PIDS=()
for entry in "${RANGES[@]}"; do
  IFS='|' read -r VM_NAME START_DATE END_DATE <<< "${entry}"
  launch_vm "${VM_NAME}" "${START_DATE}" "${END_DATE}" &
  PIDS+=($!)
done

FAILED=0
for pid in "${PIDS[@]}"; do
  if ! wait "${pid}"; then
    FAILED=$((FAILED + 1))
  fi
done

echo ""
echo "============================================================"
if [[ ${FAILED} -gt 0 ]]; then
  echo "WARNING: ${FAILED} VM creation(s) failed"
else
  echo "All ${#RANGES[@]} VMs launched."
fi
echo "  Monitor: gcloud compute instances list --filter='name~sports-full-sweep' --project=${PROJECT_ID}"
echo "  Logs:    gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep sports-full-sweep"
echo "============================================================"
