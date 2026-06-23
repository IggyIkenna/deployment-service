#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# 17-Entity Sports Reference Data Sweep
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first.
#
# Launches one VM per manifest entity type (17 VMs total), all in parallel.
# Each VM sweeps ALL dates from entity-start → today for exactly one entity.
# Manifest skip-logic fires in <1ms per date if entity is already present.
#
# API Football entities (9 VMs):
#   FIXTURES, LEAGUES, TEAMS, STANDINGS, INJURIES,
#   FIXTURE_STATS, FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS
#
# Enrichment provider entities (8 VMs):
#   FOOTYSTATS_MATCHES, FOOTYSTATS_PREDICTIONS,
#   SFI_LEAGUES, SFI_STANDINGS, TRANSFERMARKT_LEAGUES, TRANSFERMARKT_TEAMS,
#   UNDERSTAT_XG, WEATHER
#
# Usage:
#   bash launch-sports-entity-sweep-vm.sh                            # All 17 VMs
#   bash launch-sports-entity-sweep-vm.sh --dry-run                  # Print plan
#   bash launch-sports-entity-sweep-vm.sh --entity API_FOOTBALL_INJURIES  # Single entity
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
SINGLE_ENTITY=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
TODAY="$(date +%F)"
CHUNK_DAYS="${CHUNK_DAYS:-30}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)      DRY_RUN=true; shift ;;
    --entity)       SINGLE_ENTITY="$2"; shift 2 ;;
    --project)      PROJECT_ID="$2"; shift 2 ;;
    --zone)         ZONE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
    --chunk-days)   CHUNK_DAYS="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] [--entity <ENTITY>] [--project ID] [--zone Z] [--env prod|staging|dev]"
      exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

# Entity definitions: "vm-suffix|VM_VENUE|VM_SPORTS_ENTITY|start-date"
AF_ENTITIES=(
  "fixtures|API_FOOTBALL|API_FOOTBALL_FIXTURES|2019-01-01"
  "leagues|API_FOOTBALL|API_FOOTBALL_LEAGUES|2019-01-01"
  "teams|API_FOOTBALL|API_FOOTBALL_TEAMS|2019-01-01"
  "standings|API_FOOTBALL|API_FOOTBALL_STANDINGS|2019-01-01"
  "injuries|API_FOOTBALL|API_FOOTBALL_INJURIES|2021-01-01"
  "fixture-stats|API_FOOTBALL|API_FOOTBALL_FIXTURE_STATS|2019-01-01"
  "fixture-events|API_FOOTBALL|API_FOOTBALL_FIXTURE_EVENTS|2019-01-01"
  "fixture-lineups|API_FOOTBALL|API_FOOTBALL_FIXTURE_LINEUPS|2019-01-01"
  "player-stats|API_FOOTBALL|API_FOOTBALL_PLAYER_STATS|2019-01-01"
)
ENRICHMENT_ENTITIES=(
  "footystats-matches|FOOTYSTATS|FOOTYSTATS_MATCHES|2019-01-01"
  "footystats-predictions|FOOTYSTATS|FOOTYSTATS_PREDICTIONS|2019-01-01"
  "sfi-leagues|SOCCER_FOOTBALL_INFO|SFI_LEAGUES|2019-01-01"
  "sfi-standings|SOCCER_FOOTBALL_INFO|SFI_STANDINGS|2019-01-01"
  "transfermarkt-leagues|TRANSFERMARKT|TRANSFERMARKT_LEAGUES|2019-01-01"
  "transfermarkt-teams|TRANSFERMARKT|TRANSFERMARKT_TEAMS|2019-01-01"
  "understat|UNDERSTAT|UNDERSTAT_XG|2019-01-01"
  "weather|OPEN_METEO|WEATHER|2019-01-01"
)
ALL_ENTITIES=("${AF_ENTITIES[@]}" "${ENRICHMENT_ENTITIES[@]}")

if [[ -n "${SINGLE_ENTITY}" ]]; then
  FILTERED=()
  for entry in "${ALL_ENTITIES[@]}"; do
    IFS='|' read -r vm_suffix venue entity start <<< "${entry}"
    if [[ "${entity}" == "${SINGLE_ENTITY}" || "${venue}" == "${SINGLE_ENTITY}" || "${vm_suffix}" == "${SINGLE_ENTITY}" ]]; then
      FILTERED+=("${entry}")
    fi
  done
  if [[ ${#FILTERED[@]} -eq 0 ]]; then
    echo "No entity matched: ${SINGLE_ENTITY}" >&2; exit 1
  fi
  ALL_ENTITIES=("${FILTERED[@]}")
fi

echo "============================================================"
echo "Sports Entity Sweep VM Fleet (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  Range:    per-entity start → ${TODAY}"
echo "  VMs:      ${#ALL_ENTITIES[@]}"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  DryRun:   ${DRY_RUN}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

launch_vm() {
  local VM_SUFFIX="$1"
  local VM_VENUE="$2"
  local VM_SPORTS_ENTITY="$3"
  local START_DATE="$4"
  local VM_NAME="sports-entity-${VM_SUFFIX}"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create: ${VM_NAME}"
    echo "            VM_VENUE=${VM_VENUE}  VM_SPORTS_ENTITY=${VM_SPORTS_ENTITY}  ${START_DATE}→${TODAY}"
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
  METADATA="${METADATA},VM_VENUE=${VM_VENUE}"
  METADATA="${METADATA},VM_SPORTS_ENTITY=${VM_SPORTS_ENTITY}"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${TODAY}"

  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB \
    --labels="purpose=sports-entity-sweep,env=${DEPLOYMENT_ENV}" \
    --metadata="${METADATA}"
  echo "  Created: ${VM_NAME} (${VM_SPORTS_ENTITY} ${START_DATE}→${TODAY})"
  echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
}

PIDS=()
for entry in "${ALL_ENTITIES[@]}"; do
  IFS='|' read -r vm_suffix venue entity start_date <<< "${entry}"
  launch_vm "${vm_suffix}" "${venue}" "${entity}" "${start_date}" &
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
  echo "All ${#ALL_ENTITIES[@]} entity VMs launched."
fi
echo "  Monitor: gcloud compute instances list --filter='name~sports-entity-' --project=${PROJECT_ID}"
echo "  Logs:    gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep sports-entity"
echo "============================================================"
