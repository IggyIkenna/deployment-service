#!/usr/bin/env bash
# 17-Entity Sports Reference Data Sweep
#
# Launches one VM per manifest entity type (17 VMs total), all in parallel.
# Each VM sweeps ALL dates 2019-01-01 → today for exactly one entity.
# The manifest skip-logic fires in <1ms per date if the entity is already present.
#
# API Football entities (9 VMs, each with --sports-entity):
#   FIXTURES, LEAGUES, TEAMS, STANDINGS, INJURIES,
#   FIXTURE_STATS, FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS
#
# Enrichment provider entities (8 VMs, one per manifest entity key):
#   FOOTYSTATS_MATCHES, FOOTYSTATS_PREDICTIONS,
#   SFI_LEAGUES, SFI_STANDINGS, TRANSFERMARKT_LEAGUES, TRANSFERMARKT_TEAMS,
#   UNDERSTAT_XG, WEATHER
#
# Usage:
#   bash full_sports_entity_sweep.sh
#   bash full_sports_entity_sweep.sh --dry-run
#   bash full_sports_entity_sweep.sh --entity API_FOOTBALL_INJURIES   # single entity only
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
DRY_RUN=false
SINGLE_ENTITY=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
TODAY="$(date +%F)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --entity) SINGLE_ENTITY="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone) ZONE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] [--entity <ENTITY_NAME>] [--project ID] [--zone Z] [--env prod|staging|dev]"
      exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "${SCRIPT_DIR}/../common" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

GCS_BUCKET="gs://instruments-store-sports-${PROJECT_ID}"
GCS_STAGING="${GCS_BUCKET}/_vm_staging/entity_sweep"
TARBALL_NAME="instruments_entity_codebase.tar.gz"
TARBALL_PATH="/tmp/${TARBALL_NAME}"
GCS_TARBALL="${GCS_STAGING}/${TARBALL_NAME}"
GCS_SCRIPT="${GCS_STAGING}/vm_instruments_reference.sh"

# Entity definitions: "vm-suffix|--venues value|--sports-entity value|start-date"
# start-date is the earliest date the provider has data — derived from manifest.
# This avoids burning through years of empty API calls before real data begins.
# API Football entities — each gets --sports-entity to restrict to exactly one manifest key
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

# Enrichment provider entities (8 VMs) — one per manifest entity key
# Multi-entity providers split with --sports-entity so each VM handles exactly one entity
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

# Build entity list
ALL_ENTITIES=("${AF_ENTITIES[@]}" "${ENRICHMENT_ENTITIES[@]}")

# Filter to single entity if requested
if [[ -n "${SINGLE_ENTITY}" ]]; then
  FILTERED=()
  for entry in "${ALL_ENTITIES[@]}"; do
    entity_name="$(echo "${entry}" | cut -d'|' -f3)"
    venue_name="$(echo "${entry}" | cut -d'|' -f2)"
    suffix="$(echo "${entry}" | cut -d'|' -f1)"
    # Match by entity name, venue name, or vm suffix
    if [[ "${entity_name}" == "${SINGLE_ENTITY}" || \
          "${venue_name}" == "${SINGLE_ENTITY}" || \
          "${suffix}" == "${SINGLE_ENTITY}" ]]; then
      FILTERED+=("${entry}")
    fi
  done
  if [[ ${#FILTERED[@]} -eq 0 ]]; then
    echo "No entity matched: ${SINGLE_ENTITY}" >&2
    exit 1
  fi
  ALL_ENTITIES=("${FILTERED[@]}")
fi

echo "============================================================"
echo "Sports Entity Sweep (17 VMs)"
echo "  Project:    ${PROJECT_ID}"
echo "  Zone:       ${ZONE}"
echo "  Machine:    ${MACHINE_TYPE}"
echo "  Range:      per-entity start → ${TODAY}"
echo "  VMs:        ${#ALL_ENTITIES[@]}"
echo "  DryRun:     ${DRY_RUN}"
echo "============================================================"
echo ""

# ---- Step 1: Package codebase ONCE ----
echo "=== Step 1: Packaging codebase (once for all ${#ALL_ENTITIES[@]} VMs) ==="
REPOS=("unified-api-contracts" "unified-trading-library" "instruments-service")
if ! $DRY_RUN; then
  STAGING_DIR=$(mktemp -d)
  for repo in "${REPOS[@]}"; do
    echo "  Syncing ${repo}..."
    mkdir -p "${STAGING_DIR}/${repo}"
    rsync -a \
      --exclude='.git' \
      --exclude='.venv*' \
      --exclude='__pycache__' \
      --exclude='*.egg-info' \
      --exclude='node_modules' \
      --exclude='.mypy_cache' \
      --exclude='.pytest_cache' \
      --exclude='tests' \
      --exclude='htmlcov' \
      --exclude='.coverage*' \
      "${WORKSPACE_ROOT}/${repo}/" "${STAGING_DIR}/${repo}/"
  done
  echo "  Compressing..."
  (cd "${STAGING_DIR}" && tar czf "${TARBALL_PATH}" -- *)
  TARBALL_SIZE=$(du -h "${TARBALL_PATH}" | cut -f1)
  echo "  Tarball: ${TARBALL_PATH} (${TARBALL_SIZE})"
  rm -rf "${STAGING_DIR}"
else
  echo "  [DRY RUN] Would package tarball"
fi

# ---- Step 2: Upload to GCS ONCE ----
echo ""
echo "=== Step 2: Uploading to GCS ==="
if ! $DRY_RUN; then
  gsutil -q cp "${TARBALL_PATH}" "${GCS_TARBALL}"
  gsutil -q cp "${COMMON_DIR}/vm_instruments_reference.sh" "${GCS_SCRIPT}"
  rm -f "${TARBALL_PATH}"
  echo "  Done → ${GCS_STAGING}/"
else
  echo "  [DRY RUN] Would upload to ${GCS_STAGING}/"
fi

# ---- Step 3: Launch all VMs in parallel ----
echo ""
echo "=== Step 3: Launching ${#ALL_ENTITIES[@]} entity-scoped VMs ==="

launch_vm() {
  local vm_suffix="$1"
  local venues="$2"
  local sports_entity="$3"
  local start_date="$4"
  local vm_name="sports-entity-${vm_suffix}"

  local entity_label="${sports_entity:-${venues}}"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create: ${vm_name} entity=${entity_label} ${start_date}→${TODAY}"
    return 0
  fi

  STARTUP_FILE=$(mktemp)
  local entity_arg=""
  if [[ -n "${sports_entity}" ]]; then
    entity_arg="--sports-entity ${sports_entity}"
  fi

  cat > "$STARTUP_FILE" << STARTUP_EOF
#!/bin/bash
set -euo pipefail
export WORK_DIR=/tmp/instruments
export HOME=/root
export PATH="/root/.local/bin:\$PATH"

exec > >(tee /var/log/instruments-reference.log) 2>&1

export GCP_PROJECT_ID="${PROJECT_ID}"
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV}"
export CLOUD_PROVIDER=gcp
export CLOUD_MOCK_MODE=false
export DATA_MODE=real
export IS_TEST_RUN=false

echo "=== VM Startup: ${vm_name} ==="
echo "  Entity: ${entity_label}"
echo "  Range:  ${start_date} → ${TODAY}"
date

apt-get update -qq && apt-get install -yqq curl build-essential ca-certificates software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -qq && apt-get install -yqq python3.13 python3.13-venv python3.13-dev
echo "  Python: \$(python3.13 --version)"

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:\$PATH"

mkdir -p \${WORK_DIR}
echo "Downloading codebase..."
gsutil -q cp ${GCS_TARBALL} \${WORK_DIR}/codebase.tar.gz
gsutil -q cp ${GCS_SCRIPT} \${WORK_DIR}/vm_instruments_reference.sh
chmod +x \${WORK_DIR}/vm_instruments_reference.sh
tar xzf \${WORK_DIR}/codebase.tar.gz -C \${WORK_DIR}
rm \${WORK_DIR}/codebase.tar.gz

bash \${WORK_DIR}/vm_instruments_reference.sh \
  --start ${start_date} \
  --end ${TODAY} \
  --venues ${venues} \
  ${entity_arg} \
  --work-dir \${WORK_DIR}

gsutil -q cp /var/log/instruments-reference.log \
  ${GCS_STAGING}/logs/${vm_name}.log

echo "Complete. Shutting down..."
date
shutdown -h now
STARTUP_EOF

  # Delete existing VM with same name (idempotent)
  gcloud compute instances delete "${vm_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet 2>/dev/null || true

  # O-1 β remediation 2026-05-12: observability invariants for ManifestWriter
  # concurrency safety + canonical VM lifecycle metadata. Note: `vm_name` is
  # the local variable in this launcher (lowercase per existing convention).
  METADATA="DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_NAME=${vm_name}"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

  gcloud compute instances create "${vm_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB \
    --metadata="${METADATA}" \
    --metadata-from-file=startup-script="${STARTUP_FILE}" \
    --labels=purpose=sports-entity-sweep,env="${DEPLOYMENT_ENV}"

  rm "$STARTUP_FILE"
  echo "  Created: ${vm_name} (entity=${entity_label})"
}

PIDS=()
for entry in "${ALL_ENTITIES[@]}"; do
  IFS='|' read -r vm_suffix venues sports_entity start_date <<< "${entry}"
  launch_vm "${vm_suffix}" "${venues}" "${sports_entity}" "${start_date}" &
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
  echo "All ${#ALL_ENTITIES[@]} entity VMs launched"
fi
echo "============================================================"
echo ""
echo "Monitor:"
echo "  gcloud compute instances list --filter=\"name~'sports-entity-'\" --zones=${ZONE}"
echo ""
echo "Tail logs (after VM completes):"
for entry in "${ALL_ENTITIES[@]}"; do
  vm_suffix="$(echo "${entry}" | cut -d'|' -f1)"
  echo "  gsutil cat ${GCS_STAGING}/logs/sports-entity-${vm_suffix}.log"
done
