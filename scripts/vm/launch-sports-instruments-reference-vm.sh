#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch 3 GCE VMs for instruments-service sports reference data backfill.
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first.
#
# Per-entity skip logic: manifest checked per entity (all sports entities);
# only fetches those actually missing. No wasted API calls.
#
# Date range: 2014-01-01 → 2026-04-10 (full per-source-genesis history), split across 5 VMs:
#   sports-ref-v3-e1: 2014-01-01 → 2016-12-31  (understat genesis 2014; api_football 2015)
#   sports-ref-v3-e2: 2017-01-01 → 2020-05-31
#   sports-ref-v3-1:  2020-06-01 → 2022-05-31  (odds_api genesis 2020-06)
#   sports-ref-v3-2:  2022-06-01 → 2024-05-31
#   sports-ref-v3-3:  2024-06-01 → 2026-04-10  (golden window 2025-09..11)
# Per-source coverage_start clips pre-genesis fetches (footystats/transfermarkt/sfi 2019, odds 2020-06)
# to honest-empty — no wasted API calls. The 5 VMs delete+recreate on each launch (idempotent).
#
# Usage:
#   bash launch-sports-instruments-reference-vm.sh           # Launch all 3 VMs
#   bash launch-sports-instruments-reference-vm.sh --dry-run # Print plan only
#   bash launch-sports-instruments-reference-vm.sh --env staging
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"  # 32GB — e2-standard-2 (8GB) OOM'd (exit 137) on player-stats enrichment 2026-06-24
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
CHUNK_DAYS="${CHUNK_DAYS:-30}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# ONLY_VM_NAME env fallback (SPOT-preemption relaunch support): a bare
# re-invocation (RelaunchPreemptedVm passes ZERO CLI args, only the env
# lc_write_launch_params captured) must scope back down to the ONE shard VM
# that was preempted — otherwise it would re-launch all 3 shards.
ONLY_VM_NAME="${ONLY_VM_NAME:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)    DRY_RUN=true; shift ;;
    --project)    PROJECT_ID="$2"; shift 2 ;;
    --zone)       ZONE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
    --chunk-days) CHUNK_DAYS="$2"; shift 2 ;;
    --on-demand)  ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

# VM date-split ranges — clamped to the 2020-06 DATA FLOOR (operator ruling 2026-07-21):
# sports odds start 2020-06-06, so pre-floor reference backfill is fabrication-by-construction
# and the entirely-pre-floor windows (2014-2016, 2017-2020-05-31) are REMOVED; the boundary
# window starts at the floor. SSOT: codex/02-data/sports-2020-06-data-floor.md.
VM_CONFIGS=(
  "sports-ref-v3-1|2020-06-06|2022-05-31"
  "sports-ref-v3-2|2022-06-01|2024-05-31"
  "sports-ref-v3-3|2024-06-01|2026-04-10"
)

echo "============================================================"
echo "Instruments Reference Data VM Launcher (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  VMs:      ${#VM_CONFIGS[@]}"
for cfg in "${VM_CONFIGS[@]}"; do
  IFS='|' read -r name start end <<< "$cfg"
  echo "    ${name}: ${start} → ${end}"
done
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  DryRun:   ${DRY_RUN}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

for cfg in "${VM_CONFIGS[@]}"; do
  IFS='|' read -r VM_NAME START_DATE END_DATE <<< "$cfg"

  echo ""
  echo "--- ${VM_NAME}: ${START_DATE} → ${END_DATE} ---"

  if [[ -n "$ONLY_VM_NAME" && "$VM_NAME" != "$ONLY_VM_NAME" ]]; then
    echo "  [SKIP] ONLY_VM_NAME=${ONLY_VM_NAME} set — skipping ${VM_NAME}"
    continue
  fi

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    continue
  fi

  # Delete existing VM (idempotent)
  gcloud compute instances delete "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  METADATA="${METADATA},VM_TASK=instruments-backfill"
  METADATA="${METADATA},VM_SERVICE=instruments_service"
  METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  # instruments-store-sports-prd's consolidator merge cycle regularly takes
  # 400-460s (>3x the reader's 120s default) — see
  # plans/active/issues/manifest_consolidator_stale_sports_bucket_2026_07_21.md
  METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=1800"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"

  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

  # SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
  # "Close the SPOT-preemption relaunch gap"): persist the ONE shard this VM
  # covers so exit_code_fleet_monitor's PREEMPTED auto_recover actuator
  # (relaunch_backfill_vm.RelaunchPreemptedVm) re-invokes THIS launcher scoped
  # to just this shard (via the ONLY_VM_NAME env fallback above) instead of
  # re-launching all 3 shards. Best-effort, non-fatal.
  lc_write_launch_params "${VM_NAME}" "${PROJECT_ID}" "launch-sports-instruments-reference-vm.sh" \
      "ONLY_VM_NAME=${VM_NAME}" \
      "CHUNK_DAYS=${CHUNK_DAYS}" \
      "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

  # shellcheck disable=SC2086
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    ${PROVISIONING_FLAGS} \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --labels="purpose=sports-instruments-reference,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
    --metadata="${METADATA}"
  echo "  VM created and RUNNING: ${VM_NAME}"
  echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
done

echo ""
echo "============================================================"
echo "Monitor:"
echo "  gcloud compute instances list --filter='name~sports-ref-v3' --project=${PROJECT_ID}"
echo "  gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep sports-ref-v3"
echo "============================================================"
