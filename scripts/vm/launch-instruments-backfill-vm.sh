#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VMs for parallel instruments-service backfill (all asset groups).
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first.
#
# VM allocation (6 VMs, default ranges):
#   instr-backfill-cefi-1:  CeFi   2020-01-01 → 2022-06-30
#   instr-backfill-cefi-2:  CeFi   2022-07-01 → 2024-12-31
#   instr-backfill-cefi-3:  CeFi   2025-01-01 → 2026-02-28
#   instr-backfill-defi:    DeFi   2020-01-01 → 2026-02-28
#   instr-backfill-tradfi:  TradFi 2020-01-01 → 2026-02-28
#   instr-backfill-sports:  Sports 2020-06-01 → 2026-03-28
#
# Usage:
#   bash launch-instruments-backfill-vm.sh                                     # All 6 VMs
#   bash launch-instruments-backfill-vm.sh --dry-run                           # Print plan
#   bash launch-instruments-backfill-vm.sh --asset-group DEFI                  # Only DeFi VM
#   bash launch-instruments-backfill-vm.sh --asset-group CEFI \
#       --start 2026-01-01 --end 2026-03-31                                    # Override window
#   bash launch-instruments-backfill-vm.sh --force                             # Bypass manifest skip
#   bash launch-instruments-backfill-vm.sh --asset-group CEFI --venues BINANCE-FUTURES \
#       --start 2026-07-01 --end 2026-07-01 --vm-name instr-backfill-cefi-pipelinecheck-1 \
#       --test-run                                                             # Scoped single-shard smoke check (test bucket)
#
# NOTE: instruments-service has no `--data-types` CLI flag — do not add one here.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ASSET_GROUP_FILTER=""
START_OVERRIDE=""
END_OVERRIDE=""
CHUNK_DAYS="${CHUNK_DAYS:-250}"
VENUES=""
VM_NAME_OVERRIDE=""
# Guards against a name collision: some asset groups (CEFI) have >1 predefined
# VM slot in the VMS array below, all matching the same --asset-group filter.
# Without this, --vm-name would try to gcloud-create the identical name 3x.
VM_NAME_OVERRIDE_USED=false
# --test-run routes writes to the -test- bucket sibling (IS_TEST_RUN=true metadata;
# setup-data-pipeline-vm.sh:251-254 reads + exports it, get_write_bucket_name()
# rewrites -{pid} -> -test-{pid}). Used by the pipeline_e2e_check smoke harness.
TEST_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)     DRY_RUN=true; shift ;;
    --force)       FORCE=true; shift ;;
    --project)     PROJECT_ID="$2"; shift 2 ;;
    --zone)        ZONE="$2"; shift 2 ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    --asset-group) ASSET_GROUP_FILTER="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
    --start)       START_OVERRIDE="$2"; shift 2 ;;
    --end)         END_OVERRIDE="$2"; shift 2 ;;
    --chunk-days)  CHUNK_DAYS="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    --venues)      VENUES="$2"; shift 2 ;;
    --vm-name)     VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --test-run)    TEST_RUN=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
  if [[ -z "${ASSET_GROUP_FILTER}" ]]; then
    echo "ERROR: --start/--end requires --asset-group to scope the override." >&2
    exit 1
  fi
fi

if [[ -n "${VENUES}" || -n "${VM_NAME_OVERRIDE}" ]]; then
  if [[ -z "${ASSET_GROUP_FILTER}" ]]; then
    echo "ERROR: --venues/--vm-name requires --asset-group to scope the override." >&2
    exit 1
  fi
fi

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

if $FORCE; then
  echo "MODE: --force ON — manifest skip BYPASSED. Every day-shard will be re-fetched."
else
  echo "MODE: --force OFF (default) — manifest skip ACTIVE."
fi

echo "============================================================"
echo "Instruments Backfill VM Fleet Launcher (Pattern A)"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  Filter:   ${ASSET_GROUP_FILTER:-all}"
echo "  Force:    ${FORCE}"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Venues:   ${VENUES:-all}"
echo "  VM name:  ${VM_NAME_OVERRIDE:-<default>}"
echo "  TestRun:  ${TEST_RUN}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "============================================================"

launch_vm() {
  local VM_NAME="$1"
  local ASSET_GROUP="$2"
  local START_DATE="$3"
  local END_DATE="$4"

  if [[ -n "${ASSET_GROUP_FILTER}" && "${ASSET_GROUP}" != "${ASSET_GROUP_FILTER}" ]]; then
    echo "  Skipping ${VM_NAME} (${ASSET_GROUP} != ${ASSET_GROUP_FILTER})"
    return 0
  fi

  if [[ -n "${VM_NAME_OVERRIDE}" ]] && $VM_NAME_OVERRIDE_USED; then
    echo "  Skipping ${VM_NAME} (--vm-name already applied to an earlier ${ASSET_GROUP} slot; scope with --start/--end to target exactly one)"
    return 0
  fi

  if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
    [[ -n "${START_OVERRIDE}" ]] && START_DATE="${START_OVERRIDE}"
    [[ -n "${END_OVERRIDE}" ]] && END_DATE="${END_OVERRIDE}"
    # Suffix avoids singleton collision on override.
    VM_NAME="${VM_NAME}-$(echo "${END_DATE}" | tr -d '-')"
    echo "  Date-window override: ${START_DATE} → ${END_DATE} (VM: ${VM_NAME})"
  fi

  # --vm-name takes precedence over the date-suffix override above (operator named it explicitly).
  if [[ -n "${VM_NAME_OVERRIDE}" ]]; then
    VM_NAME="${VM_NAME_OVERRIDE}"
    VM_NAME_OVERRIDE_USED=true
    echo "  VM name override: ${VM_NAME}"
  fi

  echo ""
  echo "--- ${VM_NAME}: ${ASSET_GROUP} ${START_DATE} → ${END_DATE} ---"

  if $DRY_RUN; then
    echo "  [DRY RUN] Would create VM: ${VM_NAME}"
    echo "  VM_TASK=instruments-backfill  VM_ASSET_GROUP=${ASSET_GROUP}"
    [[ -n "${VENUES}" ]] && echo "  VM_VENUE=${VENUES}"
    $TEST_RUN && echo "  IS_TEST_RUN=true"
    return 0
  fi

  local METADATA
  METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  METADATA="${METADATA},VM_TASK=instruments-backfill"
  METADATA="${METADATA},VM_SERVICE=instruments_service"
  METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
  [[ -n "${VENUES}" ]] && METADATA="${METADATA},VM_VENUE=${VENUES}"
  $FORCE && METADATA="${METADATA},VM_FORCE=true"
  $TEST_RUN && METADATA="${METADATA},IS_TEST_RUN=true,MANIFEST_ALLOW_STALE_FALLBACK=true"

  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

  echo "  Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  # SPOT preemption contract (vm_fleet_preemption_autorecovery_gap_2026_07_23.md
  # item 9): lc_write_preemption_signal_file marks a SPOT shutdown as an expected
  # preemption for fleet monitors (instead of an unexplained DP_VM_GONE_NO_CAPTURE).
  lc_write_preemption_signal_file

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
    --labels="purpose=instruments-backfill,asset-group=$(echo "${ASSET_GROUP}" | tr '[:upper:]' '[:lower:]'),env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
    --metadata="${METADATA}" \
    --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}"
  echo "  VM ${VM_NAME} created."
  echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
}

# VM definitions: name|asset_group|start|end
declare -a VMS=(
  "instr-backfill-cefi-1|CEFI|2020-01-01|2022-06-30"
  "instr-backfill-cefi-2|CEFI|2022-07-01|2024-12-31"
  "instr-backfill-cefi-3|CEFI|2025-01-01|2026-02-28"
  "instr-backfill-defi|DEFI|2020-01-01|2026-02-28"
  "instr-backfill-tradfi|TRADFI|2020-01-01|2026-02-28"
  "instr-backfill-sports|SPORTS|2020-06-01|2026-03-28"
  "instr-backfill-pred|PREDICTION|2020-01-01|2026-02-28"
)

for VM_DEF in "${VMS[@]}"; do
  IFS='|' read -r VM_NAME ASSET_GROUP START_DATE END_DATE <<< "$VM_DEF"
  launch_vm "$VM_NAME" "$ASSET_GROUP" "$START_DATE" "$END_DATE"
done

echo ""
echo "============================================================"
echo "All VMs launched."
echo "  Monitor: gcloud compute instances list --filter='name~instr-backfill' --project=${PROJECT_ID}"
echo "  Logs:    gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep instr-backfill"
echo "============================================================"
