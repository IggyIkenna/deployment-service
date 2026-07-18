#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for targeted DeFi instruments-service backfill.
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first.
#
# Backfills 7 venues with full historical data:
#   CURVE-AVALANCHE, CURVE-OPTIMISM, BALANCER-ETHEREUM,
#   UNISWAP_V3-ETHEREUM, UNISWAP_V3-POLYGON, RAYDIUM-SOLANA, UNISWAP_V4-ETHEREUM
#
# VM_NAME: instr-backfill-defi-targeted (or with END_DATE suffix on override)
# VM_TASK: instruments-backfill, VM_ASSET_GROUP: DEFI
#
# Usage:
#   bash launch-defi-backfill-vm.sh                                 # Launch VM (defaults: 2020-01-01..2026-04-04)
#   bash launch-defi-backfill-vm.sh --dry-run                       # Print plan only
#   bash launch-defi-backfill-vm.sh --start 2026-04-01 --end 2026-05-16  # Override date window
#   bash launch-defi-backfill-vm.sh --force                         # Bypass manifest skip
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
DRY_RUN=false
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
START_OVERRIDE=""
END_OVERRIDE=""
CHUNK_DAYS="${CHUNK_DAYS:-250}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)    DRY_RUN=true; shift ;;
    --force)      FORCE=true; shift ;;
    --project)    PROJECT_ID="$2"; shift 2 ;;
    --zone)       ZONE="$2"; shift 2 ;;
    --env)        DEPLOYMENT_ENV="$2"; shift 2 ;;
    --start)      START_OVERRIDE="$2"; shift 2 ;;
    --end)        END_OVERRIDE="$2"; shift 2 ;;
    --chunk-days) CHUNK_DAYS="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    --preemptible) shift ;;  # deprecated no-op: SPOT is now the default
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="instr-backfill-defi-targeted"
START_DATE="2020-01-01"
END_DATE="2026-04-04"

# Apply CLI overrides. Suffix VM name with end-date to avoid singleton collision.
if [[ -n "${START_OVERRIDE}" || -n "${END_OVERRIDE}" ]]; then
  [[ -n "${START_OVERRIDE}" ]] && START_DATE="${START_OVERRIDE}"
  [[ -n "${END_OVERRIDE}" ]] && END_DATE="${END_OVERRIDE}"
  VM_NAME="instr-backfill-defi-targeted-$(echo "${END_DATE}" | tr -d '-')"
  echo "  Date-window override: ${START_DATE} → ${END_DATE} (VM: ${VM_NAME})"
fi

if $FORCE; then
  echo "MODE: --force ON — manifest skip BYPASSED."
else
  echo "MODE: --force OFF (default) — manifest skip ACTIVE."
fi

echo "============================================================"
echo "DeFi Instruments Backfill VM Launcher (Pattern A)"
echo "  VM:       ${VM_NAME}"
echo "  Project:  ${PROJECT_ID}"
echo "  Zone:     ${ZONE}"
echo "  Machine:  ${MACHINE_TYPE}"
echo "  Range:    ${START_DATE} → ${END_DATE}"
echo "  Chunk:    ${CHUNK_DAYS} days"
echo "  Force:    ${FORCE}"
echo "  Env:      ${DEPLOYMENT_ENV}"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-code.tar.gz"
echo "============================================================"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=instruments-backfill  VM_ASSET_GROUP=defi  VM_CHUNK_DAYS=${CHUNK_DAYS}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=instruments-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
# instruments-service InstrumentsHandler validates asset_group against the lowercase
# closed set {cefi,defi,prediction,sports,tradfi}. Uppercase `DEFI` is rejected with
# `Unknown asset_group 'DEFI'. Valid: ['cefi','defi','prediction','sports','tradfi']`
# and ALL payloads are dropped (0 results collected, manifest unchanged). MTDS normalises
# uppercase so the MTDS launchers don't hit this. Reference fix for sports/footystats:
# deployment-service@9ded013. Reference plan: cefi_venue_backfill_coverage_remediation_2026_05_27.md
# § 6G.
METADATA="${METADATA},VM_ASSET_GROUP=defi"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
# Daily catalog → 120s consolidated-staleness default is too short; use 24h to avoid fallback to per-VM shards
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
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
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
  # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
  # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
  # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
  # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
  # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=defi-instruments-backfill,asset-group=defi,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"
echo "  VM ${VM_NAME} created."
echo "  Logs: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"

echo ""
echo "============================================================"
echo "Monitor:"
echo "  gcloud compute instances list --filter='name=${VM_NAME}' --project=${PROJECT_ID}"
echo "  gsutil ls gs://${CODE_BUCKET}/vm-logs/ | grep ${VM_NAME}"
echo "============================================================"
