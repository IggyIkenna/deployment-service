#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for MTDS sports odds backfill (Odds API historical)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Uses VM_TASK=mtds-backfill handler (chunked loop, default 7-day chunks) with
# VM_ASSET_GROUP=SPORTS. Sports MTDS does NOT accept --tier; VM_TIER is intentionally
# omitted from metadata to avoid the MTDS CLI rejecting unrecognised arguments.
#
# Usage:
#   bash launch-mtds-sports-odds-backfill-vm.sh                              # Full 5.8yr run
#   bash launch-mtds-sports-odds-backfill-vm.sh --dry-run                    # Print plan
#   bash launch-mtds-sports-odds-backfill-vm.sh --start 2025-01-04 --end 2025-01-04
#   bash launch-mtds-sports-odds-backfill-vm.sh --vm-name mtds-backfill-odds-2020 --start 2020-06-01 --end 2020-12-31 --force
#   bash launch-mtds-sports-odds-backfill-vm.sh --env staging
#   bash launch-mtds-sports-odds-backfill-vm.sh --allow-parallel --vm-name mtds-backfill-odds-2021 --start 2021-01-01 --end 2021-12-31
#   # Scope to specific leagues (canonical league IDs, comma-separated) — e.g. a
#   # Reference/Features-tier league with real odds_api coverage that the default
#   # unscoped run (Prediction-tier only) never reaches:
#   bash launch-mtds-sports-odds-backfill-vm.sh --vm-name mtds-backfill-odds-ucl-gap \
#     --league UCL,CHINA_SUPER_LEAGUE,RUSSIA_PREMIER_LEAGUE --start 2025-09-01 --end 2025-11-30
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
# e2-highmem-4 (32GB), not e2-standard-4 (16GB): a real odds_api fetch day fans out
# over many (bookmaker, league, fixture) shards with no aggregate byte-budget cap
# (same root cause as mtds_backfill_vm_memory_hang_large_chunk_2026_07_22.md's CEFI
# Tardis OOM — confirmed recurrence here: chunk 9/9 of a live gap-fill run climbed
# 52%->90% RSS on e2-standard-4 during one real fetch day and was kernel-OOM-killed,
# exit=137). That doc's validated fix (e2-highmem-4, ~2.2x headroom over the ~14.6GB
# CEFI peak, weeks of zero-recurrence) was only ever applied to launch-mtds-backfill-vm.sh's
# CEFI branch — this separate sports-odds launcher needed the same bump independently.
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-4}"
DRY_RUN=false
# Floor-clamped default (codex/02-data/sports-2020-06-data-floor.md): odds_api is
# NOT in instruments-service get_venue_epoch()'s defense-in-depth clamp list (only
# api_football/soccerfootball_info/footystats are), so a bare run of this launcher
# with no --start override is the ONLY defense against fabricating pre-floor rows
# for this venue. Was 2020-06-01 (5 days before the ruled 2020-06-06 floor).
START_DATE="${START_DATE:-2020-06-06}"
END_DATE="${END_DATE:-2026-03-28}"
# RESUME_FORCE / RESUME_ALLOW_PARALLEL env fallbacks (SPOT-preemption relaunch
# support, cefi_completion_program_2026_07_15.md pattern): RelaunchPreemptedVm
# re-invokes this launcher with ZERO CLI args, only the env captured by
# lc_write_launch_params below — so every knob that affects WHICH shard this VM
# covers must be readable from env, not just a CLI flag.
FORCE="${RESUME_FORCE:-false}"
ALLOW_PARALLEL="${RESUME_ALLOW_PARALLEL:-false}"
CHUNK_SIZE="${CHUNK_SIZE:-250}"
VM_NAME_OVERRIDE="${VM_NAME_OVERRIDE:-}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Comma-separated canonical league IDs (e.g. UCL,CHINA_SUPER_LEAGUE) to scope the
# run to specific leagues -- empty means the CLI's default (unscoped = Prediction-
# tier only). Wired through to VM_LEAGUE metadata; setup-data-pipeline-vm.sh
# already converts it to the CLI's --league flag (';'-joined in metadata, per the
# VM_INSTRUMENT_IDS convention -- gcloud --metadata=K=V,K=V splits on ',' at the
# key level, so a literal comma in the value would break parsing).
LEAGUE="${LEAGUE:-}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)      DRY_RUN=true; shift ;;
    --project)      PROJECT_ID="$2"; shift 2 ;;
    --zone)         ZONE="$2"; shift 2 ;;
    --start)        START_DATE="$2"; shift 2 ;;
    --end)          END_DATE="$2"; shift 2 ;;
    --force)          FORCE=true; shift ;;
    --allow-parallel) ALLOW_PARALLEL=true; shift ;;  # bypass singleton guard without VM_FORCE=true
    --chunk-size)   CHUNK_SIZE="$2"; shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name)      VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
    --league)      LEAGUE="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-odds-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "MTDS Sports Odds Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  League:    ${LEAGUE:-<all Prediction-tier, default>}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

# ── Singleton lock ──
if ! $FORCE && ! $ALLOW_PARALLEL; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-backfill-odds-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Sports odds VM already running: ${EXISTING}" >&2
    echo "      Use --allow-parallel to run alongside (no VM_FORCE), or --force to force-reprocess. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=mtds-backfill  VM_ASSET_GROUP=SPORTS  (no VM_TIER — sports MTDS has no --tier)"
  echo "  FORCE=${FORCE}  ALLOW_PARALLEL=${ALLOW_PARALLEL}  VM_LEAGUE=${LEAGUE}"
  exit 0
fi

# ── Build metadata ──
METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=mtds-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
# NOTE: VM_TIER intentionally omitted — sports MTDS CLI has no --tier argument
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_SIZE}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
[[ -n "$LEAGUE" ]] && METADATA="${METADATA},VM_LEAGUE=${LEAGUE//,/;}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
# "Close the SPOT-preemption relaunch gap"): persist the EXACT env this shard
# was launched with so exit_code_fleet_monitor's PREEMPTED auto_recover
# actuator (relaunch_backfill_vm.RelaunchPreemptedVm) can re-invoke THIS
# launcher with the SAME window/shard-name instead of falling back to the
# launcher's bare defaults (mtds-backfill-odds-1, 2020-06-01..2026-03-28) —
# RESUME_ALLOW_PARALLEL=true so the relaunch never trips the singleton lock on
# a sibling shard that is still legitimately running. Best-effort, non-fatal.
lc_write_launch_params "${VM_NAME}" "${PROJECT_ID}" "launch-mtds-sports-odds-backfill-vm.sh" \
    "VM_NAME_OVERRIDE=${VM_NAME}" \
    "START_DATE=${START_DATE}" \
    "END_DATE=${END_DATE}" \
    "CHUNK_SIZE=${CHUNK_SIZE}" \
    "RESUME_FORCE=${FORCE}" \
    "RESUME_ALLOW_PARALLEL=true" \
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}" \
    "LEAGUE=${LEAGUE}"

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
  --labels="purpose=mtds-sports-odds-backfill,env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
