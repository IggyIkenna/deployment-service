#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for MTDS category-specific backfill (CeFi, DeFi, TradFi, etc.)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Usage:
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --start 2024-04-05 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group DEFI --start 2024-04-05 --end 2026-04-05 --env staging
#   bash launch-mtds-backfill-vm.sh --asset-group TRADFI --start 2026-03-29 --end 2026-04-05
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --dry-run
#   bash launch-mtds-backfill-vm.sh --asset-group CEFI --venues BINANCE-FUTURES \
#       --data-types trades --instrument-ids BTCUSDT --start 2026-07-01 --end 2026-07-01 \
#       --vm-name mtds-backfill-cefi-pipelinecheck-1 --test-run   # Scoped single-shard smoke check (test bucket)
#   bash launch-mtds-backfill-vm.sh --asset-group SPORTS --venues ODDS_API --league 'UCL;CHINA_SUPER_LEAGUE' \
#       --data-types trades --start 2025-09-01 --end 2025-11-30 \
#       --vm-name mtds-backfill-sports-odds-3leagues-1   # Scoped odds-api league backfill (sports only;
#       # use ';' between multiple leagues -- same convention as --instrument-ids, gcloud --metadata splits on ',')
#
# --instrument-ids: verbatim-passed to VM_INSTRUMENT_IDS metadata; use ';' to separate
# multiple symbols (gcloud --metadata=K=V,K=V splits on ',' at the key level — see
# setup-data-pipeline-vm.sh:218-228). --test-run sets IS_TEST_RUN=true so writes route
# to the -test- bucket sibling (MTDS's freshness-read stays PROD-driven regardless).
#
# Pre-launch prerequisite:
#   bash deployment-service/scripts/vm/create-code-tarballs.sh   # CORE includes MTDS
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
# Default resolved below (asset-group-aware) once ASSET_GROUP is parsed — an explicit
# --machine-type flag or MACHINE_TYPE env var always wins over that default.
MACHINE_TYPE="${MACHINE_TYPE:-}"
DRY_RUN=false
ASSET_GROUP=""
START_DATE=""
END_DATE=""
TIER=""
FORCE=false
CHUNK_SIZE="${CHUNK_SIZE:-250}"
# Opt-in dates-in-flight per chunk (UTL ServiceCLI --batch-date-concurrency). Empty =
# serial (today's behavior). Only pass a value once the UTL driver + ServiceCLI flag
# are deployed (in the code tarball this VM fetches) — an older UTL errors on the flag.
BATCH_DATE_CONCURRENCY="${BATCH_DATE_CONCURRENCY:-}"
VM_NAME_OVERRIDE=""
VENUES=""
DATA_TYPES=""
INSTRUMENT_IDS=""
SOURCE=""
LEAGUE=""
# --test-run routes writes to the -test- bucket sibling (IS_TEST_RUN=true metadata;
# setup-data-pipeline-vm.sh:251-254 reads + exports it — MTDS's freshness-read
# stays PROD-driven per the read-bucket asymmetry, only the WRITE target moves).
# Used by the pipeline_e2e_check smoke harness.
TEST_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --project)       PROJECT_ID="$2"; shift 2 ;;
    --zone)          ZONE="$2"; shift 2 ;;
    --asset-group)   ASSET_GROUP="$2"; shift 2 ;;
    --start)         START_DATE="$2"; shift 2 ;;
    --end)           END_DATE="$2"; shift 2 ;;
    --tier)          TIER="$2"; shift 2 ;;
    --force)         FORCE=true; shift ;;
    --chunk-size)    CHUNK_SIZE="$2"; shift 2 ;;
    --batch-date-concurrency) BATCH_DATE_CONCURRENCY="$2"; shift 2 ;;
    --machine-type)  MACHINE_TYPE="$2"; shift 2 ;;
    --vm-name)       VM_NAME_OVERRIDE="$2"; shift 2 ;;
    --venues)        VENUES="$2"; shift 2 ;;
    --data-types)    DATA_TYPES="$2"; shift 2 ;;
    --instrument-ids) INSTRUMENT_IDS="$2"; shift 2 ;;
    --source)        SOURCE="$2"; shift 2 ;;
    --league)        LEAGUE="$2"; shift 2 ;;
    --test-run)      TEST_RUN=true; shift ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)     ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$ASSET_GROUP" ]]; then
  echo "ERROR: --asset-group is required (CEFI, DEFI, TRADFI, SPORTS, PREDICTION)"
  exit 1
fi

CATEGORY_LOWER=$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')

# CEFI-specific machine-type default: the Tardis per-chunk download path showed an
# unpredictable transient memory spike (confirmed real kernel OOM-kill, anon-rss up to
# ~14.6GB, two back-to-back identical --chunk-days 1 chunks for the same symbol/venue
# set varying >2x — see mtds_backfill_vm_memory_hang_large_chunk_2026_07_22.md) that
# sits right at the e2-standard-4 (16GB) ceiling. This is the SAME OOM signature already
# root-caused + fixed for tradfi OHLCV backfills (tradfi_backfill_oom_remediation_2026_06_24.md):
# bumping to e2-highmem-4 (32GB) gave weeks of zero-OOM-recurrence fleet operation there.
# Only changes the DEFAULT for asset-group=cefi; an explicit --machine-type/MACHINE_TYPE
# always wins, and other asset groups keep the e2-standard-4 default unchanged.
if [[ -z "$MACHINE_TYPE" ]]; then
  if [[ "$CATEGORY_LOWER" == "cefi" ]]; then
    MACHINE_TYPE="e2-highmem-4"
  else
    MACHINE_TYPE="e2-standard-4"
  fi
fi

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME_OVERRIDE:-mtds-backfill-${CATEGORY_LOWER}-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "MTDS Category Backfill VM Launcher (Pattern A)"
echo "  Category:  ${ASSET_GROUP}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE:-<not set>} → ${END_DATE:-<not set>}"
echo "  Tier:      ${TIER:-N/A}"
echo "  Chunk:     ${CHUNK_SIZE} days per batch"
echo "  Force:     ${FORCE}"
echo "  Venues:    ${VENUES:-all}"
echo "  DataTypes: ${DATA_TYPES:-all}"
echo "  InstrIDs:  ${INSTRUMENT_IDS:-<full universe>}"
echo "  Source:    ${SOURCE:-<SOURCE_PRIORITY default>}"
echo "  League:    ${LEAGUE:-all (sports only)}"
echo "  TestRun:   ${TEST_RUN}"
echo "  VM:        ${VM_NAME}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

# ── Singleton lock ──
# Refuses launch if any mtds-backfill-{category}-* VM is RUNNING in the zone.
# Prevents Tardis per-IP thundering-herd (concurrent REAL, multi-day production
# backfill VMs sharing egress NAT). --test-run launches are exempt: they're tiny,
# single-day, test-bucket-only smoke-check fetches, not the multi-day production
# volume this lock exists to protect against -- and the pipeline_e2e_check smoke
# tool needs genuine concurrency across many shards' skip-leg (non-forced) checks
# to be practical (confirmed via a real full-sweep run: 20-way concurrent smoke
# checks for the same category made every non-forced launch see one of the other
# 19 concurrently-running force-leg VMs and abort, a 100%-failure false negative,
# not a real backfill collision).
VM_PREFIX="mtds-backfill-${CATEGORY_LOWER}-"
if ! $FORCE && ! $TEST_RUN; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^${VM_PREFIX}\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: MTDS backfill VM already running for ${ASSET_GROUP}: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=mtds-backfill  VM_ASSET_GROUP=${ASSET_GROUP}"
  [[ -n "$INSTRUMENT_IDS" ]] && echo "  VM_INSTRUMENT_IDS=${INSTRUMENT_IDS}"
  [[ -n "$SOURCE" ]]         && echo "  VM_SOURCE=${SOURCE}"
  [[ -n "$LEAGUE" ]]        && echo "  VM_LEAGUE=${LEAGUE}"
  $TEST_RUN && echo "  IS_TEST_RUN=true"
  exit 0
fi

# ── Build metadata ──
METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=mtds-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
# Self-declaring Tardis-cap model (cefi_completion_program_2026_07_15.md,
# operator-approved 2026-07-16): only CEFI rides Tardis through this
# multi-asset-group launcher (DEFI/TRADFI/SPORTS/PREDICTION don't) — stamp
# ONLY when --asset-group CEFI so tardis-concurrency-guard.sh's metadata count
# stays accurate for the other asset groups too.
[[ "${CATEGORY_LOWER}" == "cefi" ]] && METADATA="${METADATA},VM_TARDIS_CONSUMER=1"
# Tardis concurrency lease passthrough (opt-in, mirrors launch-cefi-sharded-backfill.sh):
# lets checker re-runs of Tardis venues SERIALIZE against production lease-enabled
# waves instead of 403ing on the single-concurrent-IP key
# (tardis_concurrent_ip_lockout_2026_07_12.md).
[[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]] && METADATA="${METADATA},TARDIS_CONCURRENCY_LEASE=1"
[[ -n "${TARDIS_CONCURRENCY_LEASE_BUCKET:-}" ]] && METADATA="${METADATA},TARDIS_CONCURRENCY_LEASE_BUCKET=${TARDIS_CONCURRENCY_LEASE_BUCKET}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_SIZE}"
[[ -n "${BATCH_DATE_CONCURRENCY:-}" ]] && METADATA="${METADATA},VM_BATCH_DATE_CONCURRENCY=${BATCH_DATE_CONCURRENCY}"
[[ -n "$START_DATE" ]] && METADATA="${METADATA},VM_START_DATE=${START_DATE}"
[[ -n "$END_DATE" ]]   && METADATA="${METADATA},VM_END_DATE=${END_DATE}"
[[ -n "$TIER" ]]       && METADATA="${METADATA},VM_TIER=${TIER}"
[[ -n "$VENUES" ]]     && METADATA="${METADATA},VM_VENUE=${VENUES}"
[[ -n "$DATA_TYPES" ]] && METADATA="${METADATA},VM_DATA_TYPES=${DATA_TYPES}"
[[ -n "$INSTRUMENT_IDS" ]] && METADATA="${METADATA},VM_INSTRUMENT_IDS=${INSTRUMENT_IDS}"
[[ -n "$SOURCE" ]]     && METADATA="${METADATA},VM_SOURCE=${SOURCE}"
[[ -n "$LEAGUE" ]]     && METADATA="${METADATA},VM_LEAGUE=${LEAGUE}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
# Test runs also get a relaxed consolidator-staleness budget: -test- buckets have
# NO standing consolidator cron (the checker force-consolidates them once in
# Phase-0), so the in-VM assert_consolidator_healthy 120s default budget re-trips
# minutes later and aborts the leg with ManifestConsolidatorStaleError (the KRX
# skip-leg "ambiguous" of 2026-07-13; MANIFEST_ALLOW_STALE_FALLBACK does NOT gate
# that pre-flight assert, only the reader's fallback merge). Test buckets are
# small, so a 24h budget is safe — mirrors launch-cefi-sharded-backfill.sh.
$TEST_RUN && METADATA="${METADATA},IS_TEST_RUN=true,MANIFEST_ALLOW_STALE_FALLBACK=true,MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

# Tardis concurrent-VM cap (operator HARD RULE 2026-07-14): cefi MTDS backfill/pipelinecheck
# VMs consume the shared Tardis key (single revolving active-IP slot), so they count toward
# the 3-VM cap alongside the sharded-backfill fleet; the lease does NOT lift the cap. NOTE:
# this launcher's --force flag means VM_FORCE (data re-fetch), NOT a guard bypass — when the
# cap refuses, wait for a running Tardis VM to finish (or an operator can bump
# TARDIS_MAX_CONCURRENT_VMS explicitly).
#
# Scoped to Tardis-sourced venues only (not blanket asset_group==cefi): this launcher is
# also used for the documented CAP-EXEMPT native-REST venues (HYPERLIQUID/ASTER/
# LIGHTER-ZKSYNC/EXTENDED-STARKNET/COINBASE-CDE), which never touch datasets.tardis.dev and
# so must not be refused just because a real Tardis VM is running elsewhere — see
# mtds_backfill_launcher_guard_overapplies_to_nontardis_venues_2026_07_28.md.
# SSOT: unified-trading-pm/plans/active/issues/tardis_concurrent_ip_lockout_2026_07_12.md
if [[ "${DRY_RUN:-false}" != "true" && "$(echo "${ASSET_GROUP}" | tr '[:upper:]' '[:lower:]')" == "cefi" ]]; then
    # shellcheck source=./tardis-concurrency-guard.sh
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"
    if tardis_venue_list_needs_guard "${VENUES}"; then
        tardis_concurrency_guard 1 "${ZONE}" "${PROJECT_ID}" || exit 1
    else
        echo "[tardis-guard] SKIPPED: --venues (${VENUES}) are all CAP-EXEMPT (non-Tardis) — no contention for the shared Tardis IP."
    fi
fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# SPOT preemption contract (vm_fleet_preemption_autorecovery_gap_2026_07_23.md
# item 9): lc_write_preemption_signal_file marks a SPOT shutdown as an expected
# preemption for fleet monitors (instead of an unexplained DP_VM_GONE_NO_CAPTURE).
lc_write_preemption_signal_file

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=mtds-backfill,asset-group=${CATEGORY_LOWER},env=${DEPLOYMENT_ENV}",managed-by=deployment-service \
  --metadata="${METADATA}" \
  --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
