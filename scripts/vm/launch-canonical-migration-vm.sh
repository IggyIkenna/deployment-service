#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Phase 3.4 canonical-migration VM launcher — rewrite historical GCS data to
# the canonical partition layout for each category. Each VM runs
# migrate_{category}_canonical.py in dry-run OR full mode.
#
# Prefer DRY-RUN first: observer sees the planned rewrites in VM logs before
# committing to actual mutations. After reviewing a dry-run run log, re-fire
# without --dry-run to do the actual migration.
#
# Usage:
#   bash launch-canonical-migration-vm.sh cefi       2020-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh tradfi     2023-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh defi       2023-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh prediction 2025-03-14 2026-04-18 dry
#   bash launch-canonical-migration-vm.sh all        2020-01-01 2024-12-31 dry
#   bash launch-canonical-migration-vm.sh cefi       2020-01-01 2024-12-31 full
#   # tradfi-cme-options: START_DATE/END_DATE are cosmetic (VM labels only) -- the
#   # script real-scopes its own day worklist from the manifest via --all-days.
#   # STAMP is required for --apply (full); pass via MIGRATION_EXTRA_ARGS="--stamp <STAMP>".
#   bash launch-canonical-migration-vm.sh tradfi-cme-options 2023-05-01 2026-01-30 dry
#   MIGRATION_EXTRA_ARGS="--stamp $(date -u +%Y%m%dT%H%M%SZ)" \
#     bash launch-canonical-migration-vm.sh tradfi-cme-options 2023-05-01 2026-01-30 full
#
# Boot disk: 50GB (MDPS/features launchers' default; 10GB default was
# causing disk-pressure OOMs on long ranges).
#
# Env overrides:
#   MACHINE_TYPE=e2-standard-16  larger VM (default e2-standard-8; tradfi full-range OOM'd on -8, needs 64GB)
#   WORKERS=24                   migrator concurrency (tradfi default 24; other AGs keep their per-AG default)
#   ON_DEMAND=true               opt out of the SPOT default (backfill/idempotent VMs → SPOT per HARD RULE)
#   BOOT_DISK_GB=50              boot disk size
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
# Migration launchers operate ON env-tiered buckets — passing `--env staging`
# migrates only that tier's data.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse --env <val> before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

ASSET_GROUP="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"
MODE="${4:-dry}"  # dry | full
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"
# MACHINE_TYPE override (default e2-standard-8). TradFi v9 migration needs e2-standard-16
# (64GB): the 2026-06-29 full-range run OOM-killed on e2-standard-8. Per-year chunking +
# --workers 24 + 64GB is the fix (D3, instruments_completion_tracker_2026_07_06.md).
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
# Backfill/idempotent migration VMs default to SPOT (HARD RULE: spot-vms-for-backfill) — the
# migrator is idempotent (already-copied objects skip), so preemption just resumes on restart.
# ON_DEMAND=true is the only opt-out (matches the fleet backfill convention).
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

if [[ -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] <cefi|tradfi|defi|prediction|sports|tradfi-cme-options|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_TS_LABEL="$(date +%Y%m%d-%H%M%S)"

_script_for() {
    case "$1" in
        # CeFi v9: flat→hive fan-out (raw_tick_data/by_date/{SYMBOL}.parquet → canonical day= partitions).
        # DRY-BY-DEFAULT + --apply (same convention as the defi v9 tool), handled in _launch below.
        cefi)       echo "python -u -m market_tick_data_service.scripts.migrate_cefi_flat_to_v9_canonical --start-date $START_DATE --end-date $END_DATE --workers 64" ;;
        # TradFi v9: 3-layout-aware path canonicaliser (L-hive pipeline_mode insert + L-hyphen pseudo-hive parse +
        # candles; overlap dedup). DRY-BY-DEFAULT + --apply (same convention as the defi/cefi/prediction v9 tools).
        # --workers default 24 (NOT 64): workers=64 on 2026-06-29 thrashed the GCS connection pool (SSL
        # UNEXPECTED_EOF + pool-full) and OOM-killed on the full range. Run PER-YEAR (--start/--end) on
        # e2-standard-16 (MACHINE_TYPE) to bound the up-front object-list accumulation. D3.
        tradfi)     echo "python -u -m market_tick_data_service.scripts.migrate_tradfi_to_v9_canonical --start-date $START_DATE --end-date $END_DATE --workers ${WORKERS:-24}" ;;
        defi)       echo "python -u -m market_tick_data_service.scripts.migrate_defi_full_v9_canonical --start-date $START_DATE --end-date $END_DATE --workers 96" ;;
        # Prediction v9: bespoke legacy(market-data-tick-prediction)→canonical(pred-prd) consolidator.
        # DRY-BY-DEFAULT + --apply (same convention as the defi v9 tool), handled in _launch below.
        prediction) echo "python -u -m market_tick_data_service.scripts.migrate_prediction_to_pred_prd_v9 --start-date $START_DATE --end-date $END_DATE --workers 64" ;;
        # Sports: --workers 16 — same-region VM has lower GCS latency than the
        # cross-region laptop run that thrashed at workers=32 (2026-05-05
        # incident: 2476 generation conflicts, run died on 404 NotFound race).
        # MANIFEST_PER_VM_SHARDS=true is already exported by setup-data-pipeline-vm.sh
        # so manifest writes hit per-VM shards instead of canonical _index.
        sports)     echo "python -m market_tick_data_service.scripts.migrate_sports_canonical --start-date $START_DATE --end-date $END_DATE --workers 16" ;;
        # TradFi CME options_chain legacy-flat -> canonical bundled migration
        # (tradfi_cme_options_chain_legacy_layout_2026_07_10.md). A standalone scripts/
        # one-off, NOT a market_tick_data_service module -- invoked as a plain script
        # from $WORKSPACE/mtds (setup-data-pipeline-vm.sh's canonical-migration CWD).
        # Real-scopes its own day worklist from the manifest (--all-days); START_DATE/
        # END_DATE are unused by the tool itself (kept only for VM label/metadata
        # consistency with the other categories). DRY-BY-DEFAULT + --apply, same
        # convention as defi/cefi/tradfi/prediction -- handled in _launch below.
        # Category name deliberately starts with "tradfi-" so the VM name
        # (canonical-migration-tradfi-cme-options-<ts>) stays under the ALREADY
        # registered "canonical-migration-tradfi-" VM_PREFIX_TO_BUCKET prefix
        # (longest-prefix startswith match) -- no new registry entry needed.
        tradfi-cme-options) echo "python -u scripts/canonicalize_cme_options_chain_legacy_flat_2026_07_14.py --all-days" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    # VM_NAME_SUFFIX lets several shard VMs of the same category+second coexist without name collision
    # (e.g. one VM per date-shard / per --buckets). Prefix stays canonical-migration-<cat>- for the watchdog.
    local vm_name="canonical-migration-${cat}-${RUN_TS}${VM_NAME_SUFFIX:+-${VM_NAME_SUFFIX}}"
    local cmd; cmd="$(_script_for "$cat")"
    [[ -z "$cmd" ]] && { echo "Unknown category: $cat"; return 1; }
    # Flag convention differs by tool: the v9 tools (migrate_defi_full_v9_canonical +
    # migrate_prediction_to_pred_prd_v9) are DRY-BY-DEFAULT and take --apply to write; the others
    # are write-by-default + --dry-run.
    if [[ "$cat" == "defi" || "$cat" == "prediction" || "$cat" == "cefi" || "$cat" == "tradfi" || "$cat" == "tradfi-cme-options" ]]; then
        [[ "$MODE" == "full" ]] && cmd="$cmd --apply"   # dry = tool default (no flag)
    else
        [[ "$MODE" == "dry" ]] && cmd="$cmd --dry-run"
    fi
    # MIGRATION_EXTRA_ARGS forwards extra flags to the migration tool — for the defi v9 discover→shard
    # flow: `--phase discover` (once per bucket) then N× `--phase migrate --buckets <one>` date-shards.
    [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"

    echo "Launching $vm_name — $cmd"
    local md="VM_TASK=canonical-migration"
    md="${md},VM_SERVICE=market_tick_data_service"
    md="${md},VM_OPERATION=migrate-${cat}"
    md="${md},VM_ASSET_GROUP=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_MIGRATION_CMD=${cmd}"
    md="${md},VM_MIGRATION_MODE=${MODE}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    # SHA-pin the code tarballs so the VM provably runs the intended commit (race-proof
    # via setup-data-pipeline-vm.sh authoritative pinned pull). Pass the SHAs in the env
    # at launch: UAC_TARBALL_SHA / UTL_TARBALL_SHA / MTDS_TARBALL_SHA. Unset = floating pull.
    [[ -n "${UAC_TARBALL_SHA:-}" ]]  && md="${md},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"
    [[ -n "${UTL_TARBALL_SHA:-}" ]]  && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
    [[ -n "${MTDS_TARBALL_SHA:-}" ]] && md="${md},MTDS_TARBALL_SHA=${MTDS_TARBALL_SHA}"

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        "${PROVISIONING_ARGS[@]}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_GB}GB" \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=canonical-migration,category="${cat}",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS_LABEL}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$ASSET_GROUP" in
    cefi|tradfi|defi|prediction|sports|tradfi-cme-options) _launch "$ASSET_GROUP" ;;
    all)
        _launch cefi
        _launch tradfi
        _launch defi
        _launch prediction
        _launch sports
        ;;
    *) echo "Unknown category: $ASSET_GROUP"; exit 2 ;;
esac

echo ""
echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
