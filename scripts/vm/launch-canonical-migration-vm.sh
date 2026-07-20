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
#   # tradfi (2026-07 orphan-proof content migration): START_DATE/END_DATE are cosmetic
#   # (VM labels only). Each VM does a FRESH single-walk of the CURRENT tradfi tick bucket on the VM
#   # then runs, IN ORDER, the three shipped passes over that ONE snapshot:
#   #   1. migrate_tradfi_canonical_2026_07  (1:1 copy->verify->delete executor)
#   #   2. rebundle_tradfi_chains_2026_07    (per-contract options_chain -> per-root bundle REDUCE)
#   #   3. recover_tradfi_garbage_underlying_2026_07 (garbage-underlying recover-or-quarantine)
#   # dry  -> all three --dry-run; mapping TSVs staged to GCS for review; NO GCS mutations.
#   # full -> all three --apply (rebundle+recover get --quarantine; migrate stays gate-free BY DESIGN so
#   #         the three passes remain a disjoint content-class partition over one snapshot). Massive-purge
#   #         is DELIBERATELY OFF (separate operator-gated step). SSOT:
#   #         plans/active/issues/tradfi_canonical_path_migration_design_2026_07_19.md
#   # DRY-RUN canary on ONE VM (whole-corpus walk, --apply first 200 objects/pass):
#   SHARD_OF=1 LIMIT=200 bash launch-canonical-migration-vm.sh tradfi 2023-01-01 2026-01-30 full
#   # ...or a single-day --apply canary:
#   CANARY_DAY=2024-01-15 bash launch-canonical-migration-vm.sh tradfi 2023-01-01 2026-01-30 full
#   # FULL sharded fan-out across N=20 VMs (each partitions by --shard-of/--shard-index; no overlap):
#   SHARD_OF=20 bash launch-canonical-migration-vm.sh tradfi 2023-01-01 2026-01-30 full
#   # ...or one specific shard on its own VM (targeted relaunch):
#   SHARD_OF=20 SHARD_INDEX=3 bash launch-canonical-migration-vm.sh tradfi 2023-01-01 2026-01-30 full
#
#   # tradfi-catalogue-canon (2026-07-20): the instruments-service Phase-B `-USD@LIN` catalogue
#   # canonicalization full sweep over the per-day
#   # `instrument_availability/by_date/day=*/venue=*/instruments.parquet` corpus (~27.1k files).
#   # START_DATE/END_DATE are cosmetic (VM labels only) -- the script lists its own worklist from
#   # the instruments-store-tradfi bucket. IN-REGION ONLY: a laptop run measured 1.6k/27.1k files
#   # in ~14 min and DECELERATING (cross-region GCS round-trip latency; the same pattern killed an
#   # earlier attempt at ~83% via socket exhaustion), so this category exists to move it onto a
#   # same-zone VM. Runs from $WORKSPACE/instruments (NOT mtds) -- hence VM_SERVICE=instruments_service
#   # so setup-data-pipeline-vm.sh stages instruments-service-code; the compound `cd ... && python`
#   # command re-homes the CWD inside the shared canonical-migration dispatch (which cds to mtds).
#   # Category name deliberately starts with "tradfi-" so the VM name stays under the ALREADY
#   # registered "canonical-migration-tradfi-" VM_PREFIX_TO_BUCKET prefix -- no new registry entry.
#   # NOTE: after a full sweep the operator MUST re-run build_instrument_catalogue.py --asset-group
#   # tradfi (the roll-up producer) or the next scheduled roll-up silently reverts catalog.parquet.
#   bash launch-canonical-migration-vm.sh tradfi-catalogue-canon 2023-01-01 2026-01-30 dry
#   bash launch-canonical-migration-vm.sh tradfi-catalogue-canon 2023-01-01 2026-01-30 full
#
# Boot disk: 50GB (MDPS/features launchers' default; 10GB default was
# causing disk-pressure OOMs on long ranges).
#
# Env overrides:
#   MACHINE_TYPE=e2-standard-16  larger VM (default e2-standard-8; the 2026-07 tradfi content passes STREAM
#                                the enumeration in bounded-memory chunks, so e2-standard-8 is fine)
#   WORKERS=24                   migrator concurrency (tradfi default 24; other AGs keep their per-AG default)
#   ON_DEMAND=true               opt out of the SPOT default (backfill/idempotent VMs → SPOT per HARD RULE)
#   BOOT_DISK_GB=50              boot disk size
#   # tradfi content-migration (2026-07) only:
#   SHARD_OF=20                  total shard count; >1 with SHARD_INDEX unset FANS OUT N sharded VMs
#   SHARD_INDEX=3                launch exactly this shard on ONE VM (canary / targeted relaunch)
#   LIMIT=200                    process only the first N (post-shard) objects PER PASS (canary scope)
#   CANARY_DAY=2024-01-15        narrow the fresh walk to one day= prefix (single-day canary scope)
#   TRADFI_TICK_BUCKET=<name>    override the resolved tradfi tick bucket (default:
#                                market-data-tick-tradfi-<prd|stg|dev>-<project>, == resolve_bucket_name)
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
# Must match setup-data-pipeline-vm.sh's WORKSPACE (where the staged repo trees land).
VM_WORKSPACE="/home/ikennaigboaka/workspace"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"
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
    echo "Usage: $0 [--env prod|staging|dev] <cefi|tradfi|defi|defi-per-instrument|prediction|sports|tradfi-cme-options|tradfi-catalogue-canon|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_TS_LABEL="$(date +%Y%m%d-%H%M%S)"

# ── tradfi content-migration (2026-07) sharding + scope knobs ──────────────────────────────────────
# SHARD_INDEX_EXPLICIT records whether the operator PINNED a single shard (launch just that VM) vs left
# it unset (SHARD_OF>1 then FANS OUT one VM per shard). Captured before the :- default masks the distinction.
SHARD_INDEX_EXPLICIT="${SHARD_INDEX+set}"
SHARD_OF="${SHARD_OF:-1}"
SHARD_INDEX="${SHARD_INDEX:-0}"
LIMIT="${LIMIT:-0}"
CANARY_DAY="${CANARY_DAY:-}"
# TradFi tick bucket (env-tiered). Bash formula == resolve_bucket_name(cloud=gcp, kind=tick-data,
# asset_group=tradfi) — VERIFIED market-data-tick-tradfi-prd-central-element-323112 for prod — and is
# the SAME shape setup-data-pipeline-vm.sh constructs (line ~1166). Baked host-side into a COMMA-FREE
# VM_MIGRATION_CMD (gcloud --metadata is comma-delimited; a resolve_bucket_name(...) one-liner has commas).
case "$DEPLOYMENT_ENV" in
    prod)    _ENV_SHORT="prd" ;;
    staging) _ENV_SHORT="stg" ;;
    *)       _ENV_SHORT="$DEPLOYMENT_ENV" ;;
esac
TRADFI_TICK_BUCKET="${TRADFI_TICK_BUCKET:-market-data-tick-tradfi-${_ENV_SHORT}-${PROJECT}}"

# Build the tradfi 2026-07 content-migration command: ONE fresh single-walk on the VM -> enumeration,
# then the three shipped passes IN ORDER over that ONE snapshot, then stage the mapping TSVs to GCS.
# Emitted as a single COMMA-FREE compound `&&` chain (see TRADFI_TICK_BUCKET note). The FIRST `python `
# is venv-rewritten by setup-data-pipeline-vm.sh's canonical-migration handler; the rebundle+recover
# `python` resolve via the venv activated on PATH (proven by the sports-v9 two-phase launcher).
# migrate runs GATE-FREE (no --quarantine/--content-repair/--purge-massive): it then LEAVES massive
# (PURGE_REFUSED_GATED), garbage-underlying (QUARANTINE_REFUSED_GATED), content-repair
# (CONTENT_REPAIR_DEFERRED) and per-contract chains (A_SKIP) IN PLACE, so passes 2/3 (which own those
# classes) still see them in the shared snapshot. rebundle+recover take --quarantine in full mode.
_tradfi_content_migration_cmd() {
    local vm_name="$1"
    local mode_flag quar_flag limit_flag walk
    if [[ "$MODE" == "full" ]]; then
        mode_flag="--apply"
        quar_flag="--quarantine"
    else
        mode_flag="--dry-run"
        quar_flag=""
    fi
    limit_flag=""
    [[ "${LIMIT:-0}" -gt 0 ]] && limit_flag="--limit ${LIMIT}"
    # CANARY_DAY narrows to one day= prefix; else the whole raw corpus. Scoping to raw_tick_data/
    # EXCLUDES the top-level _quarantine/_content_repair/_index sidecars so a resume re-walk is idempotent.
    if [[ -n "${CANARY_DAY:-}" ]]; then
        walk="gs://${TRADFI_TICK_BUCKET}/raw_tick_data/by_date/day=${CANARY_DAY}/**"
    else
        walk="gs://${TRADFI_TICK_BUCKET}/raw_tick_data/**"
    fi
    local base="market_tick_data_service.scripts"
    local sh="--shard-of ${SHARD_OF} --shard-index ${SHARD_INDEX}"
    local work="/home/ikennaigboaka/workspace/tradfi-canonical-migration"
    local enum="${work}/enumeration.txt"
    local mapd="${work}/mappings"
    local stage="gs://${CODE_BUCKET}/canonical-migration-tradfi/${RUN_TS}/${vm_name}/mappings/"
    # `\$(...)` + `\"` stay LITERAL in the metadata value (evaluated by the VM's bash), while ${...}
    # launcher locals expand host-side. No commas anywhere in the emitted string.
    printf '%s' "mkdir -p ${mapd} && gcloud storage ls -r \"${walk}\" > ${enum} && echo TRADFI_ENUM_LINES=\$(wc -l < ${enum}) && python -u -m ${base}.migrate_tradfi_canonical_2026_07 ${mode_flag} --enumeration ${enum} --out ${mapd}/migrate_mapping.tsv ${sh} ${limit_flag} --workers ${WORKERS:-24} && python -u -m ${base}.rebundle_tradfi_chains_2026_07 ${mode_flag} --enumeration ${enum} --out ${mapd}/rebundle_mapping.tsv ${sh} ${limit_flag} ${quar_flag} && python -u -m ${base}.recover_tradfi_garbage_underlying_2026_07 ${mode_flag} --enumeration ${enum} --out ${mapd}/recovery_mapping.tsv ${sh} ${limit_flag} ${quar_flag} && gcloud storage cp -r ${mapd}/ ${stage}"
}

# Build the tradfi catalogue `-USD@LIN` canonicalization command (instruments-service one-off).
# Emitted COMMA-FREE (gcloud --metadata is comma-delimited) as a single `cd ... && python ...` chain:
# the shared canonical-migration dispatch in setup-data-pipeline-vm.sh cds to $WORKSPACE/mtds, so the
# leading `cd` re-homes into the staged instruments-service tree. The FIRST literal `python ` token is
# venv-rewritten by that dispatch ($VENV/bin/python) -- the `cd` path and the script name contain no
# `python ` substring, so the rewrite lands on the intended token.
# GCP_PROJECT_ID/DEPLOYMENT_ENV are exported by setup-data-pipeline-vm.sh already, but are ALSO set
# inline here: without them resolve_bucket_name() raises BucketNamingError and the sweep dies instantly.
# PROCESS-level sharding is REQUIRED for throughput here, not a tuning nicety: the per-row
# canonicalization is pure Python and GIL-bound, so --workers alone saturates ~1.3 cores however high
# it is set. MEASURED 2026-07-20 on an in-region e2-standard-16 with --workers 64: 130% of 1600% CPU,
# 86% idle, ~1.0 files/s => ~7.4h for the 27.1k-file sweep — i.e. no better than the cross-region
# laptop run, because GCS latency was never the bottleneck. CANON_SHARDS (default 16) forks one
# PROCESS per shard over a disjoint stride partition (proven disjoint+exhaustive: sum(shards)==27100,
# all-unique) and waits, propagating a non-zero rc if ANY shard fails.
_catalogue_canon_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    local shards="${CANON_SHARDS:-16}"
    local per_worker="${WORKERS:-8}"
    local script="scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py"
    if [[ "$shards" -le 1 ]]; then
        printf '%s' "cd ${VM_WORKSPACE}/instruments && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} python -u ${script} --by-day${apply_flag} --by-day-full-sweep --workers ${per_worker}"
        return
    fi
    # `\$` / `\"` stay LITERAL in the gcloud metadata value (the VM's bash evaluates them); launcher
    # locals expand host-side. Comma-free throughout (gcloud --metadata is comma-delimited).
    # rc is collected with a per-PID `wait` (NOT `wait $(jobs -p)`, which returns only the LAST
    # job's status — VERIFIED 2026-07-20: with shard 1 exiting 7 and shard 2 exiting 0 that form
    # reports rc_all=0, silently green-lighting a partially-failed sweep).
    printf '%s' "cd ${VM_WORKSPACE}/instruments && export GCP_PROJECT_ID=${PROJECT} && export DEPLOYMENT_ENV=${DEPLOYMENT_ENV} && rc_all=0; pids=\"\"; for i in \$(seq 0 \$((${shards}-1))); do python -u ${script} --by-day${apply_flag} --by-day-full-sweep --workers ${per_worker} --shard-of ${shards} --shard-index \${i} > /tmp/canon_shard\${i}.log 2>&1 & pids=\"\${pids} \$!\"; done; for p in \${pids}; do wait \${p} || rc_all=1; done; for i in \$(seq 0 \$((${shards}-1))); do echo \"=== SHARD \${i} tail ===\"; tail -6 /tmp/canon_shard\${i}.log; done; echo \"=== CANON ALL-SHARDS COMPLETE rc_all=\${rc_all} ===\"; exit \${rc_all}"
}

_script_for() {
    case "$1" in
        # CeFi v9: flat→hive fan-out (raw_tick_data/by_date/{SYMBOL}.parquet → canonical day= partitions).
        # DRY-BY-DEFAULT + --apply (same convention as the defi v9 tool), handled in _launch below.
        cefi)       echo "python -u -m market_tick_data_service.scripts.migrate_cefi_flat_to_v9_canonical --start-date $START_DATE --end-date $END_DATE --workers 64" ;;
        # TradFi: REPOINTED 2026-07-19 to the orphan-proof CONTENT migration (fresh walk -> executor ->
        # rebundle -> recovery). The old day-walking migrate_tradfi_to_v9_canonical is superseded; the
        # tradfi command is built by _tradfi_content_migration_cmd() (needs vm_name), handled in _launch.
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
        # DeFi R3 per-instrument split (migrate_defi_batch_to_per_instrument @ market-tick-data-service).
        # Splits the historical {venue}_{chain}_{ts} + {data_type}_{ts} (oracle_prices/gas_fees) bundled
        # batches into canonical per-instrument {sanitize_defi_symbol(symbol)}.parquet leaves, retiring each
        # source to _migrated_* (recoverable — NOT deleted; --delete-old is deliberately never passed here).
        # IDEMPOTENT: already-split leaves + _migrated_* + _index/_needs_attribution markers are skipped, so
        # a re-run (e.g. after a SPOT preemption) resumes cleanly. CHUNKED per-year (--start-date/--end-date)
        # so each chunk's progress is visible in the run.log and resumable; the script's OWN pre-apply gate
        # dry-measures + LOGS each chunk's needs_attribution ratio and REFUSES to write a chunk whose
        # unattributable fraction exceeds --max-needs-attribution-ratio (0.5 default). The loop RECORDS per-
        # chunk rc and continues (a single bad year never abandons the good ones), then — only on a clean
        # all-chunk migration in full mode — chains rebuild_defi_manifest over the whole range so the manifest
        # re-derives per-instrument instrument_id=stem. DRY-BY-DEFAULT (no --apply) + --apply for full; the
        # rebuild mirrors the mode (--dry-run in dry). Flag-append + MIGRATION_EXTRA_ARGS are SUPPRESSED for
        # this category in _launch (the command is a self-contained compound loop, not a single invocation).
        # Only a single literal 'python ' token appears per python module (the loop reuses one venv python
        # after setup-data-pipeline-vm.sh's first-occurrence replacement; the venv is `source`-activated so
        # the second module also resolves to it). Year list overridable via MIGRATION_YEARS; workers via WORKERS.
        defi-per-instrument)
            local _apply=""; [[ "$MODE" == "full" ]] && _apply=" --apply"
            local _rbdry=""; [[ "$MODE" != "full" ]] && _rbdry=" --dry-run"
            local _bkt="market-data-tick-defi-prd-central-element-323112"
            local _yrs="${MIGRATION_YEARS:-2020 2021 2022 2023 2024 2025 2026}"
            printf '%s' "rc_all=0; for y in ${_yrs}; do echo \"=== R3 CHUNK year=\${y} START ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; python -u -m market_tick_data_service.scripts.migrate_defi_batch_to_per_instrument --bucket ${_bkt} --start-date \${y}-01-01 --end-date \${y}-12-31 --workers ${WORKERS:-16}${_apply}; rc=\$?; echo \"=== R3 CHUNK year=\${y} DONE rc=\${rc} ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; [ \"\${rc}\" -ne 0 ] && rc_all=1; done; echo \"=== R3 MIGRATION ALL-CHUNKS COMPLETE rc_all=\${rc_all} ===\"; if [ \"\${rc_all}\" -eq 0 ]; then echo \"=== REBUILD MANIFEST START ===\"; python -u -m market_tick_data_service.scripts.rebuild_defi_manifest --bucket ${_bkt} --start-date 2020-01-01 --end-date 2026-12-31${_rbdry}; rc_rb=\$?; echo \"=== REBUILD MANIFEST DONE rc=\${rc_rb} ===\"; exit \${rc_rb}; else echo \"=== SKIP REBUILD: migration had chunk failure(s); inspect per-chunk rc above ===\"; exit 1; fi" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    # VM_NAME_SUFFIX lets several shard VMs of the same category+second coexist without name collision
    # (e.g. one VM per date-shard / per --buckets). Prefix stays canonical-migration-<cat>- for the watchdog.
    local vm_name="canonical-migration-${cat}-${RUN_TS}${VM_NAME_SUFFIX:+-${VM_NAME_SUFFIX}}"
    local cmd
    if [[ "$cat" == "tradfi" ]]; then
        # tradfi = the 2026-07 content migration: multi-pass compound command (mode flags + --quarantine
        # embedded per-pass inside the builder, so it is NOT subject to the generic single --apply append).
        cmd="$(_tradfi_content_migration_cmd "$vm_name")"
    elif [[ "$cat" == "tradfi-catalogue-canon" ]]; then
        # Self-contained `cd ... && python ...` chain; --apply is embedded per-MODE by the builder,
        # so the generic --apply/--dry-run append below is deliberately bypassed (same reason as
        # the tradfi content-migration branch).
        cmd="$(_catalogue_canon_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    else
        cmd="$(_script_for "$cat")"
        [[ -z "$cmd" ]] && { echo "Unknown category: $cat"; return 1; }
        # Flag convention differs by tool: the v9 tools (migrate_defi_full_v9_canonical +
        # migrate_prediction_to_pred_prd_v9) are DRY-BY-DEFAULT and take --apply to write; the others
        # are write-by-default + --dry-run.
        if [[ "$cat" == "defi-per-instrument" ]]; then
            : # apply/dry + the chained rebuild are baked into the per-year loop by _script_for ($MODE);
              # a --apply/--dry-run/EXTRA_ARGS append to a compound `for … done; if … fi` string is a syntax
              # error, so BOTH the flag-append and MIGRATION_EXTRA_ARGS below are deliberately suppressed here.
        elif [[ "$cat" == "defi" || "$cat" == "prediction" || "$cat" == "cefi" || "$cat" == "tradfi-cme-options" ]]; then
            [[ "$MODE" == "full" ]] && cmd="$cmd --apply"   # dry = tool default (no flag)
        else
            [[ "$MODE" == "dry" ]] && cmd="$cmd --dry-run"
        fi
        # MIGRATION_EXTRA_ARGS forwards extra flags to the migration tool — for the defi v9 discover→shard
        # flow: `--phase discover` (once per bucket) then N× `--phase migrate --buckets <one>` date-shards.
        [[ "$cat" != "defi-per-instrument" && -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    fi

    echo "Launching $vm_name — $cmd"
    local md="VM_TASK=canonical-migration"
    # The catalogue-canon one-off lives in instruments-service, not MTDS — VM_SERVICE drives which
    # service tarball setup-data-pipeline-vm.sh stages (SERVICE_TARBALLS map).
    local _svc="market_tick_data_service"
    [[ "$cat" == "tradfi-catalogue-canon" ]] && _svc="instruments_service"
    md="${md},VM_SERVICE=${_svc}"
    md="${md},VM_OPERATION=migrate-${cat}"
    # defi-per-instrument shares the DeFi tick bucket + fleet classification — keep the asset group DEFI
    # (not the novel DEFI-PER-INSTRUMENT) so dashboards/heartbeat classify it with the rest of DeFi.
    local _ag; _ag="$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    [[ "$cat" == "defi-per-instrument" ]] && _ag="DEFI"
    # Keep the fleet asset-group TRADFI (not the novel TRADFI-CATALOGUE-CANON) so heartbeat/
    # dashboards classify this VM with the rest of tradfi.
    [[ "$cat" == "tradfi-catalogue-canon" ]] && _ag="TRADFI"
    md="${md},VM_ASSET_GROUP=${_ag}"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_MIGRATION_CMD=${cmd}"
    md="${md},VM_MIGRATION_MODE=${MODE}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    # SHA-pin the code tarballs so the VM provably runs the intended commit (race-proof
    # via setup-data-pipeline-vm.sh authoritative pinned pull). Pass the SHAs in the env
    # at launch: UAC_TARBALL_SHA / UTL_TARBALL_SHA / MTDS_TARBALL_SHA. Unset = floating pull.
    #
    # These gates read the AMBIENT SHELL ENV. That is a genuine foot-gun: forget
    # to export one and the VM floats onto whatever the tarball is at boot, with
    # no signal anywhere that the pin you thought you set was never applied.
    # A floating migration VM is not merely non-reproducible — it can run
    # different code against a half-migrated corpus than the VMs it is sharded
    # alongside. So every repo is ANNOUNCED here, pinned or not, and the unpinned
    # case is a visible WARNING rather than silence.
    _pin_summary=""
    for _pin_key in UAC_TARBALL_SHA UTL_TARBALL_SHA MTDS_TARBALL_SHA; do
        eval "_pin_val=\"\${${_pin_key}:-}\""
        if [[ -n "${_pin_val}" ]]; then
            md="${md},${_pin_key}=${_pin_val}"
            echo "  PIN  ${_pin_key}=${_pin_val:0:12}"
        else
            echo "  WARNING: ${_pin_key} is UNSET — ${vm_name} will pull the FLOATING tarball for this repo." >&2
            echo "           Export ${_pin_key}=<sha> before launch to pin it (recorded as a deliberate float otherwise)." >&2
        fi
        _pin_summary="${_pin_summary} ${_pin_key}=${_pin_val}"
    done

    # Durable pin registry (Leg B): survives this instance's deletion, which is
    # exactly the window a preemption relaunch has to recover from. Written
    # BEFORE instance creation so a VM can never exist without a record.
    # shellcheck disable=SC2086
    lc_write_tarball_pin_record "$vm_name" "$PROJECT" "launch-canonical-migration-vm.sh" ${_pin_summary}

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        # Verify the tarballs this category actually stages (VM_SERVICE-driven), not a fixed list:
        # catalogue-canon runs instruments-service code and never touches MTDS.
        local _fresh_repos=(market-tick-data-service unified-api-contracts unified-trading-library deployment-service)
        [[ "$cat" == "tradfi-catalogue-canon" ]] && _fresh_repos=(instruments-service unified-api-contracts unified-trading-library deployment-service)
        lc_verify_tarball_freshness "$CODE_BUCKET" "${_fresh_repos[@]}" \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        "${PROVISIONING_ARGS[@]}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --scopes=cloud-platform \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=canonical-migration,category="${cat}",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS_LABEL}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
}

case "$ASSET_GROUP" in
    tradfi)
        # tradfi content migration: SHARD_OF>1 with SHARD_INDEX UNSET fans out one VM per shard
        # (canonical-migration-tradfi-<ts>-shard<i>of<N>, all under the registered prefix, disjoint work).
        # A pinned SHARD_INDEX (or SHARD_OF=1) launches exactly one VM (canary / targeted relaunch).
        if [[ "$SHARD_OF" -gt 1 && -z "$SHARD_INDEX_EXPLICIT" ]]; then
            for ((_i = 0; _i < SHARD_OF; _i++)); do
                SHARD_INDEX="$_i"
                VM_NAME_SUFFIX="shard${_i}of${SHARD_OF}"
                _launch tradfi
            done
        else
            _launch tradfi
        fi
        ;;
    cefi|defi|defi-per-instrument|prediction|sports|tradfi-cme-options|tradfi-catalogue-canon) _launch "$ASSET_GROUP" ;;
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
