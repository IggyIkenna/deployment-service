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
#   # GATED MASSIVE PURGE (TRADFI_PURGE_MASSIVE_ONLY=1) — destructive, operator-authorized only.
#   # Runs the migrate pass ALONE over an enumeration grep-filtered to pipeline_mode=batch_massive,
#   # so non-massive objects are not in the input and zero-collateral holds BY CONSTRUCTION. Does NOT
#   # run rebundle/recover (they canonicalize NON-massive data — already complete). The sentinel is
#   # pulled from GCS onto the VM because the tool resolves it with Path(...).is_file() VM-side.
#   # NOTE: MIGRATION_EXTRA_ARGS is REJECTED for cat=tradfi (compound &&-chain would silently drop it).
#   TRADFI_PURGE_MASSIVE_ONLY=1 SHARD_OF=20 MTDS_TARBALL_SHA=<sha> \
#     bash launch-canonical-migration-vm.sh tradfi 2020-01-01 2026-01-30 full
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
#   # candle-census (2026-07-22): READ-ONLY dry-run census of ONE asset_group's processed_candles/
#   # corpus (cefi | defi | tradfi | prediction -- sports is OUT OF SCOPE, its candles live under a
#   # disjoint processed/ root, not processed_candles/). Each VM does a FRESH single-walk gcloud
#   # listing scoped ONLY to gs://<bucket>/processed_candles/** (never raw_tick_data/, never the
#   # bucket root), then runs migrate_candle_canonical_2026_07.py --dry-run (NEVER --apply -- this
#   # category has NO reachable --apply path; $MODE is deliberately IGNORED, it always emits
#   # --dry-run regardless of dry|full) against that enumeration, producing a mapping TSV + reconcile
#   # report, then stages both to gs://${CODE_BUCKET}/canonical-migration-candle-census/. Runs from
#   # $WORKSPACE/mdps (market-data-processing-service) -- the compound `cd ... && gcloud ... &&
#   # python ...` command re-homes the CWD inside the shared canonical-migration dispatch (which cds
#   # to mtds), matching _catalogue_canon_cmd()'s re-homing trick for instruments-service.
#   # START_DATE/END_DATE are cosmetic (VM label only) -- the script scopes its own worklist from the
#   # enumeration. Category names deliberately start with the asset_group token so the VM name stays
#   # under the ALREADY registered "canonical-migration-<ag>-" VM_PREFIX_TO_BUCKET prefix -- no new
#   # registry entry needed.
#   bash launch-canonical-migration-vm.sh cefi-candle-census 2020-01-01 2026-07-22 dry
#
#   # candle-apply (2026-07-22, P7 -- the REAL migration+purge): same corpus/wiring as candle-census
#   # (fresh single-walk -> migrate_candle_canonical_2026_07.py) but $MODE is honored for real: dry ->
#   # --dry-run reclassify-only canary; full -> --apply --quarantine --content-repair (real
#   # copy->verify->delete against production). Shardable via SHARD_OF/SHARD_INDEX exactly like the
#   # tradfi content-migration category. Sequence per the operator ruling: defi -> prediction -> cefi
#   # -> tradfi (tradfi LAST, ~99% id-canonicalisation). ALWAYS run dry first per AG to confirm the
#   # disposition counts before full.
#   bash launch-canonical-migration-vm.sh defi-candle-apply 2020-01-01 2026-07-22 dry
#   bash launch-canonical-migration-vm.sh defi-candle-apply 2020-01-01 2026-07-22 full
#   # sharded fan-out across N=8 VMs for a large corpus (e.g. tradfi):
#   SHARD_OF=8 bash launch-canonical-migration-vm.sh tradfi-candle-apply 2020-01-01 2026-07-22 full
#
#   # defi-relabel (2026-07-23): the Solana dex_pools fake-history relabel-forward migration
#   # (relabel_solana_dex_pools_fake_history.py). Population is EXACTLY 34 known (day, venue) combos
#   # (17 days x {ORCA, RAYDIUM}) -- no --shard-of; shard via MIGRATION_EXTRA_ARGS="--only-day
#   # <colon-list>" instead, one disjoint day sub-range per VM. Colon (":"), NOT comma -- `gcloud
#   # compute instances create --metadata` parses its value as a comma-delimited dict, so a
#   # comma-joined day list breaks gcloud's OWN arg parsing ("Bad syntax for dict arg", adversarial
#   # finding 2026-07-23); the script's `_parse_days()` accepts both, but only ":" survives the
#   # --metadata round-trip. START_DATE/END_DATE are cosmetic (VM labels only) -- the script scopes
#   # its own worklist from the fixed 34-combo population.
#   bash launch-canonical-migration-vm.sh defi-relabel 2025-01-01 2025-01-17 dry
#   MIGRATION_EXTRA_ARGS="--only-day 2025-01-01:2025-01-02:2025-01-03:2025-01-04:2025-01-05" \
#     VM_NAME_SUFFIX=d01to05 bash launch-canonical-migration-vm.sh defi-relabel 2025-01-01 2025-01-17 full
#
#   # defi-marker-cleanup (2026-07-24): in-region READ-ONLY dry-run safety report for
#   # scripts/one_offs/delete_migrated_defi_markers_2026_07_23.py's `_migrated_*` R3 retirement
#   # markers. DRY-RUN ONLY, HARD -- $MODE is ignored, forced to "dry"; there is NO reachable --apply
#   # path through this launcher (--apply is human-executed-only, per the tool's own docstring +
#   # CLAUDE.md's "prod-bucket deletes are human-only" -- never wired through automation, at any
#   # confidence level). START_DATE/END_DATE ARE real here -- they scope the tool's own
#   # --start-date/--end-date discovery window (unlike most cosmetic-label categories above).
#   # RESUME_SEED_GS points at a GCS copy of already-verified progress (e.g. from a prior host run) to
#   # pull down before starting; the run pushes its own progress back to that same path every 2 min (and
#   # once more on exit) so a SPOT preemption never loses more than ~2 min of verification.
#   RESUME_SEED_GS=gs://deployment-scripts-central-element-323112/canonical-migration-defi-marker-cleanup/resume-seed/delete_migrated_defi_markers_2026_07_23.resume.jsonl \
#     bash launch-canonical-migration-vm.sh defi-marker-cleanup 2020-01-01 2026-07-24 dry
#
#   # defi-lst-rates-fold (2026-07-25): in-region runner for
#   # scripts/one_offs/fold_lst_rates_migrated_markers_2026_07_25.py -- folds the missing per-instrument
#   # lst_rates leaf back in for every FLAGGED `_migrated_*` marker (COINBASE/SWELL/MAKER/ETHENA), per
#   # plans/active/defi_dex_pool_symbol_fix_backfill_purge_2026_07_25.md's [OPERATOR] re-verify+purge todo
#   # and its blocking issue doc issues/defi_lst_rates_migrated_marker_unfiltered_live_reader_2026_07_25.md.
#   # FOLD-NOT-PURGE -- copy-only (gcs_copy_object / construct-then-upload), agent-executable per
#   # codex/02-data/gcs-and-manifest-delete-safety-protocol.md Section 5 (this tool never calls a delete
#   # path -- unlike defi-marker-cleanup, which is DRY-RUN ONLY, HARD). Population is the exact,
#   # fully-enumerated 346-marker known-cluster default (no --shard-of; the script's own
#   # --markers-file/--rediscover flags are how you'd scope differently, via MIGRATION_EXTRA_ARGS).
#   # DRY-BY-DEFAULT + --apply for full (same convention as defi-relabel/defi-glued-reshard).
#   # START_DATE/END_DATE are cosmetic (VM labels only) -- the script scopes its own worklist from the
#   # fixed 346-marker population, not a date range.
#   bash launch-canonical-migration-vm.sh defi-lst-rates-fold 2020-01-01 2026-07-25 dry
#   bash launch-canonical-migration-vm.sh defi-lst-rates-fold 2020-01-01 2026-07-25 full
#
#   # tradfi-cme-monolith (2026-07-26): per-contract canonical migration for the ~30
#   # day=*/venue=CME/ticks.parquet monolith objects (migrate_cme_monolith_trades_2026_07_26.py,
#   # market-tick-data-service). Reuses the production classify_databento_symbol + write_tradfi_shard
#   # path -- an only-copy corpus (2026-07-21 reconciliation), so ALWAYS run a CME_DAY canary before
#   # --all-days. DRY-BY-DEFAULT; full embeds --apply --stamp (this launch's own RUN_TS -- mirrors
#   # cefi-late-renames). START_DATE/END_DATE are cosmetic (VM labels only) -- the tool's worklist is
#   # its own hardcoded, live-re-verified 30-day list, not date-range-driven. Category name deliberately
#   # starts with "tradfi-" so the VM name stays under the ALREADY registered
#   # "canonical-migration-tradfi-" VM_PREFIX_TO_BUCKET prefix -- no new registry entry needed.
#   CME_DAY=2026-01-05 bash launch-canonical-migration-vm.sh tradfi-cme-monolith 2026-01-01 2026-01-01 dry
#   CME_DAY=2026-01-05 bash launch-canonical-migration-vm.sh tradfi-cme-monolith 2026-01-01 2026-01-01 full
#   bash launch-canonical-migration-vm.sh tradfi-cme-monolith 2026-01-01 2026-01-01 full   # --all-days
#
#   # tradfi-cme-monolith-delete (2026-07-26): the SEPARATE, gated delete-source phase for the same
#   # tool -- never bundled with the migrate run above. The tool itself refuses to delete a source
#   # object unless the LIVE manifest already shows a captured row for that day. DRY-BY-DEFAULT; full
#   # appends --apply. Always review the dry-run candidate list before ever running full.
#   bash launch-canonical-migration-vm.sh tradfi-cme-monolith-delete 2026-01-01 2026-01-01 dry
#
#   # sports-k1k2-casing-revert (2026-07-27): the K1/K2 casing-REVERT data migration
#   # (issues/sports_k1k2_delete_bundled_with_twin_less_data_2026_07_27.md todo 1,
#   # sports_consolidated_closeout_2026_07_19.md Track C step 3). COPY-ONLY, idempotent, never
#   # deletes the uppercase source (the separate, later, [OPERATOR]-gated delete is Track V).
#   # START_DATE/END_DATE are REAL (not cosmetic) -- they scope both the migrate and report-gen
#   # passes' day range. dry -> a single migrate dry-run scan (no writes, sizing only). full ->
#   # the 3-step compound chain in ONE VM boot: (1) migrate --apply-prod --confirm-prod-write
#   # (copy uppercase->lowercase twins, content-verify), (2) on a clean exit, generate the
#   # content-re-verified manifest report (--reports-dir local, no GCS round-trip needed -- same
#   # VM, same boot), (3) on a clean exit, manifest_swap --apply-prod --confirm-prod-write (ADD
#   # lowercase canonical rows + REMOVE stale uppercase rows, CAS-protected). Same suppression
#   # class as sports-features-purge/tradfi-manifest-cas (compound `... ; exit ${rc}` chain) --
#   # --apply/--dry-run append + MIGRATION_EXTRA_ARGS are both deliberately suppressed.
#   bash launch-canonical-migration-vm.sh sports-k1k2-casing-revert 2020-06-06 2026-07-27 dry
#   bash launch-canonical-migration-vm.sh sports-k1k2-casing-revert 2020-06-06 2026-07-27 full
#
#   # sports-odds-venue-mig (2026-07-27): the odds_horizon_bucket FINE manifest-row venue
#   # migration (migrate_odds_horizon_bucket_venue_to_bookmaker_2026_07_27.py, market-data-
#   # processing-service). Re-stamps the pre-fix ODDS_API vendor sentinel on every FINE
#   # (league_id+timeframe-populated) captured row to the real per-bookmaker venue, read back
#   # from each shard's own bucketed.parquet content -- see
#   # plans/active/issues/mdps_t1_recon_job_oom_failing_7_days_2026_07_26.md Phase 2. Runs from
#   # $WORKSPACE/mdps (VM_SERVICE=market_data_processing_service re-homes the dispatcher's
#   # default `cd $WORKSPACE/mtds` -- same re-homing trick as candle-census/candle-apply).
#   # HEAVY I/O: ~184k individual shard-file reads (measured 2026-07-27) -- this category exists
#   # specifically because that read volume is squarely inside the "heavy I/O never runs from the
#   # operator's local machine" hard rule. START_DATE/END_DATE are cosmetic (VM labels only) -- the
#   # tool re-scans the whole live manifest, not a date range. DRY-BY-DEFAULT (tool default, no
#   # flag) + --apply for full (same convention as defi/cefi/tradfi-cid). The tool's own internal
#   # CAS-retry loop (3 attempts) + pre-apply GCS snapshot + post-write row-count/generation
#   # verification are the write safety -- no extra bash retry wrapper needed here (unlike
#   # tradfi-manifest-cas/-retire, which wrap a script with no internal retry of its own).
#   # MIGRATION_EXTRA_ARGS can pass e.g. "--limit 500" for a bounded canary (never combine --limit
#   # with full/--apply -- the tool itself refuses that combination).
#   bash launch-canonical-migration-vm.sh sports-odds-venue-mig 2026-07-27 2026-07-27 dry
#   bash launch-canonical-migration-vm.sh sports-odds-venue-mig 2026-07-27 2026-07-27 full
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
#   VM_NAME_OVERRIDE=<name>      (or --vm-name <name>) pin the created VM's name instead of the
#                                auto RUN_TS name — single-category launches ONLY (never with `all` /
#                                a SHARD_OF>1 fan-out, which would collide every VM onto one name).
#                                Set automatically by a SPOT-preemption relaunch (RelaunchPreemptedVm,
#                                via lc_write_launch_params) so the checkpoint blob path
#                                migrate_candle_canonical_2026_07.py writes to under the ORIGINAL
#                                vm_name is still found after relaunch.
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
# Deterministic VM-name override (--vm-name / VM_NAME_OVERRIDE), same mechanism as
# launch-mdps-backfill-vm.sh: empty by default -> the auto RUN_TS name below. Set by
# RelaunchPreemptedVm (via the LAUNCH_PARAMS.json env lc_write_launch_params persists) so a
# SPOT-preempted canonical-migration-* VM relaunches under the SAME vm_name -- this migration's
# checkpoint blob path is `vm-logs/{VM_NAME}/MIGRATION_PROGRESS-shard{N}.json` (adversarial review
# 2026-07-22, HIGH finding: without this, a relaunch's fresh RUN_TS-based name can never find the
# checkpoint the preempted VM wrote, so the shard silently restarts from line 0 every preemption).
# Single-category launches only -- the `all` fan-out and the tradfi/tradfi-cid sharded fan-out loops
# must NOT have this set (each launches N VMs from ONE invocation; setting it would collide every
# shard onto one name), which holds in practice since a relaunch always targets exactly one VM.
VM_NAME_OVERRIDE="${VM_NAME_OVERRIDE:-}"

# Pre-parse --env / --vm-name <val> before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

# RESUME_* env fallbacks (SPOT-preemption relaunch support, adversarial review 2026-07-22 HIGH
# finding, mirroring launch-mdps-backfill-vm.sh's already-working pattern): RelaunchPreemptedVm
# re-invokes this launcher with ZERO CLI args, only the env lc_write_launch_params captured -- without
# this fallback a bare re-invocation hits the positional-arg usage-error `exit 2` the review found
# (the relaunch previously had no way to succeed at all). Explicit positional args always win.
ASSET_GROUP="${1:-${RESUME_ASSET_GROUP:-}}"
START_DATE="${2:-${RESUME_START_DATE:-}}"
END_DATE="${3:-${RESUME_END_DATE:-}}"
MODE="${4:-${RESUME_MODE:-dry}}"  # dry | full
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
    # --instance-termination-action=DELETE (not STOP): a SPOT-preemption relaunch (RelaunchPreemptedVm)
    # reuses the EXACT SAME vm_name via VM_NAME_OVERRIDE (see lc_write_launch_params below), so a merely
    # STOPPED (not deleted) instance would still occupy that name and the relaunch's `gcloud compute
    # instances create` would fail with "already exists". Matches every other SPOT launcher in this
    # codebase (launch-defi-backfill-vm.sh, launch-cefi-sharded-backfill.sh) -- STOP here was an
    # inconsistency, not a deliberate choice (vm_fleet_preemption_autorecovery_gap_2026_07_23.md).
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=DELETE)
fi

if [[ -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] <cefi|cefi-dedup-apply|cefi-late-renames|cefi-content-apply|cefi-eu-twin-apply|cefi-bybit-spot-purge|manifest-restamp|tradfi|defi|defi-per-instrument|defi-pi-range|defi-relabel|defi-rebuild|defi-glued-reshard|defi-marker-cleanup|defi-gmx-purge|defi-lst-rates-fold|defi-curve-optimism-reclassify|prediction|sports|sports-features-purge|sports-k1k2-casing-revert|sports-odds-venue-mig|tradfi-cme-options|tradfi-catalogue-canon|tradfi-manifest-cas|tradfi-manifest-retire|tradfi-cme-monolith|tradfi-cme-monolith-delete|cefi-candle-census|defi-candle-census|tradfi-candle-census|prediction-candle-census|cefi-candle-apply|defi-candle-apply|tradfi-candle-apply|prediction-candle-apply|cefi-candle-orphan-sweep|defi-candle-orphan-sweep|tradfi-candle-orphan-sweep|sports-candle-orphan-sweep|prediction-candle-orphan-sweep|all> <start-date> <end-date> [dry|full|smoke (cefi-bybit-spot-purge only)]"
    echo "  manifest-restamp requires RESTAMP_BUCKET=<bucket> in the environment (no asset-group inference)."
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# SECURITY: WORKERS is substituted host-side into compound `&&`-chain command strings that are
# executed VM-side via `bash -c "$VM_MIGRATION_CMD"` under --scopes=cloud-platform (broad GCP
# creds). Because the substitution happens BEFORE that string ever reaches a shell, an unvalidated
# value containing shell metacharacters (e.g. `16; gcloud storage rm -r gs://...`) is executed as
# arbitrary shell syntax on the VM, not merely passed as a --workers argument. Validate as a bare
# positive integer HERE, before any _*_cmd() builder can embed it, so every category (candle-census
# included) is covered by one gate. (Adversarial review 2026-07-22, HIGH finding.)
if [[ -n "${WORKERS:-}" && ! "${WORKERS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: WORKERS must be a positive integer (got: '${WORKERS}')" >&2
    exit 1
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_TS_LABEL="$(date +%Y%m%d-%H%M%S)"

# ── tradfi content-migration (2026-07) sharding + scope knobs ──────────────────────────────────────
# SHARD_INDEX_EXPLICIT records whether the operator PINNED a single shard (launch just that VM) vs left
# it unset (SHARD_OF>1 then FANS OUT one VM per shard). Captured before the :- default masks the distinction.
# Also treated as "explicit" when RESUME_SHARD_INDEX is present (a SPOT-preemption relaunch of one
# specific shard's VM, per the RESUME_* fallback above) -- a relaunch must NEVER re-trigger the N-VM
# fan-out loop just because SHARD_OF>1; it must target exactly the one shard that was preempted.
SHARD_INDEX_EXPLICIT="${SHARD_INDEX+set}${RESUME_SHARD_INDEX:+set}"
SHARD_OF="${SHARD_OF:-${RESUME_SHARD_OF:-1}}"
SHARD_INDEX="${SHARD_INDEX:-${RESUME_SHARD_INDEX:-0}}"
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
# SECURITY: TRADFI_TICK_BUCKET is embedded unquoted-in-effect inside double-quoted `gcloud storage
# ls -r "gs://${bucket}/..."` segments of the same VM-side `bash -c`-executed compound strings as
# WORKERS above -- an embedded `"` breaks the intended quoting and `;`/`&&`/backticks are a second,
# independently-reachable injection point. Validate as a bare GCS bucket name (RFC-compliant subset)
# before it can be embedded anywhere. (Adversarial review 2026-07-22, MEDIUM finding — closed here
# rather than left as a TODO since it is the same one-line validation shape as the WORKERS gate.)
if [[ ! "${TRADFI_TICK_BUCKET}" =~ ^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$ ]]; then
    echo "ERROR: TRADFI_TICK_BUCKET must be a valid GCS bucket name (got: '${TRADFI_TICK_BUCKET}')" >&2
    exit 1
fi

# SECURITY: RESUME_SEED_GS (defi-marker-cleanup category only) is embedded unquoted-in-effect inside
# `gcloud storage cp ${seed_gs} ...` segments of the same VM-side `bash -c`-executed compound string as
# WORKERS/TRADFI_TICK_BUCKET above -- same injection shape, same one-line gate. Only validated when set
# (the category's own default is a hardcoded, already-safe gs:// literal).
if [[ -n "${RESUME_SEED_GS:-}" && ! "${RESUME_SEED_GS}" =~ ^gs://[a-z0-9][a-z0-9._-]{1,61}[a-z0-9](/[A-Za-z0-9._-]+)+$ ]]; then
    echo "ERROR: RESUME_SEED_GS must be a bare gs://bucket/path URI (got: '${RESUME_SEED_GS}')" >&2
    exit 1
fi

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

# Build the GATED massive-purge command (TRADFI_PURGE_MASSIVE_ONLY=1).
#
# Deliberately NOT `full` mode. The canonical content migration is COMPLETE (99.65% canonical, 0 orphan
# — plan tick-25); re-running it is neither authorized nor needed. The ONLY authorized destructive
# action is deleting `pipeline_mode=batch_massive` objects (operator ruling 2026-07-20,
# accepted-permanent-loss, unified-trading-pm@1cc566db6).
#
# ZERO-COLLATERAL BY CONSTRUCTION: the enumeration is grep-filtered to `pipeline_mode=batch_massive`
# BEFORE the tool sees it, so non-massive objects are not in the input at all — there is no code path
# by which a batch_databento or _quarantine/ object can be touched. That is strictly stronger than a
# post-hoc count check. (Measured 2026-07-20: `massive + databento == total_parquet` on every sampled
# day, so the filter is exact and total.) Only the migrate pass runs — NOT rebundle/recover, which
# exist to canonicalize NON-massive data and would move objects this purge must leave alone.
#
# The sentinel is resolved by the tool with `Path(...).is_file()` ON THE VM, so it is pulled from GCS
# rather than inlined (its text contains commas; --metadata is comma-delimited).
_tradfi_massive_purge_cmd() {
    local vm_name="$1"
    local base="market_tick_data_service.scripts"
    local sh="--shard-of ${SHARD_OF} --shard-index ${SHARD_INDEX}"
    local work="/home/ikennaigboaka/workspace/tradfi-canonical-migration"
    local enum="${work}/enumeration.txt"
    local enum_m="${work}/enumeration_massive.txt"
    local sent="${work}/massive_purge_authorization.sentinel"
    local mapd="${work}/mappings"
    local stage="gs://${CODE_BUCKET}/canonical-migration-tradfi/${RUN_TS}/${vm_name}/mappings/"
    local walk="gs://${TRADFI_TICK_BUCKET}/raw_tick_data/**"
    local sent_gs="${MASSIVE_SENTINEL_GS:-gs://${CODE_BUCKET}/canonical-migration-tradfi/sentinels/massive_purge_authorization_2026_07_20.sentinel}"
    local limit_flag=""
    [[ "${LIMIT:-0}" -gt 0 ]] && limit_flag="--limit ${LIMIT}"
    printf '%s' "mkdir -p ${mapd} && gcloud storage cp ${sent_gs} ${sent} && gcloud storage ls -r \"${walk}\" > ${enum} && grep 'pipeline_mode=batch_massive' ${enum} > ${enum_m} && echo TRADFI_MASSIVE_ENUM_LINES=\$(wc -l < ${enum_m}) && python -u -m ${base}.migrate_tradfi_canonical_2026_07 --apply --enumeration ${enum_m} --out ${mapd}/migrate_mapping.tsv ${sh} ${limit_flag} --workers ${WORKERS:-24} --purge-massive --massive-backfill-verified ${sent} && gcloud storage cp -r ${mapd}/ ${stage}"
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
# Roll-up producer (2026-07-22): build_instrument_catalogue.py --asset-group tradfi --mode incremental
# — regenerates catalog.parquet from the per-date instrument_availability snapshots and re-evaluates the
# UAC is_mvp() predicate over every row, so the +409 MVP-scope expansion (VIX FUTURE/treasury INDEX/KRW/
# crypto futures, landed uac@22e6a534 this same closeout) actually shows mvp=True in the SERVED catalogue
# instead of staying frozen at the old 70,930-row set. DIFFERENT script from tradfi-catalogue-canon
# (canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py, which fixes ids WITHIN the by_date snapshots —
# already ran earlier this closeout) — this one is the ROLL-UP that PRODUCES catalog.parquet FROM those
# snapshots. Monotonic-guard promotion is baked into the script itself (temp-object + row-count->=-current
# assert before the real promote-copy) so this launcher does not need its own extra safety wrapper.
# --mode incremental (the tool's own default) is deliberately NOT overridden to --mode full: MVP is a
# boolean re-evaluated over whatever rows already exist in the frozen catalogue + the self-widening
# trailing window, not a property of the by_date snapshots themselves, so a full re-aggregation buys
# nothing extra for this specific ask and costs a full historical re-walk.
_catalogue_promote_cmd() {
    local dry_flag=""
    [[ "$MODE" != "full" ]] && dry_flag=" --dry-run"
    printf '%s' "cd ${VM_WORKSPACE}/instruments && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} python -u scripts/build_instrument_catalogue.py --asset-group tradfi${dry_flag}"
}

# cefi-dedup-apply (2026-07-24) — the CeFi Surface-C manifest de-dup/canonicalisation one-off
# (`complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py`, instruments-service). Repeated direct
# in-session runs of this ~10.6M-row, multi-GB-RSS script were killed (SIGTERM/143) by the shared
# host's `earlyoom` daemon under concurrent multi-agent memory contention — genuinely a resource
# problem, not a code/logic one (independently verified via a completed full-corpus investigation
# run + a fast local synthetic-data test first). Moving it to an ISOLATED in-region VM (same
# `VM_SERVICE=instruments_service` re-homing trick as `tradfi-catalogue-canon`) removes that
# contention entirely. Self-contained `cd ... && python ...` chain; `--apply` is embedded per-MODE
# by the builder (the tool's own STOP-ON-SURPRISE / chain-drop-lossy-tolerance gates are the write
# safety, not this launcher). **DRAIN GATE (operator/caller responsibility, NOT automated by this
# launcher)**: `full` mode MUST run only while
# `uts-prod-manifest-consolidator-market-data-cefi-cron` is PAUSED (`gcloud scheduler jobs pause/
# resume --location=asia-northeast1`) — verify directly via `gcloud scheduler jobs describe`
# immediately before launch and RESUME it once the VM's `VM_SHUTDOWN_ON_COMPLETION` fires; a
# forgotten resume silently starves the manifest of live updates (this exact mistake already
# happened once this session, see `issues/cefi_chain_drop_root_cause_and_heavy_io_vm_rule_2026_07_24.md`
# Finding 3). START_DATE/END_DATE are cosmetic (VM labels only) — the tool re-scans the whole live
# manifest, not a date range.
#   bash launch-canonical-migration-vm.sh cefi-dedup-apply 2026-07-24 2026-07-24 dry
#   bash launch-canonical-migration-vm.sh cefi-dedup-apply 2026-07-24 2026-07-24 full
_cefi_dedup_apply_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    printf '%s' "cd ${VM_WORKSPACE}/instruments && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py${apply_flag}"
}

# defi-curve-optimism-reclassify (2026-07-27) — the one-shot CURVE/OPTIMISM dex_pool_swaps
# `attempted_failed` -> `empty_confirmed[EXPECTED_SUBGRAPH_DEINDEXED]` retroactive reclassification
# (`reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py`, instruments-service). Same
# `VM_SERVICE=instruments_service` re-homing trick as cefi-dedup-apply/tradfi-catalogue-canon — the
# script's own manifest-index read-transform-write is the heavy-I/O op this launcher's canonical-
# migration dispatch exists for (per the heavy-I/O hard rule, never run --apply from the operator's
# local machine). Self-contained `cd ... && python ...` chain; --apply is embedded per-MODE by the
# builder (the tool's own backup-then-write + idempotent design is the write safety, not this
# launcher). Dry-run already verified 144 matching rows against live prod (see
# issues/defi_curve_optimism_subgraph_no_allocations_2026_07_15.md "Verified live (2026-07-24)"); 2
# prior VM-launch attempts (2026-07-24) both failed on setup-data-pipeline-vm.sh's canonical-
# migration branch hardcoding `cd $WORKSPACE/mtds` regardless of VM_SERVICE — fixed
# deployment-service@0ed2ca6 before this category existed. START_DATE/END_DATE are cosmetic (VM
# labels only) — the script re-scans the whole live manifest for the fixed (venue, chain, data_type)
# cell, not a date range.
#   bash launch-canonical-migration-vm.sh defi-curve-optimism-reclassify 2026-07-27 2026-07-27 dry
#   bash launch-canonical-migration-vm.sh defi-curve-optimism-reclassify 2026-07-27 2026-07-27 full
_curve_optimism_reclassify_cmd() {
    local mode_flag="--dry-run"
    [[ "$MODE" == "full" ]] && mode_flag="--apply"
    printf '%s' "cd ${VM_WORKSPACE}/instruments && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py ${mode_flag}"
}

# cefi-late-renames (2026-07-24) — cefi single-instrument FILENAME wire->canonical rename
# (`migrate_cefi_tardis_filename_canonical_2026_07_17.py`, market-tick-data-service), scoped to the
# "LATE window" the fresh 4-surface reverify pinpointed: Surface A canonical-fraction was still
# 94-95% through 2025-06/08/10/11-12, then dropped to 67.04% by 2025-12-15, 33.02% by 2026-02-01,
# 23.99% by 2026-05-01 — see plans/active/cefi_consolidated_closeout_2026_07_18.md and
# issues/cefi_chain_drop_root_cause_and_heavy_io_vm_rule_2026_07_24.md Finding 4. An UNSCOPED run (no
# --start-date) walks the full 2019-03-30..today corpus day-by-day at ~65-90s/day — measured ETA
# 48-67 HOURS (killed at ~21min/day-14 in a direct local attempt this session, zero mutation
# occurred). **START_DATE/END_DATE (the launcher's own positional args) are honored for REAL here**
# (unlike cefi-dedup-apply, where they're cosmetic) — pass the actual LATE-window range, e.g.
# 2025-11-01..today, not today..today. `--venue` can be added via MIGRATION_EXTRA_ARGS to further
# scope to one colliding venue. `--stamp` (required by the tool for --apply) is derived from this
# launch's own RUN_TS so every apply run gets a unique, traceable backup-filename stamp.
#   bash launch-canonical-migration-vm.sh cefi-late-renames 2025-11-01 2026-07-24 dry
#   MIGRATION_EXTRA_ARGS="--venue BITFINEX-FUTURES" bash launch-canonical-migration-vm.sh cefi-late-renames 2025-11-01 2026-07-24 full
_cefi_late_renames_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply --stamp ${RUN_TS}"
    local workers_flag=""
    [[ -n "${WORKERS:-}" ]] && workers_flag=" --workers ${WORKERS}"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/migrate_cefi_tardis_filename_canonical_2026_07_17.py --start-date ${START_DATE} --end-date ${END_DATE}${workers_flag}${apply_flag}"
}

# cefi-content-apply (2026-07-27) — D4 SCRIPT 1, cefi parquet CONTENT `instrument_id` column
# catalogue-canonicalisation (migrate_cefi_content_instrument_id_catalogue_2026_07_17.py,
# market-tick-data-service). Rewrites the frozen parquet instrument_id COLUMN corpus-wide via the
# shared catalogue resolver so column (this)/filename (Script 2, cefi-late-renames)/manifest-key
# (Script 3, cefi-dedup-apply) all agree byte-for-byte. DRY-BY-DEFAULT (tool default, no flag); full
# embeds --apply, which the TOOL ITSELF refuses unless the Phase -1 catalogue verify gate is green (0
# cefi :PERP: ids AND 0 id!=canonical, measured live) — no extra gate needed here. START_DATE/END_DATE
# are honored FOR REAL (mirrors cefi-late-renames, unlike cefi-dedup-apply where they're cosmetic) —
# the tool's own default is the full corpus (2019-03-30..today) if left unset; pass a real sub-range
# for a sharded fan-out (further scope to one venue via MIGRATION_EXTRA_ARGS `--venue <V>`).
# --workers defaults to the tool's own pool-safe 12 (urllib3 pool_maxsize=10 headroom, market-tick-
# data-service@d47609ec) — override via WORKERS only in tandem with re-verifying _GCS_HTTP_POOL_MAXSIZE.
# DRAIN GATE is the caller's responsibility (no live cefi writers) — same as cefi-dedup-apply.
#   bash launch-canonical-migration-vm.sh cefi-content-apply 2019-03-30 2026-07-27 dry
#   MIGRATION_EXTRA_ARGS="--venue BYBIT" bash launch-canonical-migration-vm.sh cefi-content-apply 2019-03-30 2026-07-27 full
_cefi_content_apply_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    local workers_flag=""
    [[ -n "${WORKERS:-}" ]] && workers_flag=" --workers ${WORKERS}"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/migrate_cefi_content_instrument_id_catalogue_2026_07_17.py --start-date ${START_DATE} --end-date ${END_DATE}${workers_flag}${apply_flag}"
}

# cefi-eu-twin-apply (2026-07-27) — D4 SCRIPT 4, drop stale expected_unattempted MAIN-INDEX twins for
# native-canonical cefi venues (drop_cefi_eu_twins_native_canonical_2026_07_17.py, instruments-service;
# EXTENDED-STARKNET dominant, ~9,850 rows measured 2026-07-25 dry-run). Single whole-MAIN-index
# read/snapshot/write, same shape as cefi-dedup-apply. START_DATE/END_DATE are COSMETIC (VM labels
# only — the tool re-scans the whole live main index, not a date range). DRY-BY-DEFAULT (tool default,
# no flag); full embeds --apply (snapshot-first to `_index/snapshots/pre_d4_<ts>/`, STOP-ON-SURPRISE
# bounded to [8000,15000] total drops — the tool's own gate, not this launcher's). DRAIN GATE is the
# caller's responsibility — same as cefi-dedup-apply, do not run --apply with live cefi writers up.
#   bash launch-canonical-migration-vm.sh cefi-eu-twin-apply 2026-07-27 2026-07-27 dry
#   bash launch-canonical-migration-vm.sh cefi-eu-twin-apply 2026-07-27 2026-07-27 full
_cefi_eu_twin_apply_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    printf '%s' "cd ${VM_WORKSPACE}/instruments && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/drop_cefi_eu_twins_native_canonical_2026_07_17.py${apply_flag}"
}

# CME monolith trades -> canonical per-underlying migration (2026-07-26,
# migrate_cme_monolith_trades_2026_07_26.py, market_tick_data_service package module -- invoked via
# -m, NOT a standalone scripts/ one-off). Migrates the ~30 real day=*/venue=CME/ticks.parquet
# monolith objects (Databento MBP-0/trades, all CME symbols mixed per day, no Hive partitioning)
# into canonical per-contract/chain form via the SAME production classifier
# (classify_databento_symbol) and write path (write_tradfi_shard) live adapters use, then additively
# registers manifest rows. ONLY-COPY corpus (2026-07-21 reconciliation report) -- CME_DAY narrows to
# one real day (canary/pilot; ALWAYS run before --all-days); unset = --all-days (every real day, the
# tool's own static 30-day worklist, live-re-verified). DRY-BY-DEFAULT (dry = tool default, no flag);
# full embeds --apply --stamp (this launch's own RUN_TS, so every apply run gets a unique traceable
# backup-filename stamp -- mirrors cefi-late-renames above). START_DATE/END_DATE are cosmetic (VM
# labels only) -- the tool's worklist is hardcoded, not date-range-driven.
_cme_monolith_migrate_cmd() {
    local day_flag="--all-days"
    [[ -n "${CME_DAY:-}" ]] && day_flag="--day ${CME_DAY}"
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply --stamp ${RUN_TS}"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u -m market_tick_data_service.scripts.migrate_cme_monolith_trades_2026_07_26 ${day_flag}${apply_flag}"
}

# CME monolith delete-source phase -- SEPARATE from migrate above, never bundled (only-copy data).
# The tool itself refuses to delete any source object unless the LIVE manifest already shows a
# captured row for that day (re-verified live, not from a stale ledger), so this launcher needs no
# extra gate of its own. DRY-BY-DEFAULT; full appends --apply. Always review the dry-run candidate
# list before ever running full.
_cme_monolith_delete_source_cmd() {
    local day_flag="--all-days"
    [[ -n "${CME_DAY:-}" ]] && day_flag="--day ${CME_DAY}"
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u -m market_tick_data_service.scripts.migrate_cme_monolith_trades_2026_07_26 ${day_flag} --delete-source${apply_flag}"
}

# defi-glued-reshard (2026-07-23): re-run of scripts/one_offs/reshard_glued_defi_ids_2026_07_21.py
# against a fresh local snapshot of the consolidated index -- that one-off's own docstring says it's
# "kept ... in case new glued ids appear before the forward write-path fix ships", and a post-rebuild
# recount (in-region VM read, 2026-07-23) found exactly that: 34 captured rows across
# lending_indices/liquidations (KAMINO + several lending-protocol chains) matching the glued-timestamp
# regex, distinct from the LST/oracle-price class the 2026-07-21 sweep resolved to 0. DRY-BY-DEFAULT
# (dry = the tool's own default, no flag); full appends --apply. START_DATE/END_DATE are cosmetic (VM
# labels only) -- the tool re-scans the whole snapshot, not a date range.
_glued_reshard_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && mkdir -p /tmp/defi_snap && gcloud storage cp gs://market-data-tick-defi-prd-central-element-323112/_index/availability_index.parquet /tmp/defi_snap/defi_index.parquet && GCP_PROJECT_ID=${PROJECT} python -u scripts/one_offs/reshard_glued_defi_ids_2026_07_21.py /tmp/defi_snap${apply_flag}"
}

# defi-marker-cleanup (2026-07-24): in-region runner for the READ-ONLY safety report
# scripts/one_offs/delete_migrated_defi_markers_2026_07_23.py produces over the `_migrated_*` R3
# retirement markers (see that script's own module docstring). HARD, NON-NEGOTIABLE: this category
# is DRY-RUN ONLY -- $MODE is deliberately IGNORED in _launch() (mirrors *-candle-census) and there is
# NO reachable --apply code path through this launcher, ever, regardless of what an operator passes.
# The tool's own docstring + CLAUDE.md's "prod-bucket deletes are human-only" both say the same thing:
# no agent invokes --apply against this bucket autonomously, at any confidence level. --apply, if it is
# ever run, is a human typing it directly on a terminal -- never wired through automation.
#
# Moved off an operator/agent host onto an in-region VM per the (2026-07-24) "heavy I/O never runs from
# the operator's local machine" rule: a 356k-marker full-corpus walk from a non-GCP host measured a
# decelerating throughput pattern (cross-cloud GCS round-trip latency stacking on top of shared-host CPU
# contention) matching this file's own documented history for exactly this class of workload (see the
# tradfi-catalogue-canon comment above -- an earlier off-region attempt on an analogous corpus-scale walk
# hit the same decelerating pattern before this launcher moved that class of job onto a same-zone VM).
#
# RESUME-LOG CONTINUITY: the tool's own --resume-log is a LOCAL VM-disk JSONL file, so a bare SPOT
# preemption (--instance-termination-action=DELETE wipes the disk) would silently lose every marker
# verified since this VM's own boot -- the opposite of what makes SPOT safe for an idempotent/resumable
# job. RESUME_SEED_GS (if set) points at a GCS copy of progress already made (e.g. from a prior host run)
# that gets pulled down BEFORE the tool starts; a background loop pushes the live resume-log back to that
# same GCS path every 2 minutes for the whole run (plus once more on exit), so a preemption never loses
# more than ~2 minutes of verification, and a relaunch (or a fresh RESUME_SEED_GS-pointed rerun) resumes
# from there instead of re-verifying the whole corpus. Best-effort (`|| true` throughout) -- a transient
# GCS hiccup on the sync must never abort or crash the actual verification run.
#
# START_DATE/END_DATE are REAL here (unlike most cosmetic-label categories above) -- they scope the
# tool's own --start-date/--end-date discovery window. WORKERS (already validated as a bare positive
# integer host-side, see the WORKERS gate near the top of this file) sets --verify-workers; a dedicated
# same-zone VM has no shared-host contention, so a higher value than the 24-worker default measured on a
# contended host is reasonable (default here: 48).
_delete_migrated_defi_markers_cmd() {
    local work="/home/ikennaigboaka/workspace/defi-marker-cleanup"
    local resume="${work}/delete_migrated_defi_markers_2026_07_23.resume.jsonl"
    local seed_gs="${RESUME_SEED_GS:-gs://${CODE_BUCKET}/canonical-migration-defi-marker-cleanup/resume-seed/delete_migrated_defi_markers_2026_07_23.resume.jsonl}"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && mkdir -p ${work} && (gcloud storage cp ${seed_gs} ${resume} || true); (while true; do sleep 120; gcloud storage cp ${resume} ${seed_gs} >/dev/null 2>&1 || true; done) & SYNC_PID=\$!; python -u scripts/one_offs/delete_migrated_defi_markers_2026_07_23.py --project-id ${PROJECT} --start-date ${START_DATE} --end-date ${END_DATE} --resume-log ${resume} --verify-workers ${WORKERS:-48} --discover-workers 32; RC=\$?; kill \$SYNC_PID 2>/dev/null || true; gcloud storage cp ${resume} ${seed_gs} || true; exit \$RC"
}

# DeFi GMX venue-removal purge (2026-07-25) — in-region runner for
# scripts/one_offs/purge_gmx_venue_removal_2026_07_25.py, per
# plans/active/defi_gmx_venue_removal_2026_07_25.md's [OPERATOR] GCS-purge todo. Moved off an
# operator laptop per the "heavy I/O never runs from the operator's local machine" rule: a direct
# local run measured TWO distinct failure modes reading the ~1GB `_index/availability_index.parquet`
# -- (a) a deterministic `ChunkedEncodingError` at the EXACT same byte offset both attempts
# (268,435,456 = precisely 256 MiB, a local proxy/connection cutoff, not random flakiness), and (b)
# even a chunked/ranged workaround lost a race against the market-data-defi consolidator's 1-minute
# rewrite cycle (a pinned generation 404'd mid-download because the whole download took long enough
# for the object to rotate underneath it) -- both symptoms of round-trip latency/bandwidth to this
# bucket being too slow from off-region; a same-zone VM clears the full 1GB well inside one cycle.
#
# $MODE IS honored for real (unlike defi-marker-cleanup, which is dry-only): dry -> --dry-run (safe,
# no pause needed, the tool's own default posture); full -> --apply, which the TOOL ITSELF hard-aborts
# unless the caller has ALREADY paused uts-prod-manifest-consolidator-market-data-defi-cron (verified
# live via `gcloud scheduler jobs describe` inside the script) -- this launcher does not drive the
# pause/resume/durability-watch dance itself, it only runs ONE step of that human-executed sequence
# per VM boot (see the tool's own module docstring for the full sequence). `verify` (2026-07-26) ->
# --verify-only -- re-discovers GCS objects + re-reads the manifest and confirms both are zero,
# WITHOUT the consolidator-pause precondition (read-only) -- for the post-resume durability watch
# (module docstring step 6), which must run repeatedly and shouldn't require the cron paused each
# time. Runs on a VM for the same reason as everything else here: the manifest index is too large
# for a reliable plain local download (confirmed 256 MiB local-network cutoff).
_gmx_purge_cmd() {
    local mode_flag=""
    case "$MODE" in
        full) mode_flag=" --apply" ;;
        verify) mode_flag=" --verify-only" ;;
        *) mode_flag=" --dry-run" ;;
    esac
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/one_offs/purge_gmx_venue_removal_2026_07_25.py --project-id ${PROJECT}${mode_flag}"
}

# DeFi lst_rates FLAGGED-marker fold (2026-07-25) -- in-region runner for
# scripts/one_offs/fold_lst_rates_migrated_markers_2026_07_25.py. FOLD-NOT-PURGE: the tool's own
# --apply mode only ever WRITES a sibling per-instrument leaf (gcs_copy_object true-copy, or a
# construct-then-upload merge onto an existing leaf) -- it never renames or deletes the marker itself
# and never calls a delete path, so this is agent-executable per
# codex/02-data/gcs-and-manifest-delete-safety-protocol.md Section 5 (unlike defi-marker-cleanup, which
# is DRY-RUN ONLY, HARD, because ITS --apply is a prod-bucket delete). $MODE IS honored for real here
# (dry -> tool default report-only; full -> --apply, embedded by this builder) -- no consolidator-drain
# gate is needed (unlike defi-gmx-purge/cefi-bybit-spot-purge), since a fold-only write race with the
# manifest consolidator is harmless (it only ever ADDS a new leaf, never mutates/removes an existing
# manifest-tracked object). Population defaults to the tool's own exact, fully-enumerated 346-marker
# known-cluster population (COINBASE/SWELL/MAKER/ETHENA) -- no --shard-of; MIGRATION_EXTRA_ARGS forwards
# --markers-file/--limit/--workers overrides straight to the tool for a narrower re-run or smoke-test.
# START_DATE/END_DATE are cosmetic (VM labels only) -- the tool scopes its own worklist from the fixed
# population, not a date range.
_lst_rates_fold_cmd() {
    local apply_flag=""
    [[ "$MODE" == "full" ]] && apply_flag=" --apply"
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/one_offs/fold_lst_rates_migrated_markers_2026_07_25.py --project-id ${PROJECT} --workers ${WORKERS:-16}${apply_flag}"
}

# cefi-bybit-spot-purge -- BYBIT-SPOT spot-nonsense manifest row purge
# (delete_bybit_spot_spot_nonsense_manifest_2026_07_07.py). Same rationale as
# _gmx_purge_cmd for running on a VM: the consolidated cefi manifest index is
# currently under the 256 MiB local-read cutoff (a plain download works
# locally), but it is still a slow multi-minute round-trip off-region -- a
# same-zone VM is faster and gives fleet log-tee/monitoring parity, not a
# correctness requirement. $MODE has THREE values for this category (every
# other category is dry|full only): dry -> no flag (tool default, read-only);
# smoke -> --smoke (deletes ONE shard, writes, force-consolidates, then
# exits -- review the VM's run.log before running `full`); full -> --apply
# (all ~53,934 rows). Both smoke and full are real prod-manifest writes that
# the TOOL ITSELF hard-aborts unless uts-prod-manifest-consolidator-market-
# data-cefi-cron is ALREADY paused (verified live via `gcloud scheduler jobs
# describe` inside the script, mirroring _gmx_purge_cmd's tool) -- this
# launcher does not drive the pause/resume dance itself, only one step per
# VM boot (see the tool's own module docstring for the full sequence).
_bybit_spot_purge_cmd() {
    local mode_flag=""
    case "$MODE" in
        full) mode_flag=" --apply" ;;
        smoke) mode_flag=" --smoke" ;;
        *) mode_flag="" ;;
    esac
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/delete_bybit_spot_spot_nonsense_manifest_2026_07_07.py${mode_flag}"
}

# manifest-restamp -- run restamp_manifest_consolidator_2026_07_26.py once,
# in-region, for a bucket whose canonical index is too large for a plain
# local download (matches the DEFI manifest's already-confirmed 256 MiB
# local-read cutoff, same root cause as _gmx_purge_cmd). One-time remediation
# tool: 2026-07-26 found BOTH the GMX and bybit purge scripts' own
# force-consolidate calls silently failed (first: missing setup_events
# bootstrap; then, after fixing that: PermissionDenied on pubsub.topics.publish
# when mirroring the scheduled cron's own PubSubEventSink bootstrap -- this
# VM class's service account has never needed that permission before) after
# their row-deletes had already landed correctly. The script this invokes
# uses mode="local" event logging (no sink, no IAM dependency) instead of
# the bare `python -m unified_trading_library.manifest_consolidator` CLI's
# own hardcoded PubSubEventSink bootstrap, which is what hit the IAM wall.
# $RESTAMP_BUCKET is required (no asset-group inference -- the bucket IS the
# whole point of this category). $MODE is ignored; there is no dry-run
# distinction for a bare --force re-stamp of already-correct data.
_manifest_restamp_cmd() {
    if [[ -z "${RESTAMP_BUCKET:-}" ]]; then
        echo "ERROR: manifest-restamp requires RESTAMP_BUCKET=<bucket> in the environment." >&2
        return 1
    fi
    printf '%s' "cd ${VM_WORKSPACE}/mtds && GCP_PROJECT_ID=${PROJECT} DEPLOYMENT_ENV=${DEPLOYMENT_ENV} CLOUD_PROVIDER=gcp UNIFIED_ENVIRONMENT=${DEPLOYMENT_ENV} CLOUD_MOCK_MODE=false python -u scripts/one_offs/restamp_manifest_consolidator_2026_07_26.py --bucket ${RESTAMP_BUCKET}"
}

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

# Build the candle-census command (2026-07-22): a READ-ONLY dry-run census of ONE asset_group's
# processed_candles/ corpus. ONE fresh single-walk on the VM -> enumeration, then ONE dry-run pass
# of migrate_candle_canonical_2026_07.py over that snapshot, then stage the mapping TSV + reconcile
# report to GCS. Emitted as a single COMMA-FREE compound `&&` chain (gcloud --metadata is comma-
# delimited; see TRADFI_TICK_BUCKET note above). The dispatcher (setup-data-pipeline-vm.sh) ALWAYS
# `cd`s to $WORKSPACE/mtds for VM_TASK=canonical-migration regardless of VM_SERVICE, so the leading
# `cd ${VM_WORKSPACE}/mdps` re-homes into the staged market-data-processing-service tree (matching
# _catalogue_canon_cmd()'s re-homing trick for instruments-service). The migration script lives at
# scripts/ (not inside the installed package), so it is invoked as a PLAIN SCRIPT
# (`python -u scripts/migrate_candle_canonical_2026_07.py`), NOT `-m` -- the FIRST literal `python `
# token in the whole command is that invocation (cd/gcloud/echo/mkdir contain no `python ` substring),
# so setup-data-pipeline-vm.sh's venv string-replace lands on the intended token.
#
# HARD: $MODE is DELIBERATELY IGNORED here -- this category always emits --dry-run regardless of
# dry|full, so it has NO reachable code path to --apply, ever. --dry-run is the script's own default
# anyway (mutually exclusive with --apply per its argparse group), but it is passed explicitly so the
# "this launcher category can never apply" invariant is visible in the emitted command.
#
# Scoped ONLY to gs://<bucket>/processed_candles/** for the ONE requested asset_group -- never
# raw_tick_data/, never the bucket root, never more than one asset_group per VM/command.
_candle_census_cmd() {
    local ag="$1"       # cefi | defi | tradfi | prediction
    local vm_name="$2"
    local bucket
    case "$ag" in
        cefi)       bucket="market-data-tick-cefi-${_ENV_SHORT}-${PROJECT}" ;;
        defi)       bucket="market-data-tick-defi-${_ENV_SHORT}-${PROJECT}" ;;
        tradfi)     bucket="${TRADFI_TICK_BUCKET}" ;;
        # CRITICAL fix (adversarial review 2026-07-22): prediction's real bucket abbreviation is
        # "pred", NOT "prediction" -- the bucket is market-data-tick-pred-<env>-<project>
        # (verified via resolve_bucket_name / vm_prefix_registry.py's _TICK_PRED and
        # configs/cloud-providers.yaml). The "prediction" spelling used here previously does not
        # exist (404 BucketNotFoundException), so the category produced zero census output.
        prediction) bucket="market-data-tick-pred-${_ENV_SHORT}-${PROJECT}" ;;
    esac
    local work="/home/ikennaigboaka/workspace/candle-census-${ag}"
    local enum="${work}/enumeration.txt"
    local mapd="${work}/mappings"
    local out="${mapd}/candle_census_mapping.tsv"
    local stage="gs://${CODE_BUCKET}/canonical-migration-candle-census/${RUN_TS}/${vm_name}/"
    # `\$(...)` stays LITERAL in the metadata value (evaluated by the VM's bash), while ${...}
    # launcher locals expand host-side. No commas anywhere in the emitted string.
    printf '%s' "cd ${VM_WORKSPACE}/mdps && mkdir -p ${mapd} && gcloud storage ls -r \"gs://${bucket}/processed_candles/**\" > ${enum} && echo CANDLE_CENSUS_ENUM_LINES=\$(wc -l < ${enum}) && python -u scripts/migrate_candle_canonical_2026_07.py --dry-run --enumeration ${enum} --out ${out} --workers ${WORKERS:-16} && gcloud storage cp -r ${mapd}/ ${stage}"
}

# Build the candle-apply command (2026-07-22, P7): the REAL migration+purge pass over ONE
# asset_group's processed_candles/ corpus, following the P0 census + the candle_feature_canonical_
# path_divergence_2026_07_20 issue doc's todos 12/14/17/18 (resume checkpoint + classifier fixes,
# all shipped mdps@efa559a/6b9ee49 before this category was built). ONE fresh single-walk on the VM
# -> enumeration (write-contention-free: the pre-migration drain already stopped every VM that
# writes to this asset_group's candles/manifest), then ONE migrate_candle_canonical_2026_07.py pass,
# then stage the mapping TSV + reconcile report to GCS. Unlike *-candle-census (always --dry-run, no
# reachable --apply), THIS category reads $MODE for real: full -> --apply --quarantine
# --content-repair (the actual copy->verify->delete migration+purge against production -- "purge" is
# the operator's own word for this step, hence both gates ON, not left to a follow-up flag); dry ->
# --dry-run (a fresh reclassify-only run, e.g. to re-verify the census disposition counts against the
# corpus before committing to --apply on that AG). Sharded like the tradfi content-migration category
# (--shard-of/--shard-index baked directly from the globals below) -- NOT routed through
# MIGRATION_EXTRA_ARGS, since this emits a compound `&&` chain and a trailing append would silently
# land on the final `gcloud storage cp` rather than the python pass (same reason as the tradfi/
# candle-census branches).
_candle_apply_cmd() {
    local ag="$1"       # cefi | defi | tradfi | prediction
    local vm_name="$2"
    local bucket
    case "$ag" in
        cefi)       bucket="market-data-tick-cefi-${_ENV_SHORT}-${PROJECT}" ;;
        defi)       bucket="market-data-tick-defi-${_ENV_SHORT}-${PROJECT}" ;;
        tradfi)     bucket="${TRADFI_TICK_BUCKET}" ;;
        prediction) bucket="market-data-tick-pred-${_ENV_SHORT}-${PROJECT}" ;;
    esac
    local mode_flag gate_flags
    if [[ "$MODE" == "full" ]]; then
        mode_flag="--apply"
        gate_flags=" --quarantine --content-repair"
    else
        mode_flag="--dry-run"
        gate_flags=""
    fi
    local sh="--shard-of ${SHARD_OF} --shard-index ${SHARD_INDEX}"
    local limit_flag=""
    [[ "${LIMIT:-0}" -gt 0 ]] && limit_flag=" --limit ${LIMIT}"
    local work="/home/ikennaigboaka/workspace/candle-apply-${ag}"
    local enum="${work}/enumeration.txt"
    local mapd="${work}/mappings"
    local out="${mapd}/candle_apply_mapping.tsv"
    local stage="gs://${CODE_BUCKET}/canonical-migration-candle-apply/${RUN_TS}/${vm_name}/"
    # `\$(...)` stays LITERAL in the metadata value (evaluated by the VM's bash), while ${...}
    # launcher locals expand host-side. No commas anywhere in the emitted string.
    printf '%s' "cd ${VM_WORKSPACE}/mdps && mkdir -p ${mapd} && gcloud storage ls -r \"gs://${bucket}/processed_candles/**\" > ${enum} && echo CANDLE_APPLY_ENUM_LINES=\$(wc -l < ${enum}) && python -u scripts/migrate_candle_canonical_2026_07.py ${mode_flag} --enumeration ${enum} --out ${out} ${sh}${limit_flag} --workers ${WORKERS:-16}${gate_flags} && gcloud storage cp -r ${mapd}/ ${stage}"
}

# Build the candle-orphan-sweep command (2026-07-27, todo 1 of
# mdps_features_ml_strategy_orphan_sweep_tooling_gap_2026_07_27.md) — READ-ONLY GCS->manifest
# orphan sweep for ONE asset_group's processed_candles/ corpus (market-data-processing-
# service/scripts/candle_orphan_sweep.py). Unlike candle-census/candle-apply, this tool does
# its OWN internal single walk + bucket resolution (no pre-built enumeration file needed) —
# the launcher only needs to invoke it, not stage a `gcloud storage ls` enumeration first.
# $MODE is DELIBERATELY IGNORED (mirrors *-candle-census) — this category has NO reachable
# --apply/delete path, ever; it only ever writes an audit report parquet + its own resumable
# checkpoint (never manifest/GCS data). Supports ALL 5 asset_groups (unlike candle-apply,
# which excludes sports) per the design brief's scope. `--manifest-fix-cutover` defaults to
# the caa995c fix-landing date (2026-07-27, see the script's own module docstring for why an
# omitted/pre-cutover no-manifest-row object must classify (F) ambiguous, never an unverified
# (E)) — override via CANDLE_ORPHAN_SWEEP_CUTOVER for a re-run once more history is verified.
# LIMIT (shared global) bounds the walk for a canary/smoke run; omit for the real full sweep.
_candle_orphan_sweep_cmd() {
    local ag="$1"       # cefi | defi | tradfi | sports | prediction
    local vm_name="$2"
    local cutover="${CANDLE_ORPHAN_SWEEP_CUTOVER:-2026-07-27}"
    local stage="gs://${CODE_BUCKET}/canonical-migration-candle-orphan-sweep/${RUN_TS}/${vm_name}/orphan_sweep_${ag}.parquet"
    local limit_flag=""
    [[ "${LIMIT:-0}" -gt 0 ]] && limit_flag=" --limit ${LIMIT}"
    printf '%s' "cd ${VM_WORKSPACE}/mdps && python -u scripts/candle_orphan_sweep.py --asset-group ${ag} --manifest-fix-cutover ${cutover}${limit_flag} --report-out ${stage}"
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
        # TradFi parquet-CONTENT instrument_id rewrite (2026-07-21/22) — the genuinely-new content-level
        # pass the 2026-07-20 path migration never did (that one only ever moved bytes, never read/rewrote
        # the parquet's own instrument_id column). DRY-BY-DEFAULT + --apply (same convention as
        # cefi/defi/prediction), handled in _launch below. --stamp/--shard-of/--shard-index/--workers are
        # passed via MIGRATION_EXTRA_ARGS (this is a simple single-invocation category, not a compound
        # chain, so the generic append at the bottom of _launch works — see that function's comment on
        # why compound-chain categories like "tradfi"/"defi-per-instrument" must suppress it instead).
        tradfi-cid) echo "python -u -m market_tick_data_service.scripts.rewrite_tradfi_content_id_2026_07_21" ;;
        # TradFi FUTURE/OPTION chain-bundle parquet-CONTENT instrument_id rewrite (2026-07-25) — the
        # per-instrument tool above deliberately EXCLUDES chain-bundle files (multi-row-per-object,
        # different shape); this is the follow-up covering that population (277,993 candidate manifest
        # rows measured 2026-07-25). Same DRY-BY-DEFAULT + --apply convention as tradfi-cid, handled in
        # _launch below. --stamp/--shard-of/--shard-index/--workers passed via MIGRATION_EXTRA_ARGS (a
        # simple single-invocation category, same reasoning as tradfi-cid's comment above).
        tradfi-cid-cb) echo "python -u -m market_tick_data_service.scripts.rewrite_tradfi_chain_bundle_content_id_2026_07_25" ;;
        # TradFi manifest USD@LIN in-place CAS re-stamp (2026-07-18/22) — a whole-index read-mutate-write
        # against the live `_index/availability_index.parquet`. Even from an IN-REGION (asia-northeast1)
        # VM the ~23s round-trip (download+transform+backup-snapshot-upload+CAS-upload) still lost the
        # CAS race against the manifest consolidator's 60s cron TWICE in a row (measured live 2026-07-22;
        # an off-region laptop caller's ~90s window loses it EVERY time, mathematically guaranteed, since
        # the window then exceeds the cycle period). Retries in a bash loop WITHIN the same VM boot (never
        # a fresh VM per attempt — that wastes ~90s of boot/tarball-fetch overhead per retry for no
        # benefit) up to 8 times with a short jittered sleep between attempts, so different attempts land
        # at different phases relative to the consolidator's tick. DRY-BY-DEFAULT + --apply (same
        # convention), handled in _launch below; `--in-place-cas` is baked in (not optional -- the
        # additive mode's dedup-key duplication caveat is exactly what this category exists to avoid).
        # MIGRATION_EXTRA_ARGS is not meaningful for this tool (no --shard-of/--workers -- a single
        # whole-index pass, not per-object) so the generic append below is harmless if set (unused).
        tradfi-manifest-cas)
            local _applyflag=""; [[ "$MODE" == "full" ]] && _applyflag=" --apply"
            printf '%s' "rc=1; for attempt in 1 2 3 4 5 6 7 8; do echo \"=== CAS attempt \${attempt}/8 START ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; python -u scripts/migrate_tradfi_manifest_usd_lin_2026_07_18.py --in-place-cas${_applyflag}; rc=\$?; echo \"=== CAS attempt \${attempt}/8 DONE rc=\${rc} ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; [ \"\${rc}\" -eq 0 ] && break; sleep \$(( (RANDOM % 20) + 5 )); done; echo \"=== CAS FINAL rc=\${rc} after up to 8 attempts ===\"; exit \${rc}"
            ;;
        # TradFi legacy per-contract manifest RETIRE (recover_tradfi_chain_manifest_registration_2026_07_22.py,
        # retire phase only -- the register phase is a separate, already-applied one-time pass with no
        # launcher wiring of its own). Drops the stale raw per-contract manifest rows left over from the
        # pre-2026-07 CME options/futures_chain flat layout now that their canonical bundled-per-underlying
        # counterparts are confirmed registered. Whole-index in-place-CAS REPLACE -- same race against the
        # manifest consolidator's 60s cron as tradfi-manifest-cas, so same 8-attempt jittered-retry loop.
        # DRY-BY-DEFAULT: dry mode runs `--retire` alone (read-only report + recovery_retire.tsv, no write);
        # full mode adds `--apply`, the only path that performs the in-place-CAS write (the tool snapshots
        # the pre-retire manifest first). `--retire` is baked in, never optional, for this category.
        tradfi-manifest-retire)
            local _applyflag=""; [[ "$MODE" == "full" ]] && _applyflag=" --apply"
            printf '%s' "rc=1; for attempt in 1 2 3 4 5 6 7 8; do echo \"=== RETIRE attempt \${attempt}/8 START ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; python -u -m market_tick_data_service.scripts.recover_tradfi_chain_manifest_registration_2026_07_22 --retire --out recovery_retire.tsv${_applyflag}; rc=\$?; echo \"=== RETIRE attempt \${attempt}/8 DONE rc=\${rc} ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ) ===\"; [ \"\${rc}\" -eq 0 ] && break; sleep \$(( (RANDOM % 20) + 5 )); done; echo \"=== RETIRE FINAL rc=\${rc} after up to 8 attempts ===\"; exit \${rc}"
            ;;
        # Sports: --workers 16 — same-region VM has lower GCS latency than the
        # cross-region laptop run that thrashed at workers=32 (2026-05-05
        # incident: 2476 generation conflicts, run died on 404 NotFound race).
        # MANIFEST_PER_VM_SHARDS=true is already exported by setup-data-pipeline-vm.sh
        # so manifest writes hit per-VM shards instead of canonical _index.
        sports)     echo "python -m market_tick_data_service.scripts.migrate_sports_canonical --start-date $START_DATE --end-date $END_DATE --workers 16" ;;
        # Sports odds_horizon_bucket FINE manifest-row venue migration (2026-07-27) -- see the
        # top-of-file usage comment for full context. A standalone scripts/ tool in market-data-
        # processing-service (not mtds), invoked as a plain script; VM_SERVICE override below
        # re-homes the dispatcher's cd target. DRY-BY-DEFAULT + --apply, handled in _launch below
        # (same convention as tradfi-cid). --start-date/--end-date are NOT accepted by this tool
        # (whole-manifest scan) so they are deliberately NOT passed here.
        sports-odds-venue-mig) echo "python -u scripts/migrate_odds_horizon_bucket_venue_to_bookmaker_2026_07_27.py --workers ${WORKERS:-32}" ;;
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
        # DeFi R3 per-instrument split, SINGLE date-range scope — one VM per QUARTER for a parallel fan-out
        # (wall-time = the slowest quarter, not the sum). Same migrate_defi_batch_to_per_instrument tool as
        # defi-per-instrument, but scoped to the positional <start-date>..<end-date> for ONE quarter with NO
        # internal year-loop and NO chained rebuild — rebuild_defi_manifest is run ONCE over the whole corpus
        # after every quarter-VM reaches terminal. Disjoint quarters ⇒ disjoint day partitions ⇒ no leaf /
        # _needs_attribution write races, safe to parallelize. Idempotent (done cells' sources are already
        # _migrated_*, invisible to the walk → fast skip). DRY-BY-DEFAULT + --apply appended by the flag block
        # below (a single-command category, exactly like defi/tradfi). Distinct VM names via VM_NAME_SUFFIX.
        defi-pi-range)
            echo "python -u -m market_tick_data_service.scripts.migrate_defi_batch_to_per_instrument --bucket market-data-tick-defi-prd-central-element-323112 --start-date $START_DATE --end-date $END_DATE --workers ${WORKERS:-16}" ;;
        # DeFi manifest rebuild ONLY (rebuild_defi_manifest) over the whole corpus — the post-migration step
        # run ONCE after every per-quarter migrate VM is terminal. Re-derives per-instrument instrument_id=stem
        # from the actual leaves. Write-by-default (falls to the else branch → dry appends --dry-run, full
        # writes). START_DATE/END_DATE positional args scope the rebuild (pass the whole corpus range).
        defi-rebuild)
            echo "python -u -m market_tick_data_service.scripts.rebuild_defi_manifest --bucket market-data-tick-defi-prd-central-element-323112 --start-date $START_DATE --end-date $END_DATE" ;;
        # DeFi Solana dex_pools fake-history relabel-forward migration (2026-07-23) --
        # scripts/relabel_solana_dex_pools_fake_history.py, per
        # defi_solana_dex_pools_fake_history_recurrence_prd_bucket_2026_07_23.md todo 3. Population is
        # EXACTLY 34 known (day, venue) combinations (17 days x {ORCA, RAYDIUM}), confirmed FINAL
        # 2026-07-23 via a targeted bounded walk -- NOT re-derived here, NOT a corpus-wide scan. The
        # tool has no --shard-of/--shard-index; it accepts a comma- OR colon-separated `--only-day`
        # list instead (`_parse_days()`) -- use COLON here, never comma: `gcloud compute instances
        # create --metadata` parses its value as a comma-delimited dict, so a comma-joined day list
        # breaks gcloud's OWN arg parsing ("Bad syntax for dict arg", adversarial finding 2026-07-23).
        # MIGRATION_EXTRA_ARGS="--only-day 2025-01-01:2025-01-02:..." lets the
        # operator assign each VM a disjoint day sub-range. Every shard processes BOTH venues for its
        # assigned days (no separate venue split) -- RAYDIUM's ~99/day is negligible next to ORCA's
        # ~14094/day. Omitting MIGRATION_EXTRA_ARGS processes the WHOLE 34-combo population in one VM
        # (~241,281 objects, multi-hour at the tool's own sequential per-object GCS latency --
        # day-sub-range sharding across several VMs is strongly preferred over one VM walking serially).
        # DRY-BY-DEFAULT + --apply for full (same convention as defi/cefi/tradfi-cid below).
        defi-relabel) echo "python -u scripts/relabel_solana_dex_pools_fake_history.py" ;;
        # Sports derived_features post-floor residue census+purge (2026-07-27, Track F
        # follow-up, sports_consolidated_native_ao_extract_2026_07_25.md). Runs from
        # features-service (VM_SERVICE=features_service below re-homes the dispatcher's
        # default `cd $WORKSPACE/mtds` into `$WORKSPACE/features` -- same re-homing trick
        # as tradfi-catalogue-canon/candle-census). A standalone scripts/ tool, not an
        # installed package module, so invoked as a PLAIN SCRIPT. $MODE is baked in here
        # (compound chain), NOT via the generic --apply/--dry-run append below -- dry mode
        # runs the census once (no mutation); full mode applies the delete then, only on a
        # clean exit, immediately chains --recensus to verify 0 residue objects remain
        # (steps 4+5 of the todo's own spec, one VM boot instead of two). No --shard-of --
        # the tool parallelizes internally via --workers (per-day GCS listing threads), not
        # via multiple VMs.
        sports-features-purge)
            if [[ "$MODE" == "full" ]]; then
                printf '%s' "cd ${VM_WORKSPACE}/features && python -u scripts/purge_sports_derived_features_post_floor_residue_2026_07_27.py --workers ${WORKERS:-32} --apply; rc=\$?; echo \"=== PURGE DONE rc=\${rc} ===\"; if [ \"\${rc}\" -eq 0 ]; then echo \"=== RE-CENSUS START ===\"; python -u scripts/purge_sports_derived_features_post_floor_residue_2026_07_27.py --workers ${WORKERS:-32} --recensus; rc=\$?; echo \"=== RE-CENSUS DONE rc=\${rc} ===\"; fi; exit \${rc}"
            else
                printf '%s' "cd ${VM_WORKSPACE}/features && python -u scripts/purge_sports_derived_features_post_floor_residue_2026_07_27.py --workers ${WORKERS:-32}"
            fi
            ;;
        # Sports K1/K2 casing-REVERT data migration (2026-07-27, Track C step 3 —
        # issues/sports_k1k2_delete_bundled_with_twin_less_data_2026_07_27.md todo 1). Runs from
        # $VM_WORKSPACE/mtds (market-tick-data-service — the default _svc, no override needed).
        # COPY-ONLY (never deletes the uppercase source — the separate, later, [OPERATOR]-gated
        # delete pass is Track V). $MODE IS honored for real: dry -> a single migrate dry-run scan
        # (prefix-scoped listing per day, no writes, sizing only); full -> the 3-step compound
        # chain in ONE VM boot, gated at each step on the prior step's clean exit: (1) migrate
        # --apply-prod --confirm-prod-write (copy-if-missing / content-verify-if-present, never
        # re-copies a byte/key-identical twin), (2) generate_casing_revert_manifest_report (a
        # READ-ONLY re-verify pass over the now-canonical lowercase objects — writes its report to
        # a LOCAL --reports-dir, no GCS round-trip needed since step 3 runs on the SAME VM boot),
        # (3) manifest_swap --apply-prod --confirm-prod-write (CAS-protected snapshot -> REMOVE
        # stale uppercase rows -> ADD lowercase canonical rows -> verify). Same suppression class
        # as sports-features-purge/tradfi-manifest-cas (compound `...; exit ${rc}` chain) — see the
        # MIGRATION_EXTRA_ARGS suppression list in _launch below.
        sports-k1k2-casing-revert)
            local _k1k2_dir="scripts/sports/k1k2_casing_revert_2026_07_27"
            if [[ "$MODE" == "full" ]]; then
                printf '%s' "cd ${VM_WORKSPACE}/mtds && mkdir -p k1k2-revert-work && python -u ${_k1k2_dir}/migrate_sports_casing_revert_2026_07_27.py --start-date ${START_DATE} --end-date ${END_DATE} --apply-prod --confirm-prod-write --workers ${WORKERS:-16}; rc=\$?; echo \"=== MIGRATE DONE rc=\${rc} ===\"; if [ \"\${rc}\" -eq 0 ]; then echo \"=== REPORT GEN START ===\"; python -u ${_k1k2_dir}/generate_casing_revert_manifest_report_2026_07_27.py --start-date ${START_DATE} --end-date ${END_DATE} --out k1k2-revert-work/shard_0_of_1.json --workers ${WORKERS:-16}; rc=\$?; echo \"=== REPORT GEN DONE rc=\${rc} ===\"; if [ \"\${rc}\" -eq 0 ]; then echo \"=== MANIFEST SWAP START ===\"; python -u ${_k1k2_dir}/manifest_swap_casing_revert_2026_07_27.py --reports-dir k1k2-revert-work --num-shards 1 --apply-prod --confirm-prod-write; rc=\$?; echo \"=== MANIFEST SWAP DONE rc=\${rc} ===\"; fi; fi; exit \${rc}"
            else
                printf '%s' "cd ${VM_WORKSPACE}/mtds && python -u ${_k1k2_dir}/migrate_sports_casing_revert_2026_07_27.py --start-date ${START_DATE} --end-date ${END_DATE} --workers ${WORKERS:-16}"
            fi
            ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    # VM_NAME_SUFFIX lets several shard VMs of the same category+second coexist without name collision
    # (e.g. one VM per date-shard / per --buckets). Prefix stays canonical-migration-<cat>- for the watchdog.
    # BUG FOUND 2026-07-22 (candle-apply adversarial self-test, SHARD_OF=3 preview): "<ag>-candle-apply"
    # combined with a shard suffix overflows GCE's 63-char instance-name limit for the longer asset_group
    # names (measured worst case, 2-digit shard count: prediction-candle-apply -> 71 chars; GCE rejected
    # the create call with "Invalid value for field 'resource.name'"). Abbreviate ONLY the vm_name token
    # (never $cat itself, which everything else -- dispatch, tarball checks, _ag derivation -- keys off)
    # for *-candle-apply so the name stays comfortably under budget (measured worst case with this fix,
    # 2-digit shard: prediction-cdlap -> 60 chars) while the vm_name still starts with the registered
    # "canonical-migration-<ag>-" prefix (longest-prefix-startswith match in VM_PREFIX_TO_BUCKET).
    local _vm_name_cat="$cat"
    local _shard_suffix="${VM_NAME_SUFFIX:-}"
    # "defi-curve-optimism-reclassify" (31 chars) + the 21-char "canonical-migration-" prefix +
    # 15-char timestamp overflows the 63-char budget on its own (66 chars, no shard suffix even
    # needed) — abbreviate the vm_name token only, same reasoning as *-candle-apply above ($cat
    # itself stays unabbreviated everywhere else: dispatch match, _svc/_ag derivation, tarball checks).
    if [[ "$cat" == "defi-curve-optimism-reclassify" ]]; then
        _vm_name_cat="defi-curve-optm-reclass"
    elif [[ "$cat" == *-candle-apply ]]; then
        _vm_name_cat="${cat%-candle-apply}-cdlap"
        # Shorten "shard{i}of{N}" -> "s{i}of{N}" too (saves 4 more chars) — same info, tighter budget.
        # Operates on the already nounset-safe local (not $VM_NAME_SUFFIX directly, which crashes
        # under `set -u` when unset for a non-sharded single-VM candle-apply launch).
        _shard_suffix="${_shard_suffix/#shard/s}"
    elif [[ "$cat" == *-candle-orphan-sweep ]]; then
        # BUG FOUND 2026-07-27 (first real launch): "canonical-migration-prediction-candle-orphan-
        # sweep-<ts>" is 66 chars, over GCE's 63-char limit -- same overflow class as *-candle-apply
        # above. Abbreviate the vm_name token only ($cat itself stays unabbreviated everywhere else).
        _vm_name_cat="${cat%-candle-orphan-sweep}-cdlorph"
    fi
    local vm_name="canonical-migration-${_vm_name_cat}-${RUN_TS}${_shard_suffix:+-${_shard_suffix}}"
    # --vm-name/VM_NAME_OVERRIDE wins (single-category relaunch only -- see the top-of-file comment).
    if [[ -n "$VM_NAME_OVERRIDE" ]]; then
        vm_name="$VM_NAME_OVERRIDE"
    fi
    # Defense in depth: GCE instance names must be <=63 chars. Fail LOUDLY here (before any GCS/gcloud
    # side effect) rather than let `gcloud compute instances create` reject it deep inside the launch
    # sequence with a cryptic regex error, which is what originally surfaced this bug.
    if [[ ${#vm_name} -gt 63 ]]; then
        echo "ERROR: computed vm_name '${vm_name}' is ${#vm_name} chars, exceeds GCE's 63-char limit." >&2
        return 1
    fi
    local cmd
    if [[ "$cat" == "tradfi" ]]; then
        # MIGRATION_EXTRA_ARGS is NOT appendable here: this category emits a compound `&&` chain, so a
        # trailing append would land on the final `gcloud storage cp` rather than the python pass — the
        # flags would be silently DISCARDED. That silent drop previously turned an intended gated
        # massive-purge into "purge nothing + migrate the whole estate" (caught pre-flight 2026-07-20).
        # A flag that vanishes is worse than one that errors, so FAIL LOUDLY instead of dropping it.
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for category 'tradfi' — it emits a" >&2
            echo "       multi-pass compound command and the flags would be silently discarded." >&2
            echo "       For the gated massive purge use: TRADFI_PURGE_MASSIVE_ONLY=1" >&2
            return 1
        fi
        if [[ -n "${TRADFI_PURGE_MASSIVE_ONLY:-}" ]]; then
            # GATED massive-only purge over a batch_massive-filtered enumeration (migrate pass only).
            cmd="$(_tradfi_massive_purge_cmd "$vm_name")"
        else
            # tradfi = the 2026-07 content migration: multi-pass compound command (mode flags + --quarantine
            # embedded per-pass inside the builder, so it is NOT subject to the generic single --apply append).
            cmd="$(_tradfi_content_migration_cmd "$vm_name")"
        fi
    elif [[ "$cat" == "tradfi-catalogue-canon" ]]; then
        # Self-contained `cd ... && python ...` chain; --apply is embedded per-MODE by the builder,
        # so the generic --apply/--dry-run append below is deliberately bypassed (same reason as
        # the tradfi content-migration branch).
        cmd="$(_catalogue_canon_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "tradfi-catalogue-promote" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --dry-run is embedded per-MODE by the
        # builder (the tool's OWN monotonic guard is the write-safety, not this launcher).
        cmd="$(_catalogue_promote_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "cefi-dedup-apply" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --apply is embedded per-MODE by the
        # builder (the tool's own STOP-ON-SURPRISE gates are the write-safety, not this launcher).
        # DRAIN GATE is the caller's responsibility — see _cefi_dedup_apply_cmd's comment.
        cmd="$(_cefi_dedup_apply_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "defi-curve-optimism-reclassify" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --apply/--dry-run is embedded
        # per-MODE by the builder (the tool's own backup-then-write + idempotent design is the write
        # safety, not this launcher). See _curve_optimism_reclassify_cmd's comment.
        cmd="$(_curve_optimism_reclassify_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "cefi-late-renames" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --apply/--stamp are embedded
        # per-MODE by the builder (the tool's own STOP-ON-SURPRISE gates are the write-safety, not
        # this launcher). --venue (to scope to one colliding venue) is added via MIGRATION_EXTRA_ARGS.
        cmd="$(_cefi_late_renames_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "cefi-content-apply" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --apply is embedded per-MODE by the
        # builder (the tool's own Phase -1 catalogue gate + honest-unresolved counting are the
        # write-safety, not this launcher). DRAIN GATE is the caller's responsibility.
        cmd="$(_cefi_content_apply_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "cefi-eu-twin-apply" ]]; then
        # Self-contained `cd ... && python ...` single invocation; --apply is embedded per-MODE by the
        # builder (the tool's own snapshot-first + STOP-ON-SURPRISE bound are the write-safety, not
        # this launcher). DRAIN GATE is the caller's responsibility.
        cmd="$(_cefi_eu_twin_apply_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "tradfi-cme-monolith" ]]; then
        # Self-contained single invocation; --apply/--stamp/--day are embedded per-MODE/CME_DAY by
        # the builder (same reason as cefi-late-renames -- a compound value, not a blind append).
        cmd="$(_cme_monolith_migrate_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "tradfi-cme-monolith-delete" ]]; then
        cmd="$(_cme_monolith_delete_source_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "defi-glued-reshard" ]]; then
        # Self-contained `cd ... && gcloud storage cp ... && python ...` chain; --apply is embedded
        # per-MODE by the builder (same reason as tradfi-catalogue-promote -- a compound && chain
        # can't take a blind flag append).
        cmd="$(_glued_reshard_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == *-candle-census ]]; then
        # Candle census (2026-07-22): self-contained `cd ... && gcloud ... && python --dry-run ...`
        # chain; $MODE is deliberately IGNORED (see _candle_census_cmd comment -- always --dry-run,
        # no reachable --apply path) -- the generic --apply/--dry-run append below is bypassed, same
        # reason as the other compound-chain branches (tradfi / tradfi-catalogue-canon).
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for candle-census categories -- they" >&2
            echo "       emit a compound && chain and the flags would be silently discarded." >&2
            return 1
        fi
        cmd="$(_candle_census_cmd "${cat%-candle-census}" "$vm_name")"
    elif [[ "$cat" == *-candle-apply ]]; then
        # Candle apply (2026-07-22, P7): self-contained `cd ... && gcloud ... && python <mode> ...`
        # chain; $MODE IS honored here (unlike candle-census) -- the generic --apply/--dry-run append
        # below is still bypassed since mode/gates are embedded by the builder (same reason as the
        # tradfi/candle-census branches).
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for candle-apply categories -- they" >&2
            echo "       emit a compound && chain and the flags would be silently discarded." >&2
            return 1
        fi
        cmd="$(_candle_apply_cmd "${cat%-candle-apply}" "$vm_name")"
    elif [[ "$cat" == *-candle-orphan-sweep ]]; then
        # Candle orphan sweep (2026-07-27, todo 1): self-contained `cd ... && python ...`
        # invocation -- the SCRIPT does its own internal single walk + bucket resolution, so
        # (unlike candle-census/apply) there is no `gcloud storage ls` enumeration step here.
        # $MODE is DELIBERATELY IGNORED (mirrors *-candle-census) -- READ-ONLY, no reachable
        # --apply/delete path ever; only writes an audit report parquet + its own checkpoint.
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for candle-orphan-sweep categories" >&2
            echo "       -- use CANDLE_ORPHAN_SWEEP_CUTOVER / LIMIT env overrides instead." >&2
            return 1
        fi
        cmd="$(_candle_orphan_sweep_cmd "${cat%-candle-orphan-sweep}" "$vm_name")"
    elif [[ "$cat" == "defi-marker-cleanup" ]]; then
        # DRY-RUN ONLY, HARD -- $MODE is deliberately IGNORED and forced to "dry" (mirrors
        # *-candle-census): this category has NO reachable --apply code path through this launcher,
        # ever, regardless of what an operator passes. See _delete_migrated_defi_markers_cmd's comment.
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for defi-marker-cleanup -- it emits a" >&2
            echo "       fixed dry-run-only compound && chain and the flags would be silently discarded." >&2
            return 1
        fi
        MODE="dry"
        cmd="$(_delete_migrated_defi_markers_cmd)"
    elif [[ "$cat" == "defi-gmx-purge" ]]; then
        # $MODE IS honored (unlike defi-marker-cleanup) -- the SCRIPT ITSELF hard-gates --apply on
        # the consolidator cron being paused, so this launcher needs no extra gate of its own.
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for defi-gmx-purge -- the mode flag" >&2
            echo "       is embedded by the builder and a trailing append could land in the wrong place." >&2
            return 1
        fi
        cmd="$(_gmx_purge_cmd)"
    elif [[ "$cat" == "defi-lst-rates-fold" ]]; then
        # $MODE IS honored for real -- --apply is embedded by the builder. Fold-only (never a delete),
        # so no consolidator-drain gate is required (unlike defi-gmx-purge/cefi-bybit-spot-purge).
        cmd="$(_lst_rates_fold_cmd)"
        [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]] && cmd="$cmd ${MIGRATION_EXTRA_ARGS}"
    elif [[ "$cat" == "cefi-bybit-spot-purge" ]]; then
        # $MODE IS honored, THREE-valued (dry|smoke|full) -- see _bybit_spot_purge_cmd's comment.
        # The SCRIPT ITSELF hard-gates both --smoke and --apply on the consolidator cron being
        # paused, so this launcher needs no extra gate of its own (mirrors defi-gmx-purge).
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for cefi-bybit-spot-purge -- the mode" >&2
            echo "       flag is embedded by the builder and a trailing append could land in the wrong place." >&2
            return 1
        fi
        cmd="$(_bybit_spot_purge_cmd)"
    elif [[ "$cat" == "manifest-restamp" ]]; then
        # $MODE is ignored -- see _manifest_restamp_cmd's comment. RESTAMP_BUCKET is required.
        if [[ -n "${MIGRATION_EXTRA_ARGS:-}" ]]; then
            echo "ERROR: MIGRATION_EXTRA_ARGS is not supported for manifest-restamp." >&2
            return 1
        fi
        cmd="$(_manifest_restamp_cmd)" || return 1
    else
        cmd="$(_script_for "$cat")"
        [[ -z "$cmd" ]] && { echo "Unknown category: $cat"; return 1; }
        # Flag convention differs by tool: the v9 tools (migrate_defi_full_v9_canonical +
        # migrate_prediction_to_pred_prd_v9) are DRY-BY-DEFAULT and take --apply to write; the others
        # are write-by-default + --dry-run.
        if [[ "$cat" == "defi-per-instrument" || "$cat" == "tradfi-manifest-cas" || "$cat" == "tradfi-manifest-retire" || "$cat" == "sports-features-purge" || "$cat" == "sports-k1k2-casing-revert" ]]; then
            : # apply/dry is baked into the per-attempt retry loop by _script_for ($MODE) for
              # tradfi-manifest-cas/tradfi-manifest-retire (mirrors defi-per-instrument's per-year loop) -- a
              # --apply/--dry-run/EXTRA_ARGS append to a compound `for … done; exit …` string is a syntax
              # error, so BOTH the flag-append and MIGRATION_EXTRA_ARGS below are deliberately suppressed.
              # sports-features-purge's own if/full else/dry branches are the SAME class of compound
              # statement (ends in `exit ${rc}` on the full branch) -- same suppression reasoning.
              # sports-k1k2-casing-revert's full-mode 3-step chain is the SAME class again (ends in
              # `exit ${rc}`) -- same suppression reasoning.
        elif [[ "$cat" == "defi" || "$cat" == "defi-pi-range" || "$cat" == "defi-relabel" || "$cat" == "prediction" || "$cat" == "cefi" || "$cat" == "tradfi-cme-options" || "$cat" == "tradfi-cid" || "$cat" == "tradfi-cid-cb" || "$cat" == "sports-odds-venue-mig" ]]; then
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
    [[ "$cat" == "tradfi-catalogue-canon" || "$cat" == "tradfi-catalogue-promote" || "$cat" == "cefi-dedup-apply" || "$cat" == "cefi-eu-twin-apply" || "$cat" == "defi-curve-optimism-reclassify" ]] && _svc="instruments_service"
    # candle-census / candle-apply both run migrate_candle_canonical_2026_07.py which lives in
    # market-data-processing-service, not MTDS -- so they need that tarball staged instead (see
    # _candle_census_cmd / _candle_apply_cmd comments).
    [[ "$cat" == *-candle-census || "$cat" == *-candle-apply || "$cat" == *-candle-orphan-sweep ]] && _svc="market_data_processing_service"
    # sports-odds-venue-mig's script also lives in market-data-processing-service, not MTDS.
    [[ "$cat" == "sports-odds-venue-mig" ]] && _svc="market_data_processing_service"
    # sports-features-purge's script lives in features-service, not MTDS.
    [[ "$cat" == "sports-features-purge" ]] && _svc="features_service"
    md="${md},VM_SERVICE=${_svc}"
    md="${md},VM_OPERATION=migrate-${cat}"
    # defi-per-instrument shares the DeFi tick bucket + fleet classification — keep the asset group DEFI
    # (not the novel DEFI-PER-INSTRUMENT) so dashboards/heartbeat classify it with the rest of DeFi.
    local _ag; _ag="$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    [[ "$cat" == "defi-per-instrument" || "$cat" == "defi-pi-range" || "$cat" == "defi-rebuild" || "$cat" == "defi-glued-reshard" || "$cat" == "defi-marker-cleanup" || "$cat" == "defi-gmx-purge" || "$cat" == "defi-lst-rates-fold" || "$cat" == "defi-curve-optimism-reclassify" ]] && _ag="DEFI"
    # Keep the fleet asset-group TRADFI (not the novel TRADFI-CATALOGUE-CANON) so heartbeat/
    # dashboards classify this VM with the rest of tradfi.
    [[ "$cat" == "tradfi-catalogue-canon" || "$cat" == "tradfi-catalogue-promote" ]] && _ag="TRADFI"
    # Keep the fleet asset-group CEFI (not the novel CEFI-DEDUP-APPLY/CEFI-LATE-RENAMES/
    # CEFI-BYBIT-SPOT-PURGE/CEFI-CONTENT-APPLY/CEFI-EU-TWIN-APPLY) so dashboards/heartbeat classify
    # these VMs with the rest of cefi.
    [[ "$cat" == "cefi-dedup-apply" || "$cat" == "cefi-late-renames" || "$cat" == "cefi-bybit-spot-purge" || "$cat" == "cefi-content-apply" || "$cat" == "cefi-eu-twin-apply" ]] && _ag="CEFI"
    # candle-census / candle-apply category names are "<ag>-candle-census"/"<ag>-candle-apply" --
    # strip the suffix to recover the real asset group so dashboards/heartbeat classify these VMs
    # with the rest of that asset group instead of a novel "<AG>-CANDLE-CENSUS"/"-APPLY" bucket.
    [[ "$cat" == *-candle-census ]] && _ag="$(echo "${cat%-candle-census}" | tr '[:lower:]' '[:upper:]')"
    [[ "$cat" == *-candle-apply ]] && _ag="$(echo "${cat%-candle-apply}" | tr '[:lower:]' '[:upper:]')"
    [[ "$cat" == *-candle-orphan-sweep ]] && _ag="$(echo "${cat%-candle-orphan-sweep}" | tr '[:lower:]' '[:upper:]')"
    # Keep the fleet asset-group SPORTS (not the novel SPORTS-FEATURES-PURGE /
    # SPORTS-K1K2-CASING-REVERT / SPORTS-ODDS-VENUE-MIG) so dashboards/heartbeat classify these
    # VMs with the rest of sports.
    [[ "$cat" == "sports-features-purge" || "$cat" == "sports-k1k2-casing-revert" || "$cat" == "sports-odds-venue-mig" ]] && _ag="SPORTS"
    md="${md},VM_ASSET_GROUP=${_ag}"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_MIGRATION_CMD=${cmd}"
    # STALL_PROGRESS_REGEX (todo 3+4, migration_vm_hung_detection_monitoring_gap_2026_07_27.md,
    # Gap 2) -- setup-data-pipeline-vm.sh already routes every VM_TASK=canonical-migration worker
    # through the shared _launch_with_tee() -> vm-exec-with-gcs-tee.sh stall-kill (confirmed live
    # this session: no code change needed there, STALL_TIMEOUT_SEC/STALL_PROGRESS_REGEX are read off
    # GCE metadata generically, lines 460/468, independent of VM_TASK), but no launcher category ever
    # set STALL_PROGRESS_REGEX, so every category fell back to raw log-BYTE-GROWTH stall detection --
    # permanently defeated by the always-on PIPELINE_HEARTBEAT emitter wired into the SAME tee'd log
    # (it writes a line every 60s regardless of whether the real workload is alive), which is exactly
    # how 10/42 cefi-content-apply VMs sat hung 1-2.5h+ with GCE reporting RUNNING and nothing paging.
    # Only cefi-content-apply's script has had its real progress-log line format verified against a
    # live run.log this session (migrate_cefi_content_instrument_id_catalogue_2026_07_17.py); the
    # other ~20 VM_TASK=canonical-migration categories' scripts have NOT been individually checked
    # (that per-category audit is todo 5's stated scope, deliberately left open there) so they
    # intentionally do NOT get a regex here yet and keep the size-only 1800s default.
    #   "Discovery progress: day=%s cumulative_files=%d elapsed=%.1fs" -- once per scanned day
    #     (discovery phase).
    #   "Progress: %d/%d files (%.1f files/sec, %.1fs elapsed) stats=%s" -- every 200 completed
    #     files, and once more as the final tally (migrate phase).
    #   "Elapsed (migrate phase): %.1fs (%.2f files/sec)" -- final summary line.
    #   "No progress in the last poll window -- %d files still outstanding (possible wedged worker)"
    #     -- this is the tool's OWN wedged-worker WARNING, not a progress marker; it must never reset
    #     the stall timer, and it does not (case-sensitive grep -E, verified against the exact string
    #     above: "progress:" (lowercase, colon) does not match "progress in", and it has no
    #     "files/sec" substring either).
    # Regex: "progress:|files/sec" -- case-sensitive grep -E, no spaces/commas (gcloud's --metadata is
    # comma-delimited so a literal comma in the value would silently split into a bogus second key).
    # Campaign-measured healthy throughput (2.9-9.9 files/sec/VM, ~5.5 avg) gives a "Progress:" line
    # roughly every 20-70s -- >>25x headroom under the unchanged 1800s STALL_TIMEOUT_SEC default, so
    # this is a low false-positive-risk addition for legitimately-running VMs of this specific category.
    [[ "$cat" == "cefi-content-apply" ]] && md="${md},STALL_PROGRESS_REGEX=progress:|files/sec"
    # TODO(low-severity, adversarial review 2026-07-22): for *-candle-census categories, $MODE is
    # silently ignored by _candle_census_cmd() (always emits --dry-run, no reachable --apply path),
    # but it is still echoed verbatim into VM_MIGRATION_MODE metadata and the "mode=" GCE label
    # below. An operator passing "full" gets a VM labeled mode=full that only ever dry-runs -- not a
    # mutation risk, but misleading for fleet dashboards filtering on mode=full to find write-mode
    # runs. Fix: hardcode VM_MIGRATION_MODE/label to "dry" for *-candle-census regardless of $MODE.
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
    for _pin_key in UAC_TARBALL_SHA UTL_TARBALL_SHA MTDS_TARBALL_SHA MDPS_TARBALL_SHA; do
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

    # SPOT-preemption relaunch support (adversarial review 2026-07-22, HIGH finding): persist THIS
    # VM's exact category/window/shard plus its OWN vm_name, mirroring launch-mdps-backfill-vm.sh's
    # already-working mechanism, so exit_code_fleet_monitor's PREEMPTED auto_recover actuator
    # (RelaunchPreemptedVm) reproduces the SAME shard under the SAME name (the checkpoint blob path
    # migrate_candle_canonical_2026_07.py writes to is keyed on VM_NAME) instead of hitting the
    # usage-error `exit 2` / a colliding-name/fanned-out relaunch. `cat` is THIS specific concrete
    # category (never the outer `all`/tradfi-fan-out `$ASSET_GROUP`), and SHARD_OF/SHARD_INDEX are
    # THIS VM's own (per-shard, inside the fan-out loops) values -- so relaunching a single shard VM
    # can never re-trigger a multi-VM fan-out (see the SHARD_INDEX_EXPLICIT comment above). Best-effort
    # (never fails the launch).
    lc_write_launch_params "$vm_name" "$PROJECT" "launch-canonical-migration-vm.sh" \
        "VM_NAME_OVERRIDE=${vm_name}" \
        "RESUME_ASSET_GROUP=${cat}" \
        "RESUME_START_DATE=${START_DATE}" \
        "RESUME_END_DATE=${END_DATE}" \
        "RESUME_MODE=${MODE}" \
        "RESUME_SHARD_OF=${SHARD_OF}" \
        "RESUME_SHARD_INDEX=${SHARD_INDEX}" \
        "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

    # SPOT preemption contract (2026-07-23, operator-requested — this launcher family had
    # LAUNCH_PARAMS.json (above) but NOT the preemption-signal blob, so exit_code_fleet_monitor's
    # PREEMPTED auto_recover actuator (RelaunchPreemptedVm) could never distinguish a benign SPOT
    # reclaim from a genuine silent-zero failure for any canonical-migration-* VM — measured live on
    # the TRADFI candle-apply run: 18/20 SPOT shards preempted within minutes, all silently, no
    # auto-recovery fired). Mirrors launch-defi-backfill-vm.sh's already-working pattern.
    lc_write_preemption_signal_file

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        # Verify the tarballs this category actually stages (VM_SERVICE-driven), not a fixed list:
        # catalogue-canon runs instruments-service code and never touches MTDS.
        local _fresh_repos=(market-tick-data-service unified-api-contracts unified-trading-library deployment-service)
        [[ "$cat" == "tradfi-catalogue-canon" || "$cat" == "tradfi-catalogue-promote" || "$cat" == "cefi-dedup-apply" || "$cat" == "cefi-eu-twin-apply" || "$cat" == "defi-curve-optimism-reclassify" ]] && _fresh_repos=(instruments-service unified-api-contracts unified-trading-library deployment-service)
        [[ "$cat" == *-candle-census || "$cat" == *-candle-apply || "$cat" == *-candle-orphan-sweep ]] && _fresh_repos=(market-data-processing-service unified-api-contracts unified-trading-library deployment-service)
        lc_verify_tarball_freshness "$CODE_BUCKET" "${_fresh_repos[@]}" \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    # BUG FOUND 2026-07-22 (candle-apply adversarial self-test — SHARD_OF=3 preview): DRY_RUN=true
    # previously gated ONLY lc_verify_tarball_freshness above; this actual `gcloud compute instances
    # create` call ran UNCONDITIONALLY regardless of DRY_RUN for every category, always. A
    # DRY_RUN=true "preview" therefore silently created a REAL VM every time it was used (proven live:
    # a defi-candle-apply dry-mode preview created + ran `canonical-migration-defi-candle-apply-
    # 20260722-162220` for real — harmless only because that specific preview also happened to pass
    # `dry` as the migration MODE; a `full`-mode preview would have created a REAL --apply VM). Fixed:
    # DRY_RUN=true now genuinely skips VM creation, printing what would have been created instead.
    if [[ "${DRY_RUN:-false}" == "true" ]]; then
        echo "  DRY_RUN=true — NOT creating $vm_name (preview only)."
        return 0
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
        --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}" \
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
    tradfi-cid)
        # Per-object worklist (859,121 rows measured 2026-07-21) -- shard fan-out mirrors the "tradfi"
        # category above. SHARD_OF>1 with SHARD_INDEX unset fans one VM per shard; a pinned SHARD_INDEX
        # (or SHARD_OF=1) launches exactly one VM (canary / targeted relaunch).
        if [[ "$SHARD_OF" -gt 1 && -z "$SHARD_INDEX_EXPLICIT" ]]; then
            for ((_i = 0; _i < SHARD_OF; _i++)); do
                SHARD_INDEX="$_i"
                VM_NAME_SUFFIX="shard${_i}of${SHARD_OF}"
                MIGRATION_EXTRA_ARGS="--stamp ${RUN_TS} --shard-of ${SHARD_OF} --shard-index ${_i} --workers ${WORKERS:-32}" _launch tradfi-cid
            done
        else
            MIGRATION_EXTRA_ARGS="--stamp ${RUN_TS}${SHARD_INDEX_EXPLICIT:+ --shard-of ${SHARD_OF} --shard-index ${SHARD_INDEX}} --workers ${WORKERS:-32}" _launch tradfi-cid
        fi
        ;;
    tradfi-cid-cb)
        # Chain-bundle worklist (277,993 candidate manifest rows measured 2026-07-25) -- shard fan-out
        # mirrors "tradfi-cid" above exactly (same tool family, same --stamp/--shard-of/--shard-index/
        # --workers CLI surface). SHARD_OF>1 with SHARD_INDEX unset fans one VM per shard; a pinned
        # SHARD_INDEX (or SHARD_OF=1) launches exactly one VM (canary / targeted relaunch).
        if [[ "$SHARD_OF" -gt 1 && -z "$SHARD_INDEX_EXPLICIT" ]]; then
            for ((_i = 0; _i < SHARD_OF; _i++)); do
                SHARD_INDEX="$_i"
                VM_NAME_SUFFIX="shard${_i}of${SHARD_OF}"
                MIGRATION_EXTRA_ARGS="--stamp ${RUN_TS} --shard-of ${SHARD_OF} --shard-index ${_i} --workers ${WORKERS:-32}" _launch tradfi-cid-cb
            done
        else
            MIGRATION_EXTRA_ARGS="--stamp ${RUN_TS}${SHARD_INDEX_EXPLICIT:+ --shard-of ${SHARD_OF} --shard-index ${SHARD_INDEX}} --workers ${WORKERS:-32}" _launch tradfi-cid-cb
        fi
        ;;
    cefi-candle-apply|defi-candle-apply|tradfi-candle-apply|prediction-candle-apply)
        # candle-apply (2026-07-22, P7): shard fan-out mirrors "tradfi"/"tradfi-cid" above --
        # SHARD_OF>1 with SHARD_INDEX unset fans one VM per shard; a pinned SHARD_INDEX (or
        # SHARD_OF=1) launches exactly one VM (canary / targeted relaunch). Shard args are baked
        # directly into _candle_apply_cmd() from the global SHARD_OF/SHARD_INDEX (not routed through
        # MIGRATION_EXTRA_ARGS -- see that function's comment).
        if [[ "$SHARD_OF" -gt 1 && -z "$SHARD_INDEX_EXPLICIT" ]]; then
            for ((_i = 0; _i < SHARD_OF; _i++)); do
                SHARD_INDEX="$_i"
                VM_NAME_SUFFIX="shard${_i}of${SHARD_OF}"
                _launch "$ASSET_GROUP"
            done
        else
            _launch "$ASSET_GROUP"
        fi
        ;;
    cefi|defi|defi-per-instrument|defi-pi-range|defi-relabel|defi-rebuild|defi-glued-reshard|defi-marker-cleanup|defi-gmx-purge|defi-lst-rates-fold|defi-curve-optimism-reclassify|prediction|sports|tradfi-cme-options|tradfi-catalogue-canon|tradfi-catalogue-promote|tradfi-manifest-cas|tradfi-manifest-retire|tradfi-cme-monolith|tradfi-cme-monolith-delete|cefi-candle-census|defi-candle-census|tradfi-candle-census|prediction-candle-census|cefi-candle-orphan-sweep|defi-candle-orphan-sweep|tradfi-candle-orphan-sweep|sports-candle-orphan-sweep|prediction-candle-orphan-sweep|cefi-dedup-apply|cefi-late-renames|cefi-content-apply|cefi-eu-twin-apply|cefi-bybit-spot-purge|manifest-restamp|sports-features-purge|sports-k1k2-casing-revert) _launch "$ASSET_GROUP" ;;
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
