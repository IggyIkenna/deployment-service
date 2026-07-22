#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Phase 5b.3 MDPS backfill VM launcher — re-aggregate tick data into canonical
# processed_candles/ for each category using the strict-mode writer landed in
# MDPS commit c1cb73c (canonical_writer.py wrapping StreamingParquetWriter +
# SchemaContract + ManifestWriter).
#
# Input buckets:    gs://market-data-tick-{category}-*/raw_tick_data/by_date/
# Output subtree:   gs://market-data-tick-{category}-*/processed_candles/by_date/
#                       day=/timeframe=/data_type=/instrument_type=/venue=/
# Manifest:         gs://market-data-tick-{category}-*/_index/availability_index.parquet
#
# Scope per category (per plan Phase 5b.3):
#   CeFi:       2020-01-01 → 2026-04-18
#   TradFi:     2020-01-01 → 2026-04-18 (1m pass-through + re-aggregated higher)
#   DeFi:       2020-01-01 → 2026-04-18
#   Sports:     2020-06-06 → 2026-04-18 (2020-06 DATA FLOOR — codex/02-data/sports-2020-06-data-floor.md)
#   Prediction: 2025-03-14 → 2026-04-18 (Polymarket only)
#
# Operational safety:
#   * 50 GB boot disk (default 10 GB causes OOM on long ranges per Agent 4 note).
#   * DEPLOYMENT_STARTED/PROGRESS/COMPLETED events via deployment_heartbeat.py.
#   * stdout/stderr tee'd to GCS via vm-exec-with-gcs-tee.sh.
#   * asia-northeast1-c to stay in-region with the source tick buckets.
#   * Dry-run supported so the launcher can be tested without mutating GCS.
#
# Usage:
#   bash launch-mdps-backfill-vm.sh cefi       2020-01-01 2026-04-18 dry
#   bash launch-mdps-backfill-vm.sh tradfi     2020-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh defi       2020-01-01 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh sports     2020-06-06 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh prediction 2025-03-14 2026-04-18 full
#   bash launch-mdps-backfill-vm.sh all        2020-01-01 2026-04-18 full
#
# Force-reprocess (densify reprocess — re-write already-captured candle cells):
#   --force                                            # or FORCE=true env. Threads --force into the
#                                                      # MDPS `process` CLI (_write_candles(force=True)),
#                                                      # so existing parquets are re-aggregated with the
#                                                      # dense / no-leading-NaN finalizer instead of being
#                                                      # SKIPed as fresh-in-manifest. Use for the leading-NaN
#                                                      # historical remediation (mdps_state_adapter_leading_nan_audit_2026_05_29.md).
#                                                      # Example (densify cefi state adapters over the read window):
#                                                      #   bash launch-mdps-backfill-vm.sh --force cefi 2025-01-01 2026-04-18 full
#
# Narrow-scope filters (all optional; omit for full-category sweep):
#   --data-types trades                                # space-separated; MDPS_DATA_TYPES
#   --venues "BINANCE-FUTURES BYBIT"                   # space-separated; MDPS_VENUES
#   --instrument-ids "BINANCE-FUTURES:PERPETUAL:BTCUSDT BINANCE-FUTURES:PERPETUAL:ETHUSDT"
#                                                      # RECOMMENDED: canonical VENUE:INSTRUMENT_TYPE:SYMBOL
#                                                      # Each segment matched independently against hive path:
#                                                      #   venue=BINANCE-FUTURES/instrument_type=perpetual/BTCUSDT.parquet
#                                                      # LEGACY (deprecated): bare symbol e.g. "BTCUSDT"
#                                                      #   Triggers substring match + deprecation log; will be
#                                                      #   removed in a future release. Use canonical form.
#                                                      # Codex SSOT: codex/06-coding-standards/cli-convention.md
#                                                      #   § "Instrument Identity and CLI Granularity"
#                                                      # UAC parser: from unified_api_contracts.canonical import parse_instrument_key
#   --output-bucket market-data-tick-cefi-test-central-element-323112
#                                                      # MDPS_OUTPUT_BUCKET_{CAT}; writes go here
#
# Example — 3.X-3 16-day narrow-scope canary (plan mdps_filter_pushdown_memory_audit_and_fix_2026_05_28.md):
#   bash launch-mdps-backfill-vm.sh \
#     --data-types trades \
#     --venues "BINANCE-FUTURES BYBIT" \
#     --instrument-ids "BINANCE-FUTURES:PERPETUAL:BTCUSDT BINANCE-FUTURES:PERPETUAL:ETHUSDT BYBIT:PERPETUAL:BTCUSDT BYBIT:PERPETUAL:ETHUSDT" \
#     --output-bucket market-data-tick-cefi-test-central-element-323112 \
#     cefi 2026-04-15 2026-04-30 full
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
SOURCE_BUCKET_OVERRIDE=""
FILTER_DATA_TYPES=""
FILTER_VENUES=""
FILTER_INSTRUMENT_IDS=""
OUTPUT_BUCKET_OVERRIDE=""
# --force / FORCE=true env (universal launcher contract, deployment-api backfill_launch.py
# _build_argv): threads `--force` into the MDPS `process` CLI, which sets
# `_write_candles(force=True)` so already-`captured` cells are re-written instead of
# short-circuiting on `blob_exists`. This is the densify-reprocess lever for the
# MDPS leading-NaN historical remediation (issue mdps_state_adapter_leading_nan_audit_2026_05_29.md
# `[DATA] P1`). Without it the `mdps-backfill` VM_TASK branch in setup-data-pipeline-vm.sh
# runs VM_BACKFILL_CMD verbatim and never re-processes fresh-in-manifest cells.
FORCE_REPROCESS="${FORCE:-false}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# Deterministic VM-name override (--vm-name). Empty by default → the auto RUN_TS
# name (line ~173). Set by the pipeline-e2e-check driver so the shared UTL
# launch_vm_and_wait engine can poll a known gs://.../vm-logs/<vm>/ path. The
# override MUST still prefix-match a registered VM_PREFIX_TO_BUCKET entry
# (mdps-backfill-<cat>-...), so the driver names it mdps-backfill-<cat>-pipelinecheck-<ts>.
VM_NAME_OVERRIDE="${VM_NAME_OVERRIDE:-}"

# Pre-parse --env, --source-bucket, --force, and narrow-scope filter flags before positional args.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --source-bucket) SOURCE_BUCKET_OVERRIDE="$2"; shift 2 ;;
        --data-types) FILTER_DATA_TYPES="$2"; shift 2 ;;
        --venues) FILTER_VENUES="$2"; shift 2 ;;
        --instrument-ids) FILTER_INSTRUMENT_IDS="$2"; shift 2 ;;
        --output-bucket) OUTPUT_BUCKET_OVERRIDE="$2"; shift 2 ;;
        --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
        --force) FORCE_REPROCESS="true"; shift ;;
        --on-demand)   ON_DEMAND=true; shift ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]:-}"

# RESUME_* env fallbacks (SPOT-preemption relaunch support,
# cefi_completion_program_2026_07_15.md P0 "Close the SPOT-preemption relaunch
# gap"): RelaunchPreemptedVm re-invokes this launcher with ZERO CLI args, only
# the env lc_write_launch_params captured — without a fallback here a bare
# re-invocation hits the positional-arg usage error below. Explicit positional
# args always win.
ASSET_GROUP="${1:-${RESUME_ASSET_GROUP:-}}"
START_DATE="${2:-${RESUME_START_DATE:-}}"
END_DATE="${3:-${RESUME_END_DATE:-}}"
MODE="${4:-${RESUME_MODE:-dry}}"  # dry | full

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# ── Auto-pin UAC to the current LDR-tip tarball (P0 contract-propagation fix) ──
# This launcher pins UTL/MDPS into VM metadata but historically NOT UAC, so a UAC
# schema/contract change was never version-pinned at launch and the VM fell to the
# unpinned floating pull — a stale contract could reach a service VM even with a
# correct current tarball (plans/active/issues/mdps_vm_stale_uac_contract_propagation_2026_07_20.md).
# Resolve the sha of the UAC tarball that WILL actually be installed (the floating
# manifest's commit_sha, emitted only when its @<sha> pair provably exists) and pin
# it — determinism + retention-exemption + setup's per-tarball manifest-sha assert.
# An operator-exported UAC_TARBALL_SHA always wins (explicit pin).
if [[ -z "${UAC_TARBALL_SHA:-}" ]]; then
    UAC_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" unified-api-contracts)"
fi

# Filter pass-through (added 2026-05-28 for filter-pushdown canary verification —
# see unified-trading-pm/plans/active/mdps_filter_pushdown_memory_audit_and_fix_2026_05_28.md
# Phase 3). Unset by default → no behavior change for existing callers; when
# set, exported on the VM so cli/main.py::_build_legacy_argv translates them
# into --data-types / --venues / --instrument-ids.
MDPS_DATA_TYPES_OVERRIDE="${MDPS_DATA_TYPES:-}"
MDPS_VENUES_OVERRIDE="${MDPS_VENUES:-}"
MDPS_INSTRUMENT_IDS_OVERRIDE="${MDPS_INSTRUMENT_IDS:-}"
MDPS_MAX_WORKERS_OVERRIDE="${MDPS_MAX_WORKERS:-}"
# Test-isolation write target (config.py:497 — MDPS_OUTPUT_BUCKET_{CAT})
MDPS_OUTPUT_BUCKET_CEFI_OVERRIDE="${MDPS_OUTPUT_BUCKET_CEFI:-}"
MDPS_OUTPUT_BUCKET_DEFI_OVERRIDE="${MDPS_OUTPUT_BUCKET_DEFI:-}"
MDPS_OUTPUT_BUCKET_TRADFI_OVERRIDE="${MDPS_OUTPUT_BUCKET_TRADFI:-}"
MDPS_OUTPUT_BUCKET_SPORTS_OVERRIDE="${MDPS_OUTPUT_BUCKET_SPORTS:-}"
MDPS_OUTPUT_BUCKET_PREDICTION_OVERRIDE="${MDPS_OUTPUT_BUCKET_PREDICTION:-}"

if [[ -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Usage: $0 [--env prod|staging|dev] [--source-bucket <b>] [--data-types <t>] [--venues <v>] [--instrument-ids <i>] [--output-bucket <b>] [--force] <cefi|tradfi|defi|sports|prediction|all> <start-date> <end-date> [dry|full]"
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

RUN_TS="$(date +%Y%m%d-%H%M%S)"

_category_upper_for() {
    case "$1" in
        cefi)       echo "CEFI" ;;
        tradfi)     echo "TRADFI" ;;
        defi)       echo "DEFI" ;;
        sports)     echo "SPORTS" ;;
        prediction) echo "PREDICTION" ;;
        *) echo ""; return 1 ;;
    esac
}

_launch() {
    local cat="$1"
    local cat_upper; cat_upper="$(_category_upper_for "$cat")" || { echo "Unknown category: $cat"; return 1; }
    local bucket_suffix=""
    if [[ -n "$SOURCE_BUCKET_OVERRIDE" ]]; then
        bucket_suffix="-$(echo "$SOURCE_BUCKET_OVERRIDE" | sed 's/-central-element-323112//' | sed "s/^market-data-tick-${cat}/main/" | sed 's/-central-element-323112//' | cut -c1-16 | tr '_' '-')"
    fi
    # --vm-name override wins (single-category launches only — the `all` fan-out
    # must NOT set it or the 5 sequential launches would collide on one name).
    local vm_name="mdps-backfill-${cat}${bucket_suffix}-${RUN_TS}"
    if [[ -n "$VM_NAME_OVERRIDE" ]]; then
        vm_name="$VM_NAME_OVERRIDE"
    fi

    # MDPS CLI quirk: service uses ServiceBootstrap at the top level (which
    # requires --operation and --mode) BUT has add_asset_group_arg=False, so
    # --asset-group is NOT a recognised top-level arg. The legacy `process`
    # subparser accepts per-asset-group `--CEFI/--DEFI/...` flags, but those
    # are only reachable if the bootstrap bridge (_build_legacy_argv in
    # cli/main.py) reads MDPS_ASSET_GROUP env var and translates to --CEFI.
    # Hence: export MDPS_ASSET_GROUP in the command string so the Python
    # bridge picks it up. --asset-group on the top-level fails with
    # "unrecognized arguments" (2026-04-19 VM launches proved this).
    # Source-bucket env var is read by MDPS config.py (line 75-80) to locate the
    # raw tick bucket. Without it, every shard fails with "No source bucket
    # configured for category=…" before any candle is produced.
    local env_short
    case "$DEPLOYMENT_ENV" in
        prod)    env_short="prd" ;;
        staging) env_short="stg" ;;
        dev)     env_short="dev" ;;
        *)       env_short="$DEPLOYMENT_ENV" ;;
    esac
    local source_bucket="${SOURCE_BUCKET_OVERRIDE:-market-data-tick-${cat}-${env_short}-${PROJECT}}"
    local cmd="PROTOCOL_DATA_SOURCE_BUCKET_${cat_upper}=${source_bucket}"
    cmd="$cmd MDPS_ASSET_GROUP=$cat_upper"
    # Narrow-scope filter env vars — only set when flags are passed.
    # Values with spaces are single-quoted so bash -c "$cmd" parses them
    # correctly on the VM. Canonical instrument IDs: VENUE:ITYPE:SYMBOL
    # (supported since market-data-processing-service@9ea08c8).
    if [[ -n "$FILTER_DATA_TYPES" ]]; then
        cmd="$cmd MDPS_DATA_TYPES='${FILTER_DATA_TYPES}'"
    fi
    if [[ -n "$FILTER_VENUES" ]]; then
        cmd="$cmd MDPS_VENUES='${FILTER_VENUES}'"
    fi
    if [[ -n "$FILTER_INSTRUMENT_IDS" ]]; then
        cmd="$cmd MDPS_INSTRUMENT_IDS='${FILTER_INSTRUMENT_IDS}'"
    fi
    if [[ -n "$OUTPUT_BUCKET_OVERRIDE" ]]; then
        cmd="$cmd MDPS_OUTPUT_BUCKET_${cat_upper}=${OUTPUT_BUCKET_OVERRIDE}"
    fi
    # Sports MDPS catch-up: pre-2026 dates often lack upstream raw because
    # sports forward-poll has only been running recently. The dependency
    # check would otherwise abort the run on the first empty date. Bridge
    # the existing ``--skip-dependency-check`` flag (legacy subparser) via
    # the SKIP_DEPENDENCY_CHECK env var that ``cli/main.py::_build_legacy_argv``
    # honours at translate time. Other asset_groups keep the dep check on so
    # we fail fast if upstream is genuinely missing.
    if [[ "$cat" == "sports" ]]; then
        cmd="$cmd SKIP_DEPENDENCY_CHECK=true"
    fi
    # Filter overrides (canary / narrow-scope reruns) — env-var pass-through.
    [[ -n "$MDPS_DATA_TYPES_OVERRIDE" ]]    && cmd="$cmd MDPS_DATA_TYPES='$MDPS_DATA_TYPES_OVERRIDE'"
    [[ -n "$MDPS_VENUES_OVERRIDE" ]]        && cmd="$cmd MDPS_VENUES='$MDPS_VENUES_OVERRIDE'"
    [[ -n "$MDPS_INSTRUMENT_IDS_OVERRIDE" ]] && cmd="$cmd MDPS_INSTRUMENT_IDS='$MDPS_INSTRUMENT_IDS_OVERRIDE'"
    [[ -n "$MDPS_MAX_WORKERS_OVERRIDE" ]]   && cmd="MAX_WORKERS=$MDPS_MAX_WORKERS_OVERRIDE $cmd"
    # MDPS_OUTPUT_BUCKET_{CAT} — per-asset-group output bucket override (test-isolation).
    local _out_var="MDPS_OUTPUT_BUCKET_${cat_upper}_OVERRIDE"
    local _out_val="${!_out_var:-}"
    [[ -n "$_out_val" ]] && cmd="$cmd MDPS_OUTPUT_BUCKET_${cat_upper}=$_out_val"
    cmd="$cmd python -m market_data_processing_service --operation process --mode batch"
    cmd="$cmd --start-date $START_DATE --end-date $END_DATE"
    if [[ "$MODE" == "dry" ]]; then
        cmd="$cmd --dry-run"
    fi
    # Force-reprocess already-captured cells (densify reprocess) — re-writes existing
    # candle parquets instead of SKIP-on-fresh. Threaded into VM_BACKFILL_CMD because
    # the mdps-backfill VM_TASK branch runs the cmd verbatim (it does NOT honour the
    # VM_FORCE→--force metadata bridge that the download/instruments BASE_CLI branches use).
    if [[ "$FORCE_REPROCESS" == "true" ]]; then
        cmd="$cmd --force"
    fi

    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    local PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

    echo "Launching $vm_name [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
    echo "  cmd: $cmd"
    echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

    local md="VM_TASK=mdps-backfill"
    md="${md},VM_SERVICE=market_data_processing_service"
    md="${md},VM_OPERATION=backfill-${cat}"
    md="${md},VM_ASSET_GROUP=$(echo "$cat" | tr '[:lower:]' '[:upper:]')"
    md="${md},VM_START_DATE=${START_DATE}"
    md="${md},VM_END_DATE=${END_DATE}"
    md="${md},VM_BACKFILL_CMD=${cmd}"
    md="${md},VM_BACKFILL_MODE=${MODE}"
    md="${md},VM_FORCE=${FORCE_REPROCESS}"
    md="${md},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
    md="${md},VM_SHUTDOWN_ON_COMPLETION=true"
    [[ -n "${UTL_TARBALL_SHA:-}" ]]  && md="${md},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
    [[ -n "${UAC_TARBALL_SHA:-}" ]]  && md="${md},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"
    [[ -n "${MDPS_TARBALL_SHA:-}" ]] && md="${md},MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA}"
    # Durable pin registry — instance metadata dies with the instance; this
    # record is what survives into the preemption-relaunch window and what
    # exempts these tarballs from the nightly retention sweep. An unset SHA is
    # recorded as a DELIBERATE float (not a pin to nothing), which is what lets
    # the sweep fail closed on genuinely unknown VMs without blocking forever.
    lc_write_tarball_pin_record "$vm_name" "$PROJECT" "launch-mdps-backfill-vm.sh" \
        "UTL_TARBALL_SHA=${UTL_TARBALL_SHA:-}" \
        "UAC_TARBALL_SHA=${UAC_TARBALL_SHA:-}" \
        "MDPS_TARBALL_SHA=${MDPS_TARBALL_SHA:-}"
    [[ -n "$FILTER_DATA_TYPES" ]] && md="${md},VM_DATA_TYPES=${FILTER_DATA_TYPES// /;}"
    [[ -n "$FILTER_VENUES" ]] && md="${md},VM_VENUES=${FILTER_VENUES// /;}"
    [[ -n "$FILTER_INSTRUMENT_IDS" ]] && md="${md},VM_INSTRUMENT_IDS=${FILTER_INSTRUMENT_IDS// /;}"
    [[ -n "$OUTPUT_BUCKET_OVERRIDE" ]] && md="${md},VM_OUTPUT_BUCKET=${OUTPUT_BUCKET_OVERRIDE}"

    # shellcheck disable=SC2086
    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "$CODE_BUCKET" \
            market-data-processing-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    # SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
    # "Close the SPOT-preemption relaunch gap"): persist the EXACT category/window
    # this VM was launched with (via the RESUME_* env fallback above) plus the
    # SAME vm_name (so a relaunch keeps writing to this shard's PROGRESS.json
    # checkpoint trail) and the existing MDPS_* narrow-scope overrides, so
    # exit_code_fleet_monitor's PREEMPTED auto_recover actuator
    # (relaunch_backfill_vm.RelaunchPreemptedVm) reproduces the SAME shard
    # instead of hitting the usage error / a blind default. Best-effort.
    lc_write_launch_params "$vm_name" "$PROJECT" "launch-mdps-backfill-vm.sh" \
        "VM_NAME_OVERRIDE=${vm_name}" \
        "RESUME_ASSET_GROUP=${cat}" \
        "RESUME_START_DATE=${START_DATE}" \
        "RESUME_END_DATE=${END_DATE}" \
        "RESUME_MODE=${MODE}" \
        "FORCE=${FORCE_REPROCESS}" \
        "MDPS_DATA_TYPES=${FILTER_DATA_TYPES:-$MDPS_DATA_TYPES_OVERRIDE}" \
        "MDPS_VENUES=${FILTER_VENUES:-$MDPS_VENUES_OVERRIDE}" \
        "MDPS_INSTRUMENT_IDS=${FILTER_INSTRUMENT_IDS:-$MDPS_INSTRUMENT_IDS_OVERRIDE}" \
        "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

    # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
    # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
    # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
    # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
    # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
    # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
    gcloud compute instances create "$vm_name" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --scopes=cloud-platform \
        ${PROVISIONING_FLAGS} \
        --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${md}" \
        --labels=purpose=mdps-backfill,category="${cat}",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
    echo "  SSH: gcloud compute ssh $vm_name --zone=$ZONE"
    echo "  Delete: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
    echo ""
}

case "$ASSET_GROUP" in
    cefi|tradfi|defi|sports|prediction) _launch "$ASSET_GROUP" ;;
    all)
        _launch cefi
        _launch tradfi
        _launch defi
        _launch sports
        _launch prediction
        ;;
    *) echo "Unknown category: $ASSET_GROUP"; exit 2 ;;
esac

echo "Run timestamp: $RUN_TS"
echo "Mode: $MODE (dry = --dry-run; full = live writes)"
echo ""
echo "Reminder: post-backfill, run manifest reconciliation via:"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('market-data-tick-${ASSET_GROUP}-central-element-323112', \\"
echo "      service_name='market-data-processing-service', prefix='processed_candles/by_date')\""
