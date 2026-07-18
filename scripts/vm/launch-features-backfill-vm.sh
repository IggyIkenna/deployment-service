#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# DEPRECATED 2026-05-08 (Phase 8A of features_repo_consolidation_2026_05_08).
# Superseded by `launch-features-vm.sh`, which dispatches the consolidated
# features-service via `--feature-family <name>` (8 families) instead of
# requiring a separate launcher per family. Mapping:
#     bash launch-features-backfill-vm.sh delta-one CEFI 2020-01-01 2026-04-18 full
#   becomes
#     bash launch-features-vm.sh \
#         --feature-family delta_one --asset-group CEFI \
#         --start-date 2020-01-01 --end-date 2026-04-18 --launch-mode full
# Note the underscore (`delta_one`) in --feature-family — UAC FeatureFamily
# enum uses underscores; legacy positional arg used dashes (`delta-one`).
# This wrapper preserves the legacy positional invocation by translating to
# the new launcher; new callers should use `launch-features-vm.sh` directly.
#
# Phase 5c.3 features backfill VM launcher — spawn one VM per
# (feature_service × category) cell. The service CLIs follow the standardised
# axes defined in codex/06-coding-standards/cli-convention.md:
#     --operation backfill --mode batch --asset-group {CEFI|TRADFI|DEFI|SPORTS|PREDICTION}
#     --start-date YYYY-MM-DD --end-date YYYY-MM-DD
#
# Input reads: features read from
#   * gs://market-data-tick-{category}-*/{raw_tick_data,processed_candles}/...
#   * gs://instruments-store-{category}-*/...
# Output writes: per feature_group + category bucket layout, e.g.
#   * gs://features-{group}-{category}-*/features/by_date/day=/feature_group=/timeframe=/...
# Manifest: availability_index.parquet in each features bucket.
#
# Viable (feature_service × category) cells (Phase 5c.3 matrix — ≈29 cells):
#     features-delta-one      : CEFI, DEFI, TRADFI, PREDICTION
#     features-volatility     : CEFI, TRADFI
#     features-onchain        : DEFI only
#     features-sports         : SPORTS only
#     features-calendar       : CEFI, TRADFI
#     features-multi-timeframe: CEFI, DEFI, TRADFI
#     features-cross-instrument: CEFI, TRADFI, PREDICTION
#     features-commodity      : TRADFI only
#
# Scope per category (mirrors Phase 5b.3):
#   CeFi/TradFi/DeFi 2020-01-01 → 2026-04-18
#   Sports           2019-01-01 → 2026-04-18
#   Prediction       2025-03-14 → 2026-04-18
#
# Operational safety:
#   * 50 GB boot disk.
#   * DEPLOYMENT_STARTED/PROGRESS/COMPLETED events via deployment_heartbeat.py.
#   * stdout/stderr tee'd via vm-exec-with-gcs-tee.sh.
#   * asia-northeast1-c.
#   * Launch in waves: 4 concurrent VMs per wave (deployment-service budget).
#
# Usage:
#   bash launch-features-backfill-vm.sh <feature_service> <category> <start> <end> [dry|full]
#
# Examples:
#   bash launch-features-backfill-vm.sh delta-one CEFI       2020-01-01 2026-04-18 dry
#   bash launch-features-backfill-vm.sh onchain   DEFI       2020-01-01 2026-04-18 full
#   bash launch-features-backfill-vm.sh sports    SPORTS     2019-01-01 2026-04-18 full
#   bash launch-features-backfill-vm.sh commodity TRADFI     2020-01-01 2026-04-18 full

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SERVICE_SHORT="${1:-}"
ASSET_GROUP="${2:-}"
START_DATE="${3:-}"
END_DATE="${4:-}"
MODE="${5:-dry}"  # dry | full
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND="${ON_DEMAND:-false}"

# DEPRECATED 2026-05-08 / hard-redirect 2026-05-17 (slot-1-main).
# Per `plans/active/issues/features_vm_uv_resolution_unsatisfiable_2026_05_16.md`
# Phase 3 action item: redirect to the consolidated launcher rather than
# silently invoking per-family legacy modules against stale per-family
# tarballs. Mapping rule: legacy positional dashes → consolidated
# `--feature-family` underscores. `calendar` legacy positional must still
# pass `--asset-group GLOBAL` since the consolidated launcher requires
# the flag.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONSOLIDATED="${SCRIPT_DIR}/launch-features-vm.sh"
if [[ -x "$CONSOLIDATED" || -f "$CONSOLIDATED" ]] && [[ -n "$SERVICE_SHORT" ]] && [[ -n "$ASSET_GROUP" ]] && [[ -n "$START_DATE" ]] && [[ -n "$END_DATE" ]]; then
    _FAMILY="${SERVICE_SHORT//-/_}"
    _CONS_ASSET_GROUP="$ASSET_GROUP"
    # calendar family does not take --asset-group in the per-family CLI, but
    # the consolidated launcher's required flag is satisfied by the special
    # sentinel GLOBAL (see launch-features-vm.sh header line 67-68).
    if [[ "$SERVICE_SHORT" == "calendar" ]]; then
        _CONS_ASSET_GROUP="GLOBAL"
    fi
    cat >&2 <<EOM
==> [DEPRECATED] launch-features-backfill-vm.sh
    Redirecting to consolidated launcher (Phase 8A canonical path):
        bash launch-features-vm.sh --feature-family ${_FAMILY} --asset-group ${_CONS_ASSET_GROUP} \\
            --start-date ${START_DATE} --end-date ${END_DATE} \\
            --launch-mode ${MODE} --env ${DEPLOYMENT_ENV}
    New callers should use launch-features-vm.sh directly.
    See plans/active/features_repo_consolidation_2026_05_08.md § Phase 8A.

EOM
    exec env \
        "FEATURE_GROUP=${FEATURE_GROUP:-ALL}" \
        "SKIP_DEPENDENCY_CHECK=${SKIP_DEPENDENCY_CHECK:-}" \
        "FORCE=${FORCE:-}" \
        bash "$CONSOLIDATED" \
            --feature-family "${_FAMILY}" \
            --asset-group "${_CONS_ASSET_GROUP}" \
            --start-date "${START_DATE}" \
            --end-date "${END_DATE}" \
            --launch-mode "${MODE}" \
            --env "${DEPLOYMENT_ENV}"
fi
# If we get here, args were incomplete — fall through to the usage banner
# (the legacy per-family code path below is unreachable in the common case;
# it stays only as a hard-fail safety net for callers passing partial args).

# Allow --env <prod|staging|dev> override (kept as a 6th positional-or-flag
# slot for legacy callers). Shift positional args to the end if --env flag
# is supplied via remaining tail-args.
_REMAINING_ARGS=("${@:6}")
_PARSED_ARGS=()
while [[ ${#_REMAINING_ARGS[@]} -gt 0 ]]; do
    case "${_REMAINING_ARGS[0]}" in
        --env)
            DEPLOYMENT_ENV="${_REMAINING_ARGS[1]:-prod}"
            _REMAINING_ARGS=("${_REMAINING_ARGS[@]:2}")
            ;;
        *)
            _PARSED_ARGS+=("${_REMAINING_ARGS[0]}")
            _REMAINING_ARGS=("${_REMAINING_ARGS[@]:1}")
            ;;
    esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if [[ -z "$SERVICE_SHORT" || -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    cat <<EOF
Usage: $0 <feature_service_short> <ASSET_GROUP> <start-date> <end-date> [dry|full] [--env <prod|staging|dev>]

feature_service_short ∈ {
  delta-one, volatility, onchain, sports,
  calendar, multi-timeframe, cross-instrument, commodity
}
ASSET_GROUP ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION }
EOF
    exit 2
fi

# (feature_service_short, ASSET_GROUP) validity matrix.
_is_viable_cell() {
    local svc="$1" cat="$2"
    case "$svc/$cat" in
        delta-one/CEFI|delta-one/DEFI|delta-one/TRADFI|delta-one/PREDICTION) return 0 ;;
        volatility/CEFI|volatility/TRADFI) return 0 ;;
        onchain/DEFI) return 0 ;;
        sports/SPORTS) return 0 ;;
        calendar/CEFI|calendar/TRADFI) return 0 ;;
        multi-timeframe/CEFI|multi-timeframe/DEFI|multi-timeframe/TRADFI) return 0 ;;
        cross-instrument/CEFI|cross-instrument/TRADFI|cross-instrument/PREDICTION) return 0 ;;
        commodity/TRADFI) return 0 ;;
        *) return 1 ;;
    esac
}

if ! _is_viable_cell "$SERVICE_SHORT" "$ASSET_GROUP"; then
    echo "Not a viable (service × category) cell: $SERVICE_SHORT × $ASSET_GROUP"
    echo "See header for the viable matrix."
    exit 2
fi

# Python module name for each service — matches the dash→underscore rule.
_python_module_for() {
    case "$1" in
        delta-one)        echo "features_delta_one_service" ;;
        volatility)       echo "features_volatility_service" ;;
        onchain)          echo "features_onchain_service" ;;
        sports)           echo "features_sports_service" ;;
        calendar)         echo "features_calendar_service" ;;
        multi-timeframe)  echo "features_multi_timeframe_service" ;;
        cross-instrument) echo "features_cross_instrument_service" ;;
        commodity)        echo "features_commodity_service" ;;
        *) echo ""; return 1 ;;
    esac
}

PY_MODULE="$(_python_module_for "$SERVICE_SHORT")"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
ASSET_GROUP_LOWER="$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"  # bash-3-compat (${var,,} is bash 4+)
VM_NAME="features-${SERVICE_SHORT}-${ASSET_GROUP_LOWER}-backfill-${RUN_TS}"

# Canonical service CLI convention (codex/06-coding-standards/cli-convention.md).
# 2026-05-01: features-* services standardised on --operation=compute (with
# --mode batch for backfill, --mode live for pubsub). The legacy
# --operation=backfill choice was removed during the asset_group rename
# (Wave A); using it here caused argparse exit rc=2 in 16 backfill VMs.
CMD="python -m ${PY_MODULE} --operation compute --mode batch"
CMD="$CMD --start-date ${START_DATE} --end-date ${END_DATE}"
# 2026-05-01: calendar's CLI does NOT accept --asset-group (output is global,
# not per-asset_group; calendar_orchestrator.py:373 passes asset_group=""
# through to the manifest writer). Every other features-* service requires it.
case "$SERVICE_SHORT" in
    calendar) ;;
    *) CMD="$CMD --asset-group ${ASSET_GROUP}" ;;
esac
# 2026-05-01: delta-one / volatility / onchain require --feature-group; the
# other services don't accept the flag at all. Backfill defaults to ALL —
# strategy-/scenario-driven targeted runs override via FEATURE_GROUP env var.
# 2026-05-05: also support SKIP_DEPENDENCY_CHECK env override for narrow-scope
# feature-group runs that don't actually read processed_candles (e.g. lst_yields,
# lending_rates, funding_oi all read raw_tick_data directly per the carry-tracer
# pipeline) — bypasses the global preflight that would otherwise block.
case "$SERVICE_SHORT" in
    delta-one|volatility|onchain) CMD="$CMD --feature-group ${FEATURE_GROUP:-ALL}" ;;
esac
if [[ -n "${SKIP_DEPENDENCY_CHECK:-}" ]]; then
    CMD="$CMD --skip-dependency-check"
fi
# 2026-05-07: FORCE env var override for reprocess scenarios. Without
# this, the orchestrator's check_shard_freshness skip-if-exists fires
# the moment the manifest shows captured rows for the date range —
# even when the goal is to REWRITE the parquets with new schema (e.g.
# Phase 9 canonical column emission per features-onchain@7f1b2a1).
if [[ -n "${FORCE:-}" ]]; then
    CMD="$CMD --force"
fi
if [[ "$MODE" == "dry" ]]; then
    CMD="$CMD --dry-run"
fi

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  cmd: $CMD"
echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

MD="VM_TASK=features-backfill"
MD="${MD},VM_SERVICE=${PY_MODULE}"
MD="${MD},VM_OPERATION=backfill-${SERVICE_SHORT}-${ASSET_GROUP_LOWER}"
MD="${MD},VM_ASSET_GROUP=${ASSET_GROUP}"
MD="${MD},VM_START_DATE=${START_DATE}"
MD="${MD},VM_END_DATE=${END_DATE}"
MD="${MD},VM_BACKFILL_CMD=${CMD}"
MD="${MD},VM_BACKFILL_MODE=${MODE}"
MD="${MD},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# 2026-05-01: opt-in auto-delete after task completion (read by
# vm-exec-with-gcs-tee.sh:253). Without this, one-shot backfill VMs sat
# RUNNING idle after rc!=0 (or even rc==0) until manually killed — cost leak.
MD="${MD},VM_SHUTDOWN_ON_COMPLETION=true"

# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    ${PROVISIONING_FLAGS} \
    # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
    # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
    # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
    # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
    # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
    # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${MD}" \
    --labels=purpose=features-backfill,service="${SERVICE_SHORT}",category="${ASSET_GROUP_LOWER}",mode="${MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
echo "  SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Post-backfill manifest rebuild (one per features bucket):"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('features-${SERVICE_SHORT}-${ASSET_GROUP_LOWER}-central-element-323112', \\"
echo "      service_name='features-${SERVICE_SHORT}-service', prefix='features/by_date')\""
