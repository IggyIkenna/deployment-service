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

SERVICE_SHORT="${1:-}"
ASSET_GROUP="${2:-}"
START_DATE="${3:-}"
END_DATE="${4:-}"
MODE="${5:-dry}"  # dry | full
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

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

# If we get here, either a required positional arg was missing or the
# consolidated launcher (launch-features-vm.sh) could not be found. The
# standalone per-family VM-launch implementation this script used to fall
# through to (viable-cell matrix, python-module resolution, gcloud create)
# was deleted — it was unreachable dead code: the redirect above always
# fires once all 4 required args are present and the consolidated launcher
# exists, so nothing past this point could ever legitimately run it. This
# is now a plain usage/config-error fallback only.
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

echo "ERROR: consolidated launcher not found: $CONSOLIDATED" >&2
exit 1
