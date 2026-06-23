#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# DEPRECATED 2026-05-08 (Phase 8A of features_repo_consolidation_2026_05_08).
# Superseded by `launch-features-vm.sh --feature-family onchain --asset-group DEFI`.
# This wrapper still works (delegates to the new launcher with onchain DEFI
# baked in); new callers should use `launch-features-vm.sh` directly.
#
# Launch GCE VM for features-onchain-service backfill (DeFi-only).
#
# features-onchain-service is the DeFi-only feature service in the
# (feature_service × asset_group) viability matrix declared in
# launch-features-backfill-vm.sh — i.e. only the (onchain × DEFI) cell
# is valid (every other cell raises "Not a viable cell" loudly).
# Rather than re-implementing the launcher, this thin wrapper delegates
# to launch-features-backfill-vm.sh with `onchain DEFI` baked in,
# preserving the canonical shape (MANIFEST_PER_VM_SHARDS=true,
# VM_NAME=features-onchain-defi-backfill-{ts}, RUN_TS, watchdog
# heartbeat sidecar via setup-data-pipeline-vm.sh).
#
# Created 2026-05-08 (Tab 5 follow-up cycle) to fill the third
# missing-on-disk entry in `_SERVICE_LAUNCHER_SCRIPTS` of
# `deployment-api/deployment_api/services/deploy_missing.py` line 66
# (registered slug: `features-onchain-service`). Without this script,
# the Deploy-Missing UI button silently broke for features-onchain
# leaves: the API resolved the path, the operator copied the
# command, ran it, and got "No such file or directory."
#
# Plan: launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md
# § Tab 11 audit + top-10 selection (this fills the residual gap).
#
# Naming: VM prefix `features-onchain-defi-backfill-` matches the
# canonical features-* shape (see `launch-features-backfill-vm.sh`
# VM_NAME assembly). Watchdog catch-all `features-` heartbeat-only
# entry already covers it (`vm_zombie_watchdog.py:297`); no new
# explicit prefix needed.
#
# Usage (matches the underlying launcher's positional args minus the
# pre-baked `onchain DEFI`):
#   bash launch-features-onchain-backfill-vm.sh <start> <end> [dry|full]
#
# Examples:
#   bash launch-features-onchain-backfill-vm.sh 2020-01-01 2026-04-18 dry
#   bash launch-features-onchain-backfill-vm.sh 2020-01-01 2026-04-18 full
#
# Optional environment overrides honoured by the underlying launcher:
#   FEATURE_GROUP=lst_yields|lending_rates|funding_oi|...|ALL  (default ALL)
#   SKIP_DEPENDENCY_CHECK=1   (narrow-scope feature-group runs)
#   FORCE=1                   (rewrite parquets with new schema)

set -euo pipefail

# Pre-parse --dry-run anywhere in the arg list so it doesn't pollute the
# positional date args. Codified 2026-05-20 per
# plans/active/issues/launcher_dry_run_support_gap_2026_05_20.md.
DRY_RUN=false
_pos=()
for _arg in "$@"; do
    if [[ "$_arg" == "--dry-run" ]]; then
        DRY_RUN=true
    else
        _pos+=("$_arg")
    fi
done
set -- "${_pos[@]:+${_pos[@]}}"

START_DATE="${1:-}"
END_DATE="${2:-}"
MODE="${3:-dry}"  # dry | full

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    cat <<EOF
Usage: $0 <start-date> <end-date> [dry|full]

features-onchain-service is DeFi-only. Asset group is hardcoded to DEFI.

Examples:
  bash $0 2020-01-01 2026-04-18 dry
  bash $0 2020-01-01 2026-04-18 full

Env overrides (optional):
  FEATURE_GROUP=lst_yields | lending_rates | funding_oi | ALL  (default ALL)
  SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
  FORCE=1                   rewrite parquets even if manifest shows captured
EOF
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Redirect 2026-05-17 (slot-3): the prior body delegated to
# `launch-features-backfill-vm.sh onchain DEFI ...` which sets
# `VM_SERVICE=features_onchain_service` (legacy module name) → runs `python
# -m features_onchain_service` against the stale `features-onchain-service-code`
# tarball (last rebuilt 2026-05-08). After features_repo_consolidation_2026_05_08
# Phase 8A, the canonical module is `features_service` invoked via the
# consolidated launcher. Caused 1 wasted VM (features-onchain-defi-backfill-
# 20260516-220052 PREFLIGHT_SKIPPED rc=1) per
# `plans/active/issues/features_service_deprecated_launcher_wrappers_misroute_2026_05_16.md`.
# Redirect with a deprecation warning so old callers still work but route
# through the canonical path.
CONSOLIDATED="${SCRIPT_DIR}/launch-features-vm.sh"

if [[ ! -x "$CONSOLIDATED" ]]; then
    echo "ERROR: consolidated launcher not found / not executable: $CONSOLIDATED" >&2
    exit 1
fi

cat >&2 <<EOM
==> [DEPRECATED] launch-features-onchain-backfill-vm.sh
    Redirecting to consolidated launcher (Phase 8A canonical path):
        bash launch-features-vm.sh --feature-family onchain --asset-group DEFI \\
            --start-date $START_DATE --end-date $END_DATE \\
            --mode batch --launch-mode $MODE
    New callers should use launch-features-vm.sh directly.
    See plans/active/features_repo_consolidation_2026_05_08.md § Phase 8A.

EOM

if $DRY_RUN; then
    echo "[DRY-RUN] Would delegate to: $CONSOLIDATED"
    echo "[DRY-RUN]   args: --feature-family onchain --asset-group DEFI"
    echo "[DRY-RUN]         --start-date $START_DATE --end-date $END_DATE"
    echo "[DRY-RUN]         --mode batch --launch-mode $MODE"
    echo "[DRY-RUN]   env:  FEATURE_GROUP=${FEATURE_GROUP:-ALL} SKIP_DEPENDENCY_CHECK=${SKIP_DEPENDENCY_CHECK:-} FORCE=${FORCE:-}"
    echo "[DRY-RUN] No VM created."
    exit 0
fi

# Forward env overrides explicitly so the consolidated launcher's positional
# args + env reads pick them up.
exec env \
    "FEATURE_GROUP=${FEATURE_GROUP:-ALL}" \
    "SKIP_DEPENDENCY_CHECK=${SKIP_DEPENDENCY_CHECK:-}" \
    "FORCE=${FORCE:-}" \
    bash "$CONSOLIDATED" \
        --feature-family onchain \
        --asset-group DEFI \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        --mode batch \
        --launch-mode "$MODE"
