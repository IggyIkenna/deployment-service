#!/usr/bin/env bash
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
DELEGATE="${SCRIPT_DIR}/launch-features-backfill-vm.sh"

if [[ ! -x "$DELEGATE" ]]; then
    echo "ERROR: required delegate launcher not found / not executable: $DELEGATE" >&2
    exit 1
fi

echo "==> Delegating to $(basename "$DELEGATE") with onchain DEFI baked in"
echo "    start=$START_DATE end=$END_DATE mode=$MODE"

# Forward env overrides explicitly so the delegate sees them in its
# `${FEATURE_GROUP:-ALL}` / `${SKIP_DEPENDENCY_CHECK:-}` / `${FORCE:-}`
# expansions even if shells run in a stricter inherit mode.
exec env \
    "FEATURE_GROUP=${FEATURE_GROUP:-ALL}" \
    "SKIP_DEPENDENCY_CHECK=${SKIP_DEPENDENCY_CHECK:-}" \
    "FORCE=${FORCE:-}" \
    bash "$DELEGATE" onchain DEFI "$START_DATE" "$END_DATE" "$MODE"
