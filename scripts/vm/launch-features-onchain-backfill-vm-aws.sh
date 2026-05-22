#!/usr/bin/env bash
# AWS EC2 equivalent of launch-features-onchain-backfill-vm.sh.
# Thin wrapper: delegates to launch-features-backfill-vm-aws.sh with onchain DEFI baked in.
#
# GCP counterpart: launch-features-onchain-backfill-vm.sh
# features-onchain-service is DeFi-only (onchain × DEFI is the only viable cell).
#
# Usage:
#   bash launch-features-onchain-backfill-vm-aws.sh <start-date> <end-date> [dry|full]
#
# Examples:
#   bash launch-features-onchain-backfill-vm-aws.sh 2020-01-01 2026-04-18 dry
#   bash launch-features-onchain-backfill-vm-aws.sh 2020-01-01 2026-04-18 full
#
# Env overrides (forwarded to underlying launcher):
#   FEATURE_GROUP=lst_yields|lending_rates|funding_oi|...|ALL  (default ALL)
#   SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
#   FORCE=1                   rewrite parquets even if manifest shows captured
set -euo pipefail

START_DATE="${1:-}"
END_DATE="${2:-}"
MODE="${3:-dry}"

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
CONSOLIDATED="${SCRIPT_DIR}/launch-features-backfill-vm-aws.sh"

if [[ ! -x "$CONSOLIDATED" ]]; then
  echo "ERROR: consolidated AWS launcher not found / not executable: $CONSOLIDATED" >&2
  exit 1
fi

exec env \
  "FEATURE_GROUP=${FEATURE_GROUP:-ALL}" \
  "SKIP_DEPENDENCY_CHECK=${SKIP_DEPENDENCY_CHECK:-}" \
  "FORCE=${FORCE:-}" \
  bash "$CONSOLIDATED" \
    onchain DEFI \
    "$START_DATE" "$END_DATE" \
    "$MODE"
