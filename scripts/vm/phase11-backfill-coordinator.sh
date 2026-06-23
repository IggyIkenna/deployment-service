#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Phase 11 — Post-migration backfill coordinator
#
# Sequences per-asset-group backfill VM launchers in dependency order:
#   (1) instruments-service  (upstream reference data)
#   (2) MTDS / MDPS          (tick + market data — depends on IS)
#   (3) features-*           (derived — depends on MTDS)
#
# Hard gates (BOTH required before --apply):
#   --phase6-green   Phase 6 VM fleet restart confirmed; VMs writing v8 steady-state
#   --phase7a-green  Phase 7A schema migration run; all manifest rows at v8
#
# Usage:
#   bash phase11-backfill-coordinator.sh --asset-group all --dry-run
#   bash phase11-backfill-coordinator.sh --asset-group defi \
#       --phase6-green --phase7a-green --apply
#   bash phase11-backfill-coordinator.sh --asset-group cefi \
#       --start 2026-01-01 --end 2026-05-20 --phase6-green --phase7a-green --apply
#
# Single-walk discipline (HARD RULE): each parquet walked exactly once per invocation.
# All writes land at schema_version=8 + typed empty_confirmed reasons.
# No fire-and-forget: each VM set is waited to TERMINATED before the next tier runs.
#
# Gated on:
#   plans/active/code_freeze_migrate_backfill_sequencing_2026_05_10.md § Phase 11
#   plans/active/human_work_backlog_2026_05_20.md item 13 (HUMAN-HARSH-PHASE-11-BACKFILL-TO-100PCT)

set -euo pipefail

export PATH="/home/ubuntu/google-cloud-sdk/bin:${PATH}"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ASSET_GROUP="all"
DRY_RUN=true
APPLY=false
PHASE6_GREEN=false
PHASE7A_GREEN=false
START_DATE=""
END_DATE=""
MAX_WAIT_SECONDS=7200   # 2h per launcher tier
POLL_INTERVAL=60

while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-group) ASSET_GROUP="$2"; shift 2 ;;
    --start)       START_DATE="$2"; shift 2 ;;
    --end)         END_DATE="$2"; shift 2 ;;
    --apply)       DRY_RUN=false; APPLY=true; shift ;;
    --dry-run)     DRY_RUN=true;  APPLY=false; shift ;;
    --phase6-green)  PHASE6_GREEN=true; shift ;;
    --phase7a-green) PHASE7A_GREEN=true; shift ;;
    --project)     PROJECT_ID="$2"; shift 2 ;;
    --zone)        ZONE="$2"; shift 2 ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Gate checks
# ---------------------------------------------------------------------------

if [[ "$APPLY" == "true" ]]; then
  if [[ "$PHASE6_GREEN" != "true" ]]; then
    echo "ERROR: --phase6-green required before --apply." \
         "Phase 6 VM fleet restart must be complete; VMs must be writing v8 rows." >&2
    exit 1
  fi
  if [[ "$PHASE7A_GREEN" != "true" ]]; then
    echo "ERROR: --phase7a-green required before --apply." \
         "Phase 7A schema migration (upgrade_manifest_to_v8.py --apply) must be run first." >&2
    exit 1
  fi
fi

case "$ASSET_GROUP" in
  all|defi|cefi|tradfi|sports|prediction) ;;
  *) echo "ERROR: --asset-group must be one of: all defi cefi tradfi sports prediction" >&2; exit 1 ;;
esac

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MODE_LABEL="[DRY-RUN]"
if [[ "$APPLY" == "true" ]]; then
  MODE_LABEL="[APPLY]"
fi

log() { echo "[phase11-coordinator] $*"; }

# Build date flag string for per-launcher overrides.
_date_flags() {
  local flags=""
  if [[ -n "$START_DATE" ]]; then flags="${flags} --start ${START_DATE}"; fi
  if [[ -n "$END_DATE" ]];   then flags="${flags} --end ${END_DATE}"; fi
  echo "${flags}"
}

# Run one launcher script; in dry-run mode just print the invocation.
run_launcher() {
  local launcher="$1"
  local description="${2:-}"
  local extra_flags="${3:-}"

  local date_flags
  date_flags="$(_date_flags)"

  local cmd="bash ${SCRIPT_DIR}/${launcher} ${date_flags} ${extra_flags}"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "  ${MODE_LABEL} Would run: ${cmd}  # ${description}"
    return 0
  fi

  log "  RUNNING: ${cmd}  # ${description}"
  bash "${SCRIPT_DIR}/${launcher}" ${date_flags} ${extra_flags}
}

# Wait for all VMs whose names start with <prefix> to leave RUNNING state.
# Exits 1 if still running after MAX_WAIT_SECONDS.
wait_for_vms() {
  local prefix="$1"
  local started_at
  started_at="$(date +%s)"

  log "  Waiting for VMs matching prefix '${prefix}' to complete..."
  while true; do
    local running
    running="$(gcloud compute instances list \
      --filter="name~\"^${prefix}\" AND status=RUNNING" \
      --zones="${ZONE}" \
      --project="${PROJECT_ID}" \
      --format='value(name)' 2>/dev/null | tr '\n' ' ' || true)"

    if [[ -z "$running" ]]; then
      log "  All '${prefix}*' VMs completed."
      return 0
    fi

    local elapsed=$(( $(date +%s) - started_at ))
    if [[ $elapsed -ge $MAX_WAIT_SECONDS ]]; then
      echo "ERROR: Timed out waiting for VMs: ${running}" >&2
      return 1
    fi

    log "  Still running: ${running}(elapsed ${elapsed}s / max ${MAX_WAIT_SECONDS}s)"
    sleep "$POLL_INTERVAL"
  done
}

# Conditionally wait — only in apply mode.
maybe_wait() {
  if [[ "$APPLY" == "true" ]]; then
    wait_for_vms "$1"
  fi
}

# ---------------------------------------------------------------------------
# Per-asset-group launcher sequences
# ---------------------------------------------------------------------------

run_defi() {
  log "=== DeFi backfill (est 184k MISSING_EXPECTED cells) ==="
  # Tier 1: instruments-service DeFi (upstream reference data)
  run_launcher "launch-defi-backfill-vm.sh" "IS DeFi instruments reference data"
  maybe_wait "instr-backfill-defi"

  # Tier 2: MTDS DeFi (tick data — depends on IS)
  run_launcher "launch-mtds-backfill-vm.sh" "MTDS DeFi raw_tick_data" "--asset-group defi"
  run_launcher "launch-mtds-lending-indices-backfill-vm.sh" "MTDS lending indices (DeFi)"
  run_launcher "launch-mtds-lst-rates-backfill-vm.sh"       "MTDS LST rates (Lido/RocketPool/Coinbase/Jito/Marinade)"
  run_launcher "launch-mtds-gas-fees-backfill-vm.sh"        "MTDS gas fees (Ethereum/Solana)"
  run_launcher "launch-mtds-dex-pools-backfill-vm.sh"       "MTDS DEX pools (Uniswap/Curve/Balancer)"
  run_launcher "launch-mtds-solana-drift-backfill-vm.sh"    "MTDS Solana Drift"
  run_launcher "launch-mtds-liquidations-backfill-vm.sh"    "MTDS liquidations"
  run_launcher "launch-mtds-vault-share-price-backfill-vm.sh" "MTDS vault share prices"
  maybe_wait "mtds-backfill"

  # Tier 3: features-onchain (depends on MTDS DeFi + IS DeFi)
  run_launcher "launch-features-onchain-backfill-vm.sh" "features-onchain (DeFi)"
  run_launcher "launch-governance-backfill-vm.sh"       "governance features (DeFi)"
  maybe_wait "features-onchain"
}

run_cefi() {
  log "=== CeFi backfill (est 16k MISSING_EXPECTED cells) ==="
  # Tier 1: instruments-service CeFi
  run_launcher "launch-cefi-instruments-backfill.sh" "IS CeFi instruments"
  maybe_wait "instr-backfill-cefi"

  # Tier 2: MTDS CeFi (sharded)
  run_launcher "launch-cefi-sharded-backfill.sh"         "MTDS CeFi sharded tick data"
  run_launcher "launch-mtds-perp-funding-backfill-vm.sh" "MTDS CeFi perp funding rates"
  run_launcher "launch-tier3-cefi-backfill.sh"           "MTDS CeFi tier-3 venues"
  maybe_wait "mtds-cefi"
}

run_tradfi() {
  log "=== TradFi backfill (est 7k MISSING_EXPECTED cells) ==="
  run_launcher "launch-tradfi-backfill-vm.sh"           "MTDS TradFi OHLCV / options"
  run_launcher "launch-mtds-extended-ohlcv-backfill.sh" "MTDS TradFi extended OHLCV"
  maybe_wait "mtds-tradfi"
}

run_sports() {
  log "=== Sports backfill (est 25k MISSING_EXPECTED cells) ==="
  # Tier 1: instruments-service Sports
  run_launcher "launch-instruments-backfill-vm.sh" "IS Sports instruments" "--asset-group sports"
  maybe_wait "instr-backfill-sports"

  # Tier 2: MTDS Sports data providers
  run_launcher "launch-mtds-sports-odds-backfill-vm.sh" "MTDS Sports odds"
  run_launcher "launch-footystats-backfill-vm.sh"       "MTDS Sports footystats fixtures"
  run_launcher "launch-api-football-backfill-vm.sh"     "MTDS Sports API-football"
  run_launcher "launch-understat-backfill-vm.sh"        "MTDS Sports understat (xG)"
  run_launcher "launch-transfermarkt-backfill-vm.sh"    "MTDS Sports transfermarkt"
  run_launcher "launch-openmeteo-backfill-vm.sh"        "MTDS Sports openmeteo weather"
  maybe_wait "mtds-sports"

  # Tier 3: features-sports (depends on MTDS sports)
  run_launcher "launch-features-sports-parallel-backfill-vm.sh" "features-sports (parallel)"
  maybe_wait "features-sports"
}

run_prediction() {
  log "=== Prediction backfill (est 3k MISSING_EXPECTED cells) ==="
  run_launcher "launch-mtds-prediction-backfill-vm.sh" "MTDS prediction (Polymarket/Kalshi)"
  run_launcher "launch-mdps-backfill-vm.sh"            "MDPS prediction market data"
  maybe_wait "mtds-prediction"
}

# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

log "Phase 11 backfill coordinator — ${MODE_LABEL}"
log "  asset_group=${ASSET_GROUP}  env=${DEPLOYMENT_ENV}  project=${PROJECT_ID}  zone=${ZONE}"
if [[ -n "$START_DATE" ]]; then log "  start=${START_DATE}"; fi
if [[ -n "$END_DATE" ]];   then log "  end=${END_DATE}"; fi

if [[ "$APPLY" == "false" ]]; then
  log "  Running in DRY-RUN mode. Pass --phase6-green --phase7a-green --apply to execute."
fi

case "$ASSET_GROUP" in
  defi)       run_defi ;;
  cefi)       run_cefi ;;
  tradfi)     run_tradfi ;;
  sports)     run_sports ;;
  prediction) run_prediction ;;
  all)
    run_defi
    run_cefi
    run_tradfi
    run_sports
    run_prediction
    ;;
esac

log "Phase 11 ${MODE_LABEL} complete."
if [[ "$APPLY" == "false" ]]; then
  log "Re-run with --phase6-green --phase7a-green --apply to execute."
fi
exit 0
