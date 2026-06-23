#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Relaunch-staging artifact — codified 2026-05-29
#
# This script began life as a /tmp/ Phase A kill list on 2026-05-27 (operator-
# discovered fleet rot: 5 zero-output OKX-futures VMs + 3 boot-hung + stalled
# infra burning ~$240/day). The /tmp/ original was lost to reboot; this is the
# durable reconstruction.
#
# State as of 2026-05-29T16:38Z:
#   The original kill list is MOSTLY OBSOLETE — operator (or the
#   vm-zombie-watchdog after slot 2 fixed its pip install / unified-trading-library
#   import on 2026-05-27) has already cleared the bulk of the wasted VMs.
#
#   Current `gcloud compute instances list --project=central-element-323112
#   --filter='status=RUNNING'` shows:
#     - alerting-quietness-20260522-083225        (keep — healthy PubSub subscriber)
#     - mdps-backfill-defi-20260528-071130        (active backfill)
#     - mdps-backfill-tradfi-20260528-{2137xx} × 5 (active TradFi sharded backfill)
#     - mdps-prediction-2025-20260523-124620      (keep — healthy producer)
#     - mtds-lending-indices-20260529-153923      (fresh, today's lending-indices fetch)
#     - mtds-solana-drift-backfill                (active Drift Solana backfill)
#     - sports-scheduler-20260525-072005          (relaunched on slot-2 venv fix)
#     - vm-zombie-watchdog-20260528-212634        (relaunched, post pip-fix)
#
#   None of the original kill-list candidates are currently running.
#
# This script is kept as:
#   1. An operational SSOT for "what was Phase A and why" (audit trail).
#   2. A template for future relaunch-staging when fleet rot is rediscovered.
#   3. The Phase B (Tardis-renewal-gated relaunch) plan is still relevant if/when
#      the operator decides to renew Tardis — see § Phase B below.
#
# Originating sources:
#   - plans/active/cefi_venue_backfill_coverage_remediation_2026_05_27.md §5 (relaunch gate)
#   - plans/active/issues/running_vm_fleet_status_2026_05_27.md (25-VM audit, evidence)
#
# This script is DRY-RUN BY DEFAULT — set EXEC=1 to actually run any action.
#
# Updated coverage context (2026-05-29 stress-test correction):
#   Earlier-session venue-day coverage claim of "98% trades captured" was WRONG at
#   instrument-level granularity. Real instrument-day coverage for Tardis-CeFi venues:
#     - trades:               62-74%  (claimed 98%)
#     - book_snapshot_5:      63.4%   (claimed 83.4%)
#     - derivative_ticker:    41.7%   (claimed 71.0%)
#     - liquidations:         14.8%   (claimed 62.6%)
#     - futures_chain:         7.8%   (claimed 100% — chain-dimension bug)
#     - options_chain:         1.5%   (claimed 100%)
#   The Tardis-renewal decision should be reconsidered against these instrument-level
#   numbers, not the venue-day rollup.

set -euo pipefail
EXEC="${EXEC:-0}"
say() { printf '\n=== %s ===\n' "$*"; }
do_cmd() {
  echo "+ $*"
  if [[ "$EXEC" == "1" ]]; then "$@"; else echo "  [dry-run; set EXEC=1 to run]"; fi
}

PROJECT=central-element-323112
ZONE=asia-northeast1-c

# ===========================================================================
# § Phase A — Historical kill list (status as of 2026-05-29: ALREADY DONE)
# ===========================================================================

say "A1. Verify Tardis key state (informational; gates Phase B)"
K=$(gcloud secrets versions access latest --secret=tardis-api-key --project="$PROJECT" 2>/dev/null || true)
if [[ -n "$K" ]]; then
  info=$(curl -sS --max-time 10 https://api.tardis.dev/v1/api-key-info -H "Authorization: Bearer $K" 2>/dev/null)
  echo "  tardis api-key-info = $info"
  if [[ "$info" == "[]" ]]; then
    echo "  → key plan EXPIRED; Phase B remains gated."
  else
    echo "  → key plan ACTIVE; Phase B is unblocked."
  fi
fi

say "A2-A4. Historical kill list — NO-OP today (none of these are running)"
echo "  The following VMs were the Phase A kill candidates on 2026-05-27:"
echo "    cefi-okx-futures-{2020..2025}-heavy   (5× e2-highmem-16, zero-output)"
echo "    cefi-deribit-2021-heavy               (OOM, 57h unresponsive)"
echo "    cefi-bybit-2024-heavy                 (boot-hung — gsutil wheel-cache deadlock)"
echo "    cefi-hyperliquid-2025-heavy           (boot-hung)"
echo "    cefi-kraken-spot-2024-heavy           (boot-hung)"
echo "    mdps-prediction-2026                  (network-wedged, 21h)"
echo "    mdps-sports-2025                      (stalled, 19h zero output)"
echo "    us-backfill                           (done, GCE stuck)"
echo "    footystats-fwd-20260523-170007        (older of duplicate)"
echo "    qg-snapshot                           (never wrote a log)"
echo ""
echo "  As of 2026-05-29: ALL ABSENT from the running fleet. Phase A is effectively complete."

say "A5. Verify the currently-running fleet is the healthy set (read-only)"
do_cmd gcloud compute instances list --project="$PROJECT" \
  --filter="status=RUNNING" --format="value(name)"

# ===========================================================================
# § Phase B — Tardis-renewal-gated targeted relaunch (still relevant)
# ===========================================================================

if [[ -n "${info:-}" ]] && [[ "$info" == "[]" ]]; then
  say "B-GATE: Tardis subscription expired — Phase B halted."
  echo "  Phase B only runs when api-key-info returns non-empty entitlements."
  exit 0
fi

say "B1. Tardis-renewed: targeted OKX backfill (fills the real concentrated gaps)"
echo "  Per the corrected 2026-05-29 coverage analysis (instrument-day granularity):"
echo "    OKX-FUTURES book_snapshot_5 / derivative_ticker / trades — biggest concentrated gap area"
echo "    OKX-SWAP / OKX-SPOT book_snapshot_5 — secondary"
echo "    DERIBIT liquidations — substantive"
echo "    Most other gaps are <300 days each — judgment call"
echo ""
echo "  ONLY filter narrows the universe so we don't pay for already-have data."
do_cmd env DEPLOYMENT_ENV=prod \
  ONLY="OKX-FUTURES:2020:heavy OKX-FUTURES:2021:heavy OKX-FUTURES:2022:heavy \
        OKX-FUTURES:2023:heavy OKX-FUTURES:2024:heavy OKX-FUTURES:2025:heavy OKX-FUTURES:2026:heavy \
        OKX-SWAP:2020:heavy   OKX-SWAP:2021:heavy   OKX-SWAP:2022:heavy   OKX-SWAP:2023:heavy   OKX-SWAP:2024:heavy   OKX-SWAP:2025:heavy   OKX-SWAP:2026:heavy \
        OKX-SPOT:2020:heavy   OKX-SPOT:2021:heavy   OKX-SPOT:2022:heavy   OKX-SPOT:2023:heavy   OKX-SPOT:2024:heavy   OKX-SPOT:2025:heavy" \
  bash deployment-service/scripts/vm/launch-cefi-sharded-backfill.sh

say "B2. Relaunch previously boot-hung Tardis-gated VMs on slot-2's fixed launcher"
do_cmd env DEPLOYMENT_ENV=prod ONLY="BYBIT:2024:heavy KRAKEN-SPOT:2024:heavy" \
  bash deployment-service/scripts/vm/launch-cefi-sharded-backfill.sh

# ===========================================================================
# What's NOT in this script
# ===========================================================================
#  - The chain dimension bug fix (already shipped 2026-05-28 mtds@2e91d74f).
#  - book_snapshot_5 / derivative_ticker for BINANCE-FUTURES/BYBIT/DERIBIT —
#    only ~200-300d gaps; judgment call against Tardis subscription cost.
#  - Per-instrument-day backfill scoping — sub-agent recommended this as the
#    correct granularity for any future relaunch.
