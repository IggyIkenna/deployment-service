#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch sharded CeFi Tardis backfill VMs. (CeFi-ONLY — the former TradFi block was
# REMOVED 2026-06-23; TradFi rides the canonical Databento launchers, see the wait
# block's NOTE below.)
#
# Operator instrument filter (authoritative, 2026-04-19):
#   CeFi:
#     BTC + ETH  → spot + futures + perps + options (options DERIBIT only)
#     Selected x-coins (SOL XRP BNB DOGE ADA AVAX LINK) → spot + perps + futures
#     (NO options on x-coins — saves Tardis cost)
#     Options ONLY on DERIBIT.
#
# Why this matters: without --instrument-ids, MTDS downloads the FULL symbol
# universe per venue from Tardis. Binance-Futures alone has ~1000+ instruments;
# Binance-Spot >2000. One unfiltered week-year shard ≈ hundreds of $$ in Tardis
# API cost. Per-venue symbol lists below are hand-curated from adapter symbol
# conventions + operator's selected coin list.
#
# Symbol conventions per venue (verified from:
#   unified-api-contracts/unified_api_contracts/registry/venue_mapping.py
#   market-tick-data-service/market_tick_data_service/market_interface/adapters/
# ):
#   BINANCE-SPOT      → lowercase concatenated (btcusdt, ethusdt, solusdt)
#   BINANCE-FUTURES   → uppercase concatenated USDT-M perps (BTCUSDT, ETHUSDT)
#   BYBIT             → uppercase linear perps (BTCUSDT, ETHUSDT)
#   DERIBIT           → base currency prefix; Tardis expands chain globs
#                       server-side for options (BTC, ETH, BTC-PERPETUAL,
#                       ETH-PERPETUAL)
#   COINBASE-SPOT     → dash-separated (BTC-USD, ETH-USD)
#   OKX-SPOT          → dash-separated (BTC-USDT, ETH-USDT)
#   OKX-SWAP          → SWAP suffix (BTC-USDT-SWAP, ETH-USDT-SWAP)
#   OKX-FUTURES       → date-suffixed quarterlies — we skip (too many permutations,
#                       perps are the liquid product)
#   HYPERLIQUID       → perp-only, base symbol (BTC, ETH)
#   UPBIT             → KRW-prefixed (KRW-BTC, KRW-ETH)
#
# (TradFi removed 2026-06-23 — served by the canonical Databento launchers, not here.)
#
# Observability (MUST match):
#   - startup-script-url=gs://deployment-scripts.../vm/setup-data-pipeline-vm.sh
#   - VM_TASK=cefi-backfill → routes through setup-data-pipeline-vm.sh line 424
#     elif branch which assembles --operation/--mode/--asset-group/--venues/... CLI
#   - VM_INSTRUMENT_IDS comma-separated (setup script expands to space-sep
#     --instrument-ids nargs='+' argparse)
#   - heartbeat + watchdog are in the GCS tee wrapper; nothing to add here
#
# Dry-run:  DRY_RUN=1 bash scripts/vm/launch-cefi-sharded-backfill.sh
set -e

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

PROJECT=central-element-323112
# Zone: asia-northeast1-c (matches launch-mdps-backfill-vm.sh,
# launch-canonical-migration-vm.sh, launch-features-backfill-vm.sh).
# Previous -b was a typo; Tokyo -b has fewer e2-standard-4 instances available.
ZONE=asia-northeast1-c
STARTUP=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh

DRY_RUN="${DRY_RUN:-0}"
FORCE="${FORCE:-0}"
MAX_CONCURRENT="${MAX_CONCURRENT:-15}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# SINGLE_VM_QUEUE (tardis_concurrent_ip_lockout_2026_07_12, operator course-correction
# 2026-07-13): opt-in mode that collapses every matched (venue,year,group) shard onto
# ONE combined VM per (group, data_types) bucket instead of one VM per shard. One VM =
# one egress IP, and the Tardis single-concurrent-IP lock is per-IP (not per-connection
# — the 403 body says "already active from ANOTHER IP address"), so a single VM can run
# several concurrent Tardis download streams (bounded by TARDIS_MAX_CONCURRENT_DOWNLOADS,
# shared process-wide across every venue in its --venues list via the pooled
# TardisAdapter singleton + asyncio.gather fan-out in process_ticks) without the
# ~20-80x wall-clock hit of the TARDIS_CONCURRENCY_LEASE cross-VM serialization stopgap.
# Still opt-in TARDIS_CONCURRENCY_LEASE + a control bucket is recommended as a safety
# net even in this mode (harmless no-op cost when only one VM is ever running).
SINGLE_VM_QUEUE="${SINGLE_VM_QUEUE:-0}"
declare -A _QUEUE_VENUES  # key="group|data_types" -> space-separated venue list
declare -A _QUEUE_START   # key -> min matched start_date
declare -A _QUEUE_END     # key -> max matched end_date

# Registry-driven machine-sizing (operator 2026-06-23): a memory-heavy venue
# launches correctly-sized on the FIRST try, NOT via repeated OOM-escalation.
# The per-venue floor lives in deployment_service launch_budget_registry
# (VENUE_TASK_MEMORY_TIER) — e.g. Coinbase cefi → highmem-128gb (256 for the
# heaviest ranges). registry_machine_floor <venue> echoes the registry's GCE
# machine-type for cefi-backfill:<venue>, or empty if unavailable.
_REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
_PY="${_REPO_ROOT}/.venv/bin/python"
[[ -x "$_PY" ]] || _PY="python3"
registry_machine_floor() {
  local venue_lc
  venue_lc="$(echo "$1" | tr '[:upper:]' '[:lower:]')"
  PYTHONPATH="${_REPO_ROOT}" "$_PY" - "$venue_lc" <<'PYEOF' 2>/dev/null || true
import sys
from deployment_service.data_pipeline_monitors.launch_budget_registry import machine_type_for
print(machine_type_for(task="cefi-backfill", venue=sys.argv[1]))
PYEOF
}
# machine_ram_gb <machine-type> echoes the RAM (GiB) for a GCE machine type,
# ladder OR off-ladder (e.g. e2-highmem-16=128); 0 if unknown.
machine_ram_gb() {
  PYTHONPATH="${_REPO_ROOT}" "$_PY" - "$1" <<'PYEOF' 2>/dev/null || echo 0
import sys
from deployment_service.data_pipeline_monitors.launch_budget_registry import gce_machine_ram_gb
print(gce_machine_ram_gb(sys.argv[1]))
PYEOF
}
# TARDIS_KEY_CHECK: set to 0 to skip the key-validity probe (e.g. if Secret Manager
# is inaccessible). Default 1 — always probe so an expired key aborts early.
TARDIS_KEY_CHECK="${TARDIS_KEY_CHECK:-1}"
# FREE_ONLY: set to 1 to launch VMs that only download Tardis free-tier dates
# (1st of every month + last 7 days rolling window). Useful when the paid key
# is expired — VMs skip paid dates via TARDIS_FREE_ONLY=1 metadata rather than
# spinning at 100% CPU on 401 responses. Requires an active (even free-tier) key
# in Secret Manager so the VM can authenticate at all.
FREE_ONLY="${FREE_ONLY:-0}"

# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

# Parse --env (Phase 0f env-tier targeting per bucket-naming SSOT). The legacy
# behavior (no CLI args, env-var overrides only) is preserved — only --env is
# accepted on the command line. Other config still flows via env vars
# (DRY_RUN / FORCE / MACHINE_TYPE_HEAVY / etc.) per the comment block above.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "ERROR: unknown flag '$1' (only --env and --on-demand are supported; other config via env vars)" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ─── Tardis key validity check ────────────────────────────────────────────────
# Probe api-key-info before launching any VMs so an expired key aborts early
# instead of spawning dozens of VMs that spin at 100% CPU on HTTP 401 responses.
# Pattern from relaunch_staged_2026_05_29.sh §A1.
#
# FREE_ONLY=1 overrides the abort: VMs are launched with TARDIS_FREE_ONLY=1 in
# metadata so TickDataHandler skips paid-tier dates. Only free-tier dates
# (1st-of-month + last 7 days rolling) are downloaded — safe even with an expired
# key as long as the key itself is not absent (Tardis still needs a Bearer token).
#
# Bypass: TARDIS_KEY_CHECK=0 bash launch-cefi-sharded-backfill.sh
if [[ "$TARDIS_KEY_CHECK" == "1" && "$DRY_RUN" != "1" ]]; then
  _TARDIS_KEY=$(gcloud secrets versions access latest \
    --secret=tardis-api-key --project="$PROJECT" 2>/dev/null || true)
  if [[ -n "$_TARDIS_KEY" ]]; then
    _TARDIS_INFO=$(curl -sS --max-time 10 https://api.tardis.dev/v1/api-key-info \
      -H "Authorization: Bearer $_TARDIS_KEY" 2>/dev/null || true)
  fi
  _KEY_ACTIVE=0
  if [[ -n "${_TARDIS_KEY:-}" && "${_TARDIS_INFO:-}" != "[]" && -n "${_TARDIS_INFO:-}" ]]; then
    _KEY_ACTIVE=1
  fi
  if [[ "$_KEY_ACTIVE" == "0" ]]; then
    if [[ "$FREE_ONLY" != "1" ]]; then
      cat >&2 <<EOF
ERROR: Tardis API key expired or absent (api-key-info: ${_TARDIS_INFO:-<empty>}).
Paid historical dates will return HTTP 401 — launching VMs would waste CPU + cost.

Options:
  Renew key:    gcloud secrets versions add tardis-api-key --data-file=<keyfile> --project=$PROJECT
  Free-only:    FREE_ONLY=1 bash $0 [--env $DEPLOYMENT_ENV]
                (VMs download only 1st-of-month + last-7-days dates; skip paid dates)
  Bypass check: TARDIS_KEY_CHECK=0 bash $0 [--env $DEPLOYMENT_ENV]
EOF
      exit 1
    fi
    echo "WARNING: Tardis key expired/absent — FREE_ONLY=1, VMs will only download free-tier dates." >&2
  else
    echo "Tardis key active (entitlements confirmed)." >&2
  fi
fi

# ─── Tardis concurrent-VM cap (replaces the pre-2026-07-14 blanket singleton lock) ──
# HARD RULE (operator, 2026-07-14): at most 3 Tardis-consuming VMs at a time, and every
# launcher counts the existing fleet before creating new ones. Empirical basis (SSOT:
# unified-trading-pm/plans/active/issues/tardis_concurrent_ip_lockout_2026_07_12.md):
# the shared key enforces a single REVOLVING active-IP slot — N=3 grinds at ~50-70%
# request efficiency, N=6 starved below the stall watchdog and ALL SIX died
# (2026-07-13T23:15Z wave). The old any-duplicate refusal is superseded: up to 3 total
# is now allowed. The historical context still applies (shared account + NAT; the
# 2026-05-05 Databento silent-drop incident was the same over-concurrency shape).
# The guard itself runs just before the launch loop, where the planned shard count
# (venue × year × heavy/light fan-out) is computable — see tardis_concurrency_guard.
# The lease does NOT lift the cap (every surviving multi-VM wave ran lease-ON; the only
# lease-OFF multi-VM wave collapsed) — enable TARDIS_CONCURRENCY_LEASE=1 for any multi-VM
# launch. Only FORCE=1 overrides the cap.
# shellcheck source=./tardis-concurrency-guard.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"

# ─── Per-venue instrument universe = catalogue mvp (cefi_universe_capture_rule) ──
# 2026-06-23: the hardcoded 9-coin SYMBOLS_<VENUE> lists (+ stale Upbit KRW) are
# RETIRED. CeFi shards launch with NO VM_INSTRUMENT_IDS so MTDS
# TardisAdapter._resolve_symbols resolves the per-venue universe from the IS
# by_date instruments snapshot, MVP-GATED via the shared UAC predicate
# `is_in_mvp_capture_universe` (the SAME SSOT as cefi_catalog_reader + the
# onchain-perp ALL handler + the manifest mvp-denominator). So every venue
# attempts its FULL perp-gated MVP capture universe per day (hundreds of
# instruments — small-coin funding included), not a static 9. Cost is bounded by
# the mvp gate (perp-gated subset), not the full Tardis exchange universe.
# DERIBIT options/futures still ride the bulk OPTIONS.csv.gz / FUTURES.csv.gz
# path (chain glob), unaffected. To restrict a surgical re-run to specific
# symbols, set VM_INSTRUMENT_IDS in the env (passes through verbatim).

# ─── Per-venue data-type splits ──────────────────────────────────────────────
# Heavy = order flow (trades + book_snapshot_5). Memory/CPU bound.
# Light = periodic/derived (derivative_ticker, liquidations, chains).
# Options venue (DERIBIT) adds options_chain to light.
DATA_HEAVY="trades;book_snapshot_5"
DATA_LIGHT_PERPS="derivative_ticker;liquidations;futures_chain"
DATA_LIGHT_DERIBIT="derivative_ticker;options_chain;futures_chain"

# ─── Launcher ────────────────────────────────────────────────────────────────
_launched=0
_concurrent=0

_stagger() {
  # Stagger VM creation by 3s so RUN_TS timestamps don't collide across shards
  # (canonical-migration had this bug 2026-04-18 — several VMs shared RUN_TS
  # and clobbered each other's GCS progress files).
  sleep 3
}

_batch_guard() {
  _concurrent=$((_concurrent + 1))
  if (( _concurrent >= MAX_CONCURRENT )); then
    echo "  ... reached MAX_CONCURRENT=$MAX_CONCURRENT, waiting 10s before next batch"
    sleep 10
    _concurrent=0
  fi
}

launch_cefi_shard() {
  local venue="$1"         # canonical (BINANCE-FUTURES, DERIBIT, etc.)
  local year="$2"
  local group="$3"         # heavy | light
  local data_types="$4"
  # NOTE: no $5 symbols arg — CeFi universe is catalogue-mvp-driven (no
  # VM_INSTRUMENT_IDS). VM_INSTRUMENT_IDS env-override is honoured for surgical
  # re-runs (passed verbatim to --instrument-ids → bypasses the mvp GCS path).
  local symbols="${VM_INSTRUMENT_IDS:-}"

  local start_date end_date machine
  if [[ "$year" == "2026" ]]; then
    start_date="${year}-01-01"
    # Dynamic "yesterday" cutoff (was a hardcoded 2026-05-22 that went stale
    # 52+ days ago, permanently blocking any 2026 launch for a venue whose
    # entire coverage window postdates the hardcode — found 2026-07-13 via
    # COINBASE-CDE, launched 2026-07-10, cefi G4 re-verification).
    end_date="$(date -u -d yesterday +%Y-%m-%d)"
  else
    start_date="${year}-01-01"
    end_date="${year}-12-31"
  fi

  # Machine types — heavy groups (trades + book_snapshot_5 for 9 symbols).
  # 2026-04-22 P2.B + P2.B follow-up: after killing ``small_frames`` accumulation
  # in TardisAdapter.download_batch (MTDS 5ec195f) + removing the legacy
  # PartitionedTickWriter dual-write path (same commit) + pre-flight
  # granularity fix (MTDS ab6338b) that filters to only missing data_types
  # per venue/date, heavy-profile peak RSS dropped from >8 GB (OS-OOM on
  # e2-standard-2) to ~500 MB on the same smoke (cefi-smoke-p2b-20260422,
  # BINANCE-FUTURES 2026-04-18 BTCUSDT+ETHUSDT trades+book_snapshot_5 =
  # 8.66M records, ran to completion rc=0). Downgraded heavy profile from
  # e2-highmem-4 (32 GB, $0.27/hr) to e2-standard-2 (8 GB, $0.07/hr) — a
  # 4x cost reduction on 95 VMs × expected 24-48 h runtime.
  #
  # 2026-05-06: bumped heavy default from e2-standard-2 (8 GB) to
  # e2-highmem-2 (16 GB) post-streaming-finalize ship (MTDS f07f3f9 default-on).
  # AVAX-USD smoke peaked at 1.04 GB; BTC-USD heavy days scale ~10-20×.
  # 2026-05-22: bumped heavy from e2-highmem-2 (16 GB) to e2-highmem-4 (32 GB)
  # after systematic OOM failures across BINANCE-FUTURES/SPOT/BYBIT 2021+;
  # 2021 bull-market days scale ~20-30× from AVAX baseline → 20-30 GB peaks.
  # Light bumped from e2-standard-2 (8 GB) to e2-highmem-2 (16 GB) after
  # BYBIT-2021-light OOM on 8 GB. DERIBIT light (options_chain) uses e2-highmem-4
  # (32 GB) — options_chain 2026 data is multi-GB per day.
  # 2026-05-23: bumped heavy default from e2-highmem-4 (32 GB) to e2-highmem-8
  # (64 GB). shard_memory_profile.py recommends e2-highmem-8 for any DERIBIT /
  # BINANCE-FUTURES / BYBIT / OKX-SWAP heavy book_snapshot_5 shards (38 GB peak
  # observed on DERIBIT BTC-PERPETUAL + BTC-options_chain expiry days). 2024
  # bull-market BINANCE-SPOT data is also 5-10× larger than the 2022-calibrated
  # 18 MB profile, causing constant 30s memory-pressure pauses on 32 GB that
  # double backfill runtime. e2-highmem-8 ($0.54/hr) at 64 GB eliminates pauses;
  # the 2× cost is dominated by VM count × runtime reduction.
  # 2026-05-24: bumped light default from e2-highmem-2 (16 GB) to e2-highmem-4
  # (32 GB) after OOM failures on BINANCE-FUTURES/OKX-SWAP light VMs. Root cause:
  # CeFi consolidated manifest (36 MB compressed) expands to ~12-13 GB in pandas
  # memory during read_availability_index at startup — 1682 per-VM shards causing
  # large merged DataFrame. e2-highmem-2 (16 GB) runs out of RAM during manifest
  # load; e2-highmem-4 (32 GB) gives 2x headroom. DERIBIT already on e2-highmem-4.
  # 2026-05-24 (second bump): light bumped from e2-highmem-4 (32 GB) to
  # e2-highmem-8 (64 GB). Root cause: day-boundary OOM. After completing day N
  # (peak RSS 23.7 GB on 32 GB) Python's GC hasn't freed day-N data before
  # day N+1 allocates its streaming buffer (confirmed: BINANCE-FUTURES 2024-01-01
  # → 01-02 OOM kill, rc=137). 23.7 GB baseline + day-2 peak > 32 GB; 64 GB
  # eliminates the constraint. Override: MACHINE_TYPE_HEAVY / MACHINE_TYPE_LIGHT.
  # 2026-05-25: bumped heavy default from e2-highmem-8 (64 GB) to e2-highmem-16
  # (128 GB). Root cause: 2022-2023 bull-market Tardis streaming peaks exceed 64 GB
  # (confirmed: BINANCE-FUTURES-2023 peak_rss=60.3 GB, DERIBIT-2022 peak_rss=65.7 GB
  # both OOM'd rc=137 on e2-highmem-8). 9 concurrent book_snapshot_5 streams for
  # high-volume years saturate 64 GB; e2-highmem-16 ($1.08/hr) at 128 GB provides
  # 2× headroom. Lower-volume years (BYBIT-2021 peaked at 24 GB) benefit from
  # the extra CPUs (16 vs 8) even if RAM is not needed.
  if [[ "$group" == "heavy" ]]; then
    machine="${MACHINE_TYPE_HEAVY:-e2-highmem-16}"
  else
    machine="${MACHINE_TYPE_LIGHT:-e2-highmem-8}"
  fi
  # Registry-driven floor (operator 2026-06-23): if launch_budget_registry has a
  # per-venue tier LARGER than the group default (e.g. a venue registered at
  # highmem-256gb), start there — correctly-sized on the FIRST try, not via
  # repeated OOM-escalation. An explicit MACHINE_TYPE_HEAVY/_LIGHT override still
  # wins (operator intent), so only raise when no override was given.
  if { [[ "$group" == "heavy" && -z "${MACHINE_TYPE_HEAVY:-}" ]]; } ||
     { [[ "$group" != "heavy" && -z "${MACHINE_TYPE_LIGHT:-}" ]]; }; then
    local registry_machine
    registry_machine="$(registry_machine_floor "$venue")"
    if [[ -n "$registry_machine" && "$registry_machine" != "$machine" ]]; then
      local cur_ram reg_ram
      cur_ram="$(machine_ram_gb "$machine")"
      reg_ram="$(machine_ram_gb "$registry_machine")"
      if [[ "$reg_ram" -gt "$cur_ram" ]]; then
        echo "  [machine-sizing] $venue: registry floor ${registry_machine} (${reg_ram}GB) > default ${machine} (${cur_ram}GB) → using ${registry_machine}"
        machine="$registry_machine"
      fi
    fi
  fi

  # ONLY="venue1:year1:group1 venue2:year2:group2 ..." filters to specific
  # combos. Used by relaunch-OOM workflow to retry only the dead heavy VMs
  # at higher memory, e.g.:
  #   ONLY="BINANCE-SPOT:2026:heavy DERIBIT:2026:heavy" \
  #     MACHINE_TYPE_HEAVY=e2-highmem-8 FORCE=1 \
  #     bash launch-cefi-sharded-backfill.sh
  if [[ -n "${ONLY:-}" ]]; then
    local key="${venue}:${year}:${group}"
    local matched=0
    for tok in $ONLY; do
      [[ "$tok" == "$key" ]] && matched=1 && break
    done
    [[ $matched -eq 0 ]] && return 0
  fi

  local venue_lower
  venue_lower=$(echo "$venue" | tr '[:upper:]' '[:lower:]')
  # RUN_TS suffix per workspace VM naming convention — sortable, unique across
  # re-launches so the orchestrator can write to its own per-VM manifest shard.
  local vm_name="cefi-${venue_lower}-${year}-${group}-${RUN_TS}"

  local meta="startup-script-url=$STARTUP"
  meta+="|VM_TASK=cefi-backfill"
  meta+="|VM_SERVICE=market_tick_data_service"
  meta+="|VM_OPERATION=download"
  meta+="|VM_ASSET_GROUP=CEFI"
  meta+="|VM_VENUE=$venue"
  meta+="|VM_START_DATE=$start_date"
  meta+="|VM_END_DATE=$end_date"
  meta+="|VM_DATA_TYPES=$data_types"
  # No VM_INSTRUMENT_IDS by default → MTDS resolves the catalogue-mvp universe
  # from the IS by_date snapshot (perp-gated). Only stamp it on a surgical
  # VM_INSTRUMENT_IDS env-override re-run.
  [[ -n "$symbols" ]] && meta+="|VM_INSTRUMENT_IDS=$symbols"
  meta+="|VM_FORCE=${VM_FORCE:-false}"
  meta+="|DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # 2026-05-01: opt-in auto-delete after task completion (read by
  # vm-exec-with-gcs-tee.sh:253). Without this, one-shot backfill VMs sat
  # RUNNING idle after rc!=0 (or even rc==0) until manually killed — cost leak.
  meta+="|VM_SHUTDOWN_ON_COMPLETION=true"
  # CeFi bucket has 34M+ rows (mostly Deribit options) + 1700+ per-VM shards.
  # Without this override ManifestReader falls back to loading all shards when
  # the consolidated availability_index.parquet is >120s old (the default),
  # causing the Python process to OOM-kill at startup (rc=137) on every machine
  # size tested (up to e2-highmem-8 / 64 GB). 86400s (24h) forces the reader to
  # always use the consolidated index instead of the per-VM shard fallback.
  meta+="|MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
  # Opt-in fail-fast (2026-05-28): when the staleness budget above IS exceeded,
  # UTL read_availability_index raises ManifestConsolidatorStaleError instead of
  # OOM-killing at the per-VM shard merge. Pairs with the shell preflight in
  # setup-data-pipeline-vm.sh (deployment-service@7add531) — preflight catches
  # before Python starts; this catches anything that slips through (e.g. the
  # consolidator goes stale mid-run, not at bootstrap).
  meta+="|MANIFEST_FAIL_ON_STALE_FALLBACK=true"
  # STALL_PROGRESS_REGEX (cefi_bf_2021_heavy_vm_stalled_2026_07_12 root-cause):
  # without this, vm-exec-with-gcs-tee.sh's stall watchdog falls back to raw
  # LOCAL_LOG byte-growth detection — which the setup-data-pipeline-vm.sh
  # vm-life-emitter (a `while true; echo PIPELINE_HEARTBEAT; sleep 60` loop
  # wired into the SAME tee'd command, BUG1b 2026-06-22) defeats forever: its
  # own 60s heartbeat lines keep the log "growing" even when the real Tardis
  # worker is fully hung, so the 30-min stall timer never fires. Confirmed via
  # gs://deployment-scripts-.../vm-logs/cefi-binance-futures-2021-heavy-20260703-105623/run.log:
  # BINANCE-FUTURES/2021-07-01/book_snapshot_5 streaming produced its last
  # "StreamingParquetWriter: uploaded ..." line at 2026-07-04T15:14:33Z, then
  # ONLY PIPELINE_HEARTBEAT noise for the next 7+ days until manual termination
  # 2026-07-12 — no traceback, no OOM, no next-symbol request line. "uploaded"
  # is emitted by UTL StreamingParquetWriter/StreamingShardFinalizer on every
  # per-shard GCS finalize, used by BOTH the heavy per-symbol streaming path
  # (tardis_cefi_shards.py) and the light/DERIBIT bulk chain-glob path
  # (tardis_bulk_download.py) — a safe universal per-shard progress marker
  # across every venue/group this launcher spawns. Same pattern as
  # launch-mdps-sharded-backfill.sh / launch-sfi-backfill-vm.sh /
  # launch-mtds-gas-fees-backfill-vm.sh. =/space/comma-free (metadata-safe).
  meta+="|STALL_PROGRESS_REGEX=uploaded"
  # FREE_ONLY=1 → pass TARDIS_FREE_ONLY=1 so TickDataHandler skips paid dates.
  [[ "$FREE_ONLY" == "1" ]] && meta+="|TARDIS_FREE_ONLY=1"
  # Tardis single-concurrent-IP lease (option (a) stopgap, DEFAULT-OFF —
  # tardis_concurrent_ip_lockout_2026_07_12). Opt-in ONLY: set
  # TARDIS_CONCURRENCY_LEASE=1 + TARDIS_CONCURRENCY_LEASE_BUCKET=<control-bucket>
  # on the launcher to serialise every Tardis-calling VM through one GCS TTL lease
  # (fixes the concurrent-IP 403 lockout at the cost of ~20-80x slower waves). Left
  # unstamped by default so MTDS's default-False flag keeps waves fully parallel.
  # The bucket MUST be a coordination bucket, NOT a data/manifest-walked bucket.
  [[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]] && meta+="|TARDIS_CONCURRENCY_LEASE=1"
  [[ -n "${TARDIS_CONCURRENCY_LEASE_BUCKET:-}" ]] && meta+="|TARDIS_CONCURRENCY_LEASE_BUCKET=${TARDIS_CONCURRENCY_LEASE_BUCKET}"
  # tardis_concurrent_ip_lockout_2026_07_12 course-correction: operator-tunable
  # intra-process Tardis download concurrency (default 16 when unset — see
  # MarketTickDataServiceConfig.tardis_max_concurrent_downloads). Conservative for a
  # first smoke wave (e.g. 4-8), stepped up once the 403 rate is confirmed clean.
  [[ -n "${TARDIS_MAX_CONCURRENT_DOWNLOADS:-}" ]] && meta+="|TARDIS_MAX_CONCURRENT_DOWNLOADS=${TARDIS_MAX_CONCURRENT_DOWNLOADS}"

  # SINGLE_VM_QUEUE: accumulate this matched shard into its (group,data_types) bucket
  # instead of launching a per-shard VM now — _flush_single_vm_queue launches ONE
  # combined VM per bucket after the whole venue/year loop below has run.
  if [[ "${SINGLE_VM_QUEUE:-0}" == "1" ]]; then
    local qkey="${group}|${data_types}"
    if [[ -z "${_QUEUE_VENUES[$qkey]:-}" ]]; then
      _QUEUE_VENUES[$qkey]="$venue"
      _QUEUE_START[$qkey]="$start_date"
      _QUEUE_END[$qkey]="$end_date"
    else
      # A single venue matches this bucket once per (year,group) shard — e.g.
      # BITGET-FUTURES 2024/2025/2026 all land in the same heavy|trades;book_snapshot_5
      # bucket. Only widen start/end; do NOT re-append an already-queued venue, or
      # --venues ends up space-repeated ("BITGET-FUTURES BITGET-FUTURES BITGET-FUTURES")
      # — a real bug (live-verified 2026-07-14): the repeated venue arg broke MTDS's
      # active-venue resolution entirely ("No active venues" for every date, 0 rows).
      case " ${_QUEUE_VENUES[$qkey]} " in
        *" $venue "*) ;; # already queued — dedup, no-op
        *) _QUEUE_VENUES[$qkey]="${_QUEUE_VENUES[$qkey]} $venue" ;;
      esac
      [[ "$start_date" < "${_QUEUE_START[$qkey]}" ]] && _QUEUE_START[$qkey]="$start_date"
      [[ "$end_date" > "${_QUEUE_END[$qkey]}" ]] && _QUEUE_END[$qkey]="$end_date"
    fi
    echo "[queue] $venue $year $group -> bucket '$qkey' (venues so far: ${_QUEUE_VENUES[$qkey]})"
    _launched=$((_launched + 1))
    return 0
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] $vm_name  venue=$venue year=$year group=$group"
    echo "          data_types=$data_types"
    echo "          symbols=${symbols:-<catalogue-mvp universe (no VM_INSTRUMENT_IDS)>}"
    echo "          metadata=$meta"
  else
    echo "Launching $vm_name ($venue $year $group)"
    # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
    local prov_flags="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
    if [[ "${ON_DEMAND:-false}" == "true" ]]; then prov_flags=""; fi
    # Metadata passed as single --metadata arg to avoid shell interpretation
    # of ';' inside VM_DATA_TYPES / VM_INSTRUMENT_IDS values.
    # shellcheck disable=SC2086
    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "deployment-scripts-${PROJECT}" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    if [[ "${DRY_RUN:-false}" != "true" ]]; then
        lc_verify_tarball_freshness "deployment-scripts-${PROJECT}" \
            market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
            || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
    fi

    # Real bug (live-verified 2026-07-14): gcloud's default --metadata
    # delimiter is comma, same as the intra-value separator VM_INSTRUMENT_IDS
    # needs ("BTCUSDH25,BTCUSDH26,..."). With the default comma delimiter,
    # gcloud misparses each symbol after the first as its own bare (no "=")
    # metadata key -> rejects the whole flag -> prints usage help instead of
    # creating the VM, and (because the create is backgrounded with `&` and
    # only `| tail -1` is captured) this failure is SILENT: the launcher still
    # reports "All N VMs launched" with zero VMs actually created. meta is now
    # built with "|" as the pair-separator (see meta+="|..." above) so a
    # comma-bearing value like VM_INSTRUMENT_IDS survives intact; the "^|^"
    # prefix tells gcloud to use "|" as the delimiter instead of ",".
    gcloud compute instances create "$vm_name" \
      --zone="$ZONE" --machine-type="$machine" \
      ${prov_flags} \
      --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
      --boot-disk-size=50GB \
      --scopes=cloud-platform --metadata="^|^${meta}" \
      --labels=env="${DEPLOYMENT_ENV}" \
      --project="$PROJECT" --async 2>&1 | tail -1 &
    _stagger
    _batch_guard
  fi
  _launched=$((_launched + 1))
}

# ─── CeFi sharding — year-per-shard × heavy/light group ──────────────────────
# Universe is catalogue-mvp-driven (no symbols arg — see the universe block
# above; MTDS resolves the perp-gated MVP universe from the IS by_date snapshot).
# Year ranges = per-venue genesis → 2026 (per Tardis availableSince).
# HYPERLIQUID + ASTER are covered by the dedicated onchain-perp launcher
# (launch-cefi-hl-aster-historical-backfill.sh) — NOT launched here.
#
# Scope overrides (smoke / first-wave):
#   VENUES="BINANCE-FUTURES" YEARS="2024" bash launch-cefi-sharded-backfill.sh
# VENUES (default = all Tardis CEX venues) selects the venue set; YEARS clamps
# every venue's loop to the listed years.
VENUES="${VENUES:-BINANCE-FUTURES BINANCE-SPOT BYBIT BYBIT-SPOT DERIBIT COINBASE-SPOT COINBASE-FUTURES OKX-SPOT OKX-SWAP OKX-FUTURES KRAKEN-SPOT KRAKEN-FUTURES BITFINEX-SPOT BITFINEX-FUTURES BITGET-SPOT BITGET-FUTURES UPBIT}"
YEARS_OVERRIDE="${YEARS:-}"

# Per-venue genesis years (first full year with Tardis data).
_venue_years() {
  case "$1" in
    BINANCE-FUTURES|BINANCE-SPOT|DERIBIT|COINBASE-SPOT) echo "2020 2021 2022 2023 2024 2025 2026" ;;
    BITGET-SPOT|BITGET-FUTURES)                          echo "2023 2024 2025 2026" ;;
    UPBIT)                                               echo "2022 2023 2024 2025 2026" ;;
    *)                                                   echo "2021 2022 2023 2024 2025 2026" ;;
  esac
}

# Derivatives venues carry the light (derivative_ticker/liquidations) group too.
_venue_is_derivatives() {
  case "$1" in
    BINANCE-FUTURES|BYBIT|DERIBIT|OKX-SWAP|OKX-FUTURES|KRAKEN-FUTURES|BITFINEX-FUTURES|BITGET-FUTURES) return 0 ;;
    *) return 1 ;;
  esac
}

# ─── Tardis 3-VM cap check (see the guard comment block above; lib sourced there) ──
# Planned count mirrors the launch loop below exactly: heavy per (venue, year) + light
# for derivatives venues. SINGLE_VM_QUEUE=1 collapses the fan-out into at most one VM
# per (group|data_types) bucket, so cap the estimate at 2 (heavy+light) in that mode.
if [[ "$DRY_RUN" != "1" ]]; then
  PLANNED_TARDIS_VMS=0
  for venue in $VENUES; do
    years="${YEARS_OVERRIDE:-$(_venue_years "$venue")}"
    for year in $years; do
      PLANNED_TARDIS_VMS=$((PLANNED_TARDIS_VMS + 1))
      if [[ "$venue" == "DERIBIT" ]] || _venue_is_derivatives "$venue"; then
        PLANNED_TARDIS_VMS=$((PLANNED_TARDIS_VMS + 1))
      fi
    done
  done
  if [[ "${SINGLE_VM_QUEUE:-0}" == "1" && "$PLANNED_TARDIS_VMS" -gt 2 ]]; then
    PLANNED_TARDIS_VMS=2
  fi
  tardis_concurrency_guard "$PLANNED_TARDIS_VMS" "$ZONE" "$PROJECT" || exit 1
fi

for venue in $VENUES; do
  years="${YEARS_OVERRIDE:-$(_venue_years "$venue")}"
  for year in $years; do
    launch_cefi_shard "$venue" "$year" "heavy" "$DATA_HEAVY"
    if [[ "$venue" == "DERIBIT" ]]; then
      launch_cefi_shard "$venue" "$year" "light" "$DATA_LIGHT_DERIBIT"
    elif _venue_is_derivatives "$venue"; then
      launch_cefi_shard "$venue" "$year" "light" "$DATA_LIGHT_PERPS"
    fi
  done
done

# ─── SINGLE_VM_QUEUE flush — launch ONE combined VM per (group,data_types) bucket ──
# See SINGLE_VM_QUEUE declaration above. No-op (empty _QUEUE_VENUES) unless
# SINGLE_VM_QUEUE=1, in which case launch_cefi_shard accumulated matches above instead
# of launching per-shard.
_launch_queued_vm() {
  local qkey="$1" venues_str="$2" start_date="$3" end_date="$4"
  local group="${qkey%%|*}" data_types="${qkey#*|}"
  local machine
  if [[ "$group" == "heavy" ]]; then
    machine="${MACHINE_TYPE_HEAVY:-e2-highmem-16}"
  else
    machine="${MACHINE_TYPE_LIGHT:-e2-highmem-8}"
  fi
  local vm_name="cefi-queue-${group}-${RUN_TS}"

  local meta="startup-script-url=$STARTUP"
  meta+=",VM_TASK=cefi-backfill"
  meta+=",VM_SERVICE=market_tick_data_service"
  meta+=",VM_OPERATION=download"
  meta+=",VM_ASSET_GROUP=CEFI"
  meta+=",VM_VENUE=$venues_str"
  meta+=",VM_START_DATE=$start_date"
  meta+=",VM_END_DATE=$end_date"
  meta+=",VM_DATA_TYPES=$data_types"
  meta+=",VM_FORCE=${VM_FORCE:-false}"
  meta+=",DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  meta+=",VM_SHUTDOWN_ON_COMPLETION=true"
  meta+=",MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
  meta+=",MANIFEST_FAIL_ON_STALE_FALLBACK=true"
  meta+=",STALL_PROGRESS_REGEX=uploaded"
  [[ "$FREE_ONLY" == "1" ]] && meta+=",TARDIS_FREE_ONLY=1"
  [[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]] && meta+=",TARDIS_CONCURRENCY_LEASE=1"
  [[ -n "${TARDIS_CONCURRENCY_LEASE_BUCKET:-}" ]] && meta+=",TARDIS_CONCURRENCY_LEASE_BUCKET=${TARDIS_CONCURRENCY_LEASE_BUCKET}"
  [[ -n "${TARDIS_MAX_CONCURRENT_DOWNLOADS:-}" ]] && meta+=",TARDIS_MAX_CONCURRENT_DOWNLOADS=${TARDIS_MAX_CONCURRENT_DOWNLOADS}"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] $vm_name  bucket=$qkey machine=$machine"
    echo "          venues=$venues_str"
    echo "          start=$start_date end=$end_date data_types=$data_types"
    echo "          metadata=$meta"
    return 0
  fi

  echo "Launching QUEUE VM $vm_name (venues: $venues_str; $start_date..$end_date; $data_types)"
  local prov_flags="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
  if [[ "${ON_DEMAND:-false}" == "true" ]]; then prov_flags=""; fi
  # shellcheck disable=SC2086
  gcloud compute instances create "$vm_name" \
    --zone="$ZONE" --machine-type="$machine" \
    ${prov_flags} \
    --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform --metadata="$meta" \
    --labels=env="${DEPLOYMENT_ENV}" \
    --project="$PROJECT" --async 2>&1 | tail -1
}

if [[ "${SINGLE_VM_QUEUE:-0}" == "1" ]]; then
  echo ""
  echo "=== SINGLE_VM_QUEUE: flushing ${#_QUEUE_VENUES[@]} bucket(s) into combined VM(s) ==="
  for qkey in "${!_QUEUE_VENUES[@]}"; do
    _launch_queued_vm "$qkey" "${_QUEUE_VENUES[$qkey]}" "${_QUEUE_START[$qkey]}" "${_QUEUE_END[$qkey]}"
  done
fi

# NOTE (2026-06-23): the former TradFi block (CME ES + CBOE VIX via Tardis venues
# CME-FUTURES / CBOE-VIX-FUTURES / CME-OPTIONS / CBOE-VIX-OPTIONS) was REMOVED.
# Those venue tags are NOT canonical TRADFI venues — UAC VENUES_BY_ASSET_GROUP["tradfi"]
# is {NASDAQ, NYSE, CME, ICE, CBOE} — so MTDS's _build_active_venues_for_date()
# intersected the canonical set against the non-canonical --venues filter → empty →
# "No active venues for TRADFI" every date → 0 rows captured at exit_code=0, and the
# VMs self-deleted (VM_SHUTDOWN_ON_COMPLETION). TradFi market data is served by the
# canonical Databento launchers (launch-tradfi-bf-*-ohlcv-*.sh, VM_TASK=mtds-backfill,
# venue=CME/CBOE, --source databento) + launch-tradfi-backfill-vm.sh — NOT this CeFi
# Tardis launcher. This script is now CeFi-only.

wait
if [[ "$DRY_RUN" == "1" ]]; then
  echo ""
  echo "=========================================="
  echo "DRY-RUN: $_launched VMs would be launched"
  echo "=========================================="
else
  echo "All $_launched VMs launched"
fi
