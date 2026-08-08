#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that backfills sports FIXTURES from API-Football. Same code
# path as the live adapter + Cloud Run T+1 recon — just dispatched to a VM so
# multi-year runs don't block a laptop.
#
# Two invocation shapes:
#
#   1. Rolling forward-poll (preferred for FIXTURES schedule-polling):
#        bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7
#        bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
#
#      Resolves to --lookback-days / --lookahead-days / --force-window on the VM,
#      so the instruments-service CLI computes [today-N, today+M] at VM boot
#      time (UTC). This matches the rolling-window contract in
#      codex/02-data/sports-scheduling-and-sharding.md §4 and avoids stale
#      start/end dates frozen at launcher-invocation time.
#
#   2. Explicit historical range (use for pre-deployment backfill):
#        bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15
#        bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31
#
# Writes to gs://instruments-store-sports-prd-central-element-323112/
#   sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet
# per day in the resolved window (inclusive).
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   # Rolling mode:
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider API_FOOTBALL \
#     --lookback-days $VM_LOOKBACK_DAYS --lookahead-days $VM_LOOKAHEAD_DAYS \
#     [--force-window]
#
#   # Explicit-date mode:
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider API_FOOTBALL \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - api-football-api-key in Secret Manager
#
# Usage:
#   bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15                          # historical backfill
#   bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31        # explicit forward range
#   bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7                     # rolling forward-poll
#   bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
#   bash launch-api-football-backfill-vm.sh --force <start> <end>                          # bypass singleton lock
#
# --entity FIXTURES | INJURIES | FIXTURE_STATS | FIXTURE_EVENTS | FIXTURE_LINEUPS | PLAYER_STATS
#   Restricts the VM to a single manifest entity. Use FIXTURES for forward-poll
#   (API-Football publishes schedules weeks/months ahead; per-fixture
#   enrichments only exist post-match).
#
# Cost: e2-standard-2 for ~5-30 min depending on range size. API-Football
# fixtures-by-date returns all leagues in one call per date, so the wall clock
# is dominated by rate-limit pacing (one call per date in the range).
#
# Singleton lock: refuses to launch if any af-backfill-* OR af-audit-* VM is
# already running in the zone. API-Football shares one API key across all VMs
# and enforces per-key rate limits — multiple concurrent VMs produce 429s
# without useful throughput. Reference incident: 2026-04-19 SFI thundering
# herd (10 VMs / 6h / ~4 useful writes), same shape. Pass --force only when
# you have a genuinely disjoint reason (e.g. testing on a different date
# window while an unrelated run is live).
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
ENTITY=""
LOOKBACK=""
LOOKAHEAD=""
FORCE_WINDOW=false
RECOVERY_FIXTURE_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
# Registry-driven rate-budget (operator 2026-06-23): N = how many VMs will run
# concurrently against the SHARED api_football source quota (Custom plan =
# 1200 req/min AND 450,000 req/DAY resetting 00:00 UTC — ONE quota across ALL
# endpoints). The launcher splits the EFFECTIVE ceiling/N → per-VM req/min +
# matched concurrency, stamps them into VM metadata, and the adapter runs its
# self-enforced throttle at that rate. The effective ceiling is daily-aware:
# when REMAINING_DAILY_QUOTA is exported it = min(1200, remaining / minutes to
# 00:00 UTC), so a fleet launched late in the day self-throttles below 1200.
# Default 1 (this is the only VM). The singleton lock below already prevents
# accidental over-subscription, but set --fleet-vms N when you DELIBERATELY fan
# out N api-football VMs (--force). REMAINING_DAILY_QUOTA is optional (omit ⇒ the
# per-minute cap is the only constraint, fine when the day is fresh).
FLEET_VMS="${FLEET_VMS:-1}"
# Tracks whether the operator SET the divisor, vs it being the default. When unset we
# DERIVE it from the measured running fleet (below) instead of assuming this VM is alone.
FLEET_VMS_EXPLICIT=false
REMAINING_DAILY_QUOTA="${REMAINING_DAILY_QUOTA:-}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

# --skip-lock: bypass ONLY the singleton lock (deliberate entity-sharded
# fleet fan-out) WITHOUT setting VM_FORCE on the instrument CLI. --force
# conflated the two (lock bypass + redo_all): the 2026-07-14 GW re-run wave
# launched VMs 2-5 with --force just to clear the lock and inadvertently ran
# them redo_all (presence-skip bypassed → full re-fetch of already-captured
# fixtures). Over a 91-day window that wasted ~30k calls; over 2020→present
# it would burn the entire multi-day quota re-fetching present data. Issue:
# sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14.
SKIP_LOCK=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --skip-lock) SKIP_LOCK=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --force-window) FORCE_WINDOW=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    --fleet-vms) FLEET_VMS="$2"; FLEET_VMS_EXPLICIT=true; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) break ;;
  esac
done

if ! [[ "$FLEET_VMS" =~ ^[0-9]+$ ]] || [[ "$FLEET_VMS" -lt 1 ]]; then
  echo "ERROR: --fleet-vms must be a positive integer (got: $FLEET_VMS)" >&2
  exit 1
fi

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
# "Close the SPOT-preemption relaunch gap"): RelaunchPreemptedVm re-invokes this
# launcher with ZERO CLI args, only the env lc_write_launch_params captured below
# — without this fallback a bare re-invocation hits the positional-arg usage
# error (explicit mode) or silently no-ops (rolling mode never set), so the
# preempted VM's window/entity is lost instead of resumed. Only applies when NO
# CLI args were given at all (an explicit invocation always wins).
if [[ $# -eq 0 && -z "$LOOKBACK" && -z "$LOOKAHEAD" ]]; then
  if [[ -n "${RESUME_START_DATE:-}" && -n "${RESUME_END_DATE:-}" ]]; then
    set -- "$RESUME_START_DATE" "$RESUME_END_DATE"
  elif [[ -n "${RESUME_LOOKBACK:-}" || -n "${RESUME_LOOKAHEAD:-}" ]]; then
    LOOKBACK="${RESUME_LOOKBACK:-0}"
    LOOKAHEAD="${RESUME_LOOKAHEAD:-0}"
    [[ "${RESUME_FORCE_WINDOW:-false}" == "true" ]] && FORCE_WINDOW=true
  fi
  [[ -n "${RESUME_ENTITY:-}" ]] && ENTITY="${RESUME_ENTITY}"
fi

# Mode resolution: rolling (--lookback/--lookahead) vs explicit (<start> <end>).
USE_ROLLING=false
if [[ -n "$LOOKBACK" || -n "$LOOKAHEAD" ]]; then
  USE_ROLLING=true
fi

if $USE_ROLLING; then
  if [[ $# -ne 0 ]]; then
    cat >&2 <<EOF
ERROR: cannot combine --lookback/--lookahead with positional <start> <end> dates.
Pick one mode: rolling (--lookback N --lookahead M) OR explicit (<start> <end>).
EOF
    exit 1
  fi
  # Default missing side to 0 so --lookback 1 alone works (end = today).
  [[ -z "$LOOKBACK" ]] && LOOKBACK=0
  [[ -z "$LOOKAHEAD" ]] && LOOKAHEAD=0
  if ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]] || ! [[ "$LOOKAHEAD" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --lookback / --lookahead must be non-negative integers (got lookback=$LOOKBACK, lookahead=$LOOKAHEAD)" >&2
    exit 1
  fi
  RANGE_DESC="rolling [today-${LOOKBACK}..today+${LOOKAHEAD}] UTC"
else
  if [[ $# -ne 2 ]]; then
    cat >&2 <<EOF
Usage: bash launch-api-football-backfill-vm.sh [--force] [--skip-lock] [--entity ENTITY] \\
         ( <START_DATE> <END_DATE> | --lookback N --lookahead M [--force-window] )

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  --lookback N / --lookahead M: rolling window resolved to [today-N..today+M] on the VM at boot (UTC).
  --force-window: disable skip-if-exists for the rolling window (forward-poll overwrite contract).
  --skip-lock: bypass ONLY the singleton lock (entity-sharded --fleet-vms fan-out) WITHOUT
               redo_all on the VM. --force = lock bypass + VM_FORCE=true (full re-fetch) — do
               NOT use --force just to clear the lock on a fleet launch.
  ENTITY (optional): FIXTURES | INJURIES | FIXTURE_STATS | FIXTURE_EVENTS |
                     FIXTURE_LINEUPS | PLAYER_STATS. Omit for all entities.

Examples:
  bash launch-api-football-backfill-vm.sh 2018-01-01 2019-01-15
  bash launch-api-football-backfill-vm.sh --entity FIXTURES 2026-04-21 2026-05-31
  bash launch-api-football-backfill-vm.sh --lookback 1 --lookahead 7
  bash launch-api-football-backfill-vm.sh --entity FIXTURES --lookback 1 --lookahead 7 --force-window
EOF
    exit 1
  fi

  START_DATE="$1"
  END_DATE="$2"

  # Sanity-check the date format up front — typos turn into silent VM boots.
  if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
    exit 1
  fi

  # 2020-06 sports DATA FLOOR (codex/02-data/sports-2020-06-data-floor.md): sports
  # odds/fixtures data starts 2020-06-06 — a pre-floor START_DATE is
  # fabrication-by-construction, not just stale. The venue-epoch skip gate
  # (get_venue_epoch) is defense-in-depth once the VM is running, not a
  # substitute for the launcher itself refusing a pre-floor explicit start (the
  # other sports launchers — launch-sports-entity-sweep-vm.sh,
  # launch-sports-instruments-reference-vm.sh, launch-mdps-backfill-vm.sh —
  # structurally can't accept one; this launcher's free-form <START_DATE> arg
  # could until now).
  SPORTS_DATA_FLOOR="2020-06-06"
  if [[ "$START_DATE" < "$SPORTS_DATA_FLOOR" ]]; then
    echo "ERROR: START_DATE ($START_DATE) is before the 2020-06-06 sports data floor — pre-floor" >&2
    echo "       API-Football fixtures/odds data is fabrication-by-construction, not stale data." >&2
    echo "       See codex/02-data/sports-2020-06-data-floor.md. Use ${SPORTS_DATA_FLOOR} or later." >&2
    exit 1
  fi
  RANGE_DESC="${START_DATE}..${END_DATE}"

  if $FORCE_WINDOW; then
    echo "WARNING: --force-window is only meaningful with --lookback/--lookahead; ignored in explicit-date mode." >&2
    FORCE_WINDOW=false
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# ── Rate-budget divisor from MEASURED concurrency (not an assumption) ──
# The per-VM throttle is EFFECTIVE_RPM / FLEET_VMS, and FLEET_VMS defaulted to 1 —
# i.e. every VM assumed it was ALONE unless a human remembered --fleet-vms N. Nothing
# enforced that promise, so every path that creates concurrency (--force, --skip-lock,
# a second actor, and especially auto-relaunch, which replays the ORIGINAL env) silently
# oversubscribed the shared key. Measured 2026-07-18: five concurrent VMs each throttling
# at a full-budget share produced 61 `rateLimit` FALSE attempted_failed rows in ~30min.
# We know the ceiling exactly (1200 req/min + 450k/day, ONE quota across all endpoints) —
# so the divisor must be a MEASURED fact, not a promise. Count first, divide by reality.
if ! $FLEET_VMS_EXPLICIT; then
  RUNNING_AF_COUNT="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^af-audit-") AND status=RUNNING' \
    --zones="$ZONE" --format='value(name)' 2>/dev/null | wc -l | tr -d ' ')"
  RUNNING_AF_COUNT="${RUNNING_AF_COUNT:-0}"
  if [[ "$RUNNING_AF_COUNT" -gt 0 ]]; then
    FLEET_VMS=$(( RUNNING_AF_COUNT + 1 ))
    echo "Rate-budget divisor: ${RUNNING_AF_COUNT} api-football VM(s) already RUNNING → --fleet-vms auto-derived to ${FLEET_VMS} (this VM throttles to 1/${FLEET_VMS} of the key ceiling)." >&2
    echo "  NOTE: the ${RUNNING_AF_COUNT} EXISTING VM(s) keep the budget they computed at THEIR launch, so the key is still oversubscribed until they finish. Prefer serialising over fanning out." >&2
  fi
fi

# ── Singleton lock: API-Football rate-limits per-key ──
# Catches both af-backfill-* (this script) and af-audit-* (truth-set audit
# launcher) — they share the api_football key and must be mutually exclusive.
# --skip-lock bypasses ONLY this check (deliberate --fleet-vms fan-out where
# the registry rate split governs the shared key); --force also bypasses it
# but ADDITIONALLY sets VM_FORCE=true (redo_all) on the VM — see the flag
# comment above.
if ! $FORCE && ! $SKIP_LOCK; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^af-audit-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: API-Football VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — API-Football rate-limits per-key; concurrent
VMs thrash on 429s without producing useful data (see 2026-04-19 SFI incident).

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ...

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing entity-fleet VM; deleting a live VM destroys hours of in-progress
work (see zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md
"Incident 2 correction" — this exact refusal message is the documented root
cause of 3 prior agent-deleted-own-fleet incidents). If, after that check, it is genuinely stale, construct the delete
command yourself — do not copy-paste one from this message — per infra.md
STEP 0.65's 3-signal staleness check (heartbeat age, run.log tail, manifest
mtime); when unsure, escalate instead of deleting.
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="af-backfill-${RUN_TS}"

ENTITY_DESC="all entities"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY only"
echo "Launching $VM_NAME: API_FOOTBALL backfill ${RANGE_DESC} ($ENTITY_DESC)"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${REPO_ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY="python3"

# ── Live daily-quota read (query, don't hardcode — operator 2026-06-23) ──
# api-football EXPOSES its real plan + usage at GET /status:
# {"response":{"requests":{"current":<used_today>,"limit_day":<plan_per_day>}}}.
# When REMAINING_DAILY_QUOTA was NOT hand-passed, default it to a LIVE read
# (limit_day - current) so the allocation is daily-aware off REAL usage rather
# than a hardcoded number. Resilient: any failure (no key, network, bad body)
# leaves REMAINING_DAILY_QUOTA empty → the registry per-minute cap is the only
# constraint (fine when the day is fresh), exactly as before. The fleet daily
# limit itself stays the registry fallback here; the running adapter additionally
# reads the live /status via get_live_quota() at runtime.
if [[ -z "$REMAINING_DAILY_QUOTA" ]]; then
  AF_KEY="$(gcloud secrets versions access latest --secret=api-football-api-key --project="$PROJECT" 2>/dev/null || true)"
  if [[ -n "$AF_KEY" ]]; then
    STATUS_JSON="$(curl -fsS --max-time 15 -H "x-apisports-key: ${AF_KEY}" \
      "https://v3.football.api-sports.io/status" 2>/dev/null || true)"
    if [[ -n "$STATUS_JSON" ]]; then
      # Pass the /status JSON as argv[1] (NOT stdin) — a here-doc already feeds
      # the program on stdin, so the JSON must go via an argument (SC2259).
      LIVE_REMAINING="$("$PY" - "$STATUS_JSON" <<'PYEOF' 2>/dev/null || true
import json, sys
try:
    body = json.loads(sys.argv[1])
    reqs = (body.get("response") or {}).get("requests") or {}
    limit_day = reqs.get("limit_day")
    current = reqs.get("current") or 0
    if isinstance(limit_day, int) and limit_day > 0:
        print(max(0, limit_day - int(current)))
except Exception:
    pass
PYEOF
)"
      if [[ "$LIVE_REMAINING" =~ ^[0-9]+$ ]]; then
        REMAINING_DAILY_QUOTA="$LIVE_REMAINING"
        echo "Live api_football /status: remaining_daily_quota=${REMAINING_DAILY_QUOTA} (limit_day - current; authoritative over the registry constant)"
      else
        echo "NOTE: api_football /status read returned no usable limit_day — leaving REMAINING_DAILY_QUOTA unset (per-minute cap only)." >&2
      fi
    fi
  fi
fi

# ── Registry-driven rate-budget allocation (Part 1) ──
# Deterministically split the api_football EFFECTIVE fleet ceiling (Custom plan
# 1200 req/min, daily-aware: min(1200, REMAINING_DAILY_QUOTA / minutes to 00:00
# UTC) — ONE quota shared across ALL endpoints) across FLEET_VMS concurrent VMs
# and derive this VM's per-minute share + matched concurrency. The registry
# asserts the fail-closed hard rule on BOTH axes (sum(per_vm × N) ≤ per-minute
# ceiling AND fleet_rpm × minutes_to_reset ≤ remaining daily quota). REMAINING_DAILY_QUOTA
# defaults to the live /status read above; the SSOT math runs in the repo venv so
# the same allocation runs here as the actuator consumes.
BUDGET_LINE="$(
  PYTHONPATH="${REPO_ROOT}" "$PY" - "$FLEET_VMS" "$REMAINING_DAILY_QUOTA" <<'PYEOF' 2>/dev/null || true
import sys
from deployment_service.data_pipeline_monitors.launch_budget_registry import (
    allocate_rate_budget,
    assert_fleet_within_budget,
)

n = int(sys.argv[1])
remaining = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
# max_per_query_rate_rpm=12 mirrors the operator's "each slot ~12 req/min"
# framing → concurrency = per_vm_rpm // 12. now_utc defaults to the UTC clock,
# so REMAINING_DAILY_QUOTA alone makes the allocation daily-aware (the effective
# ceiling drops below 1200 late in the day).
a = allocate_rate_budget("api_football", n_vms=n, max_per_query_rate_rpm=12, remaining_daily_quota=remaining)
assert_fleet_within_budget("api_football", n_vms=n, per_vm_rpm=a.per_vm_rpm, remaining_daily_quota=remaining)
print(f"{a.per_vm_rpm} {a.concurrency} {a.min_request_interval_s} {a.effective_source_rpm}")
PYEOF
)"
if [[ -n "$BUDGET_LINE" ]]; then
  read -r PER_VM_RPM ADAPTER_CONCURRENCY MIN_INTERVAL EFFECTIVE_RPM <<<"$BUDGET_LINE"
  echo "Rate-budget: api_football effective ${EFFECTIVE_RPM} req/min (Custom cap 1200/min; live /status daily quota) ÷ ${FLEET_VMS} VMs → ${PER_VM_RPM} req/min/VM, concurrency=${ADAPTER_CONCURRENCY}, interval=${MIN_INTERVAL}s (PRIMARY throttle)"
else
  PER_VM_RPM=""
  ADAPTER_CONCURRENCY=""
  echo "WARNING: rate-budget registry unavailable — VM keeps the adapter's class-default throttle (429 backoff still the safety net)." >&2
fi

# VM_TASK selection (2026-07-27, memory-accumulation OOM fix): a rolling
# window run is always a few days wide (never accumulates a large per-VM
# shard), so it stays on the generic single-shot "sports-backfill" dispatch.
# An explicit start/end range can span YEARS in one process — the per-VM
# shard's read-merge-write grows with cumulative shard size every flush
# (unified-trading-library ManifestWriter._flush_per_vm_pending), so a long
# single process eventually OOMs (measured: exit_code=137 after ~20.5hrs /
# ~2,400 dates / ~159K accumulated shard rows on af-backfill-20260726-110610).
# Routing to the EXISTING "instruments-backfill" VM_TASK (already used by
# launch-sports-instruments-reference-vm.sh / launch-sports-full-sweep-vm.sh
# / launch-sports-is-gap-fill.sh / launch-sports-entity-sweep-vm.sh) chunks
# the range into VM_CHUNK_DAYS-day windows, each run as a FRESH process —
# memory resets between chunks, and a chunk's failure (`|| true` in that
# branch's loop) doesn't abort the remaining chunks. No shared-dispatcher
# code changed; this only redirects what THIS launcher's own metadata sets.
METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
if $USE_ROLLING; then
  METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
  METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
  $FORCE_WINDOW && METADATA="${METADATA},VM_FORCE_WINDOW=true"
else
  # The "instruments-backfill" chunk-loop branch doesn't plumb
  # --recovery-fixture-ids (it only builds --sports-provider/--sports-entity/
  # --data-types/--force) — a targeted recovery run is bounded/short-lived
  # anyway (not the multi-year-range OOM this redirect exists for), so it
  # stays on the original generic dispatch rather than silently losing the
  # flag.
  if [[ -z "$RECOVERY_FIXTURE_IDS" ]]; then
    METADATA="${METADATA/VM_TASK=sports-backfill/VM_TASK=instruments-backfill}"
    METADATA="${METADATA},VM_CHUNK_DAYS=90"
  fi
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
fi
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
[[ -n "$RECOVERY_FIXTURE_IDS" ]] && METADATA="${METADATA},VM_RECOVERY_FIXTURE_IDS=${RECOVERY_FIXTURE_IDS}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# instruments-store-sports-prd's consolidator merge cycle regularly takes
# 400-460s (>3x the reader's 120s default) — see
# plans/active/issues/manifest_consolidator_stale_sports_bucket_2026_07_21.md
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=1800"
# Stamp the allocated rate-budget so the VM's adapter throttles at exactly its
# fleet share (setup-data-pipeline-vm.sh exports → typed config → adapter).
[[ -n "$PER_VM_RPM" ]] && METADATA="${METADATA},SPORTS_ADAPTER_RATE_RPM=${PER_VM_RPM}"
[[ -n "$ADAPTER_CONCURRENCY" ]] && METADATA="${METADATA},SPORTS_ADAPTER_CONCURRENCY=${ADAPTER_CONCURRENCY}"

# Machine type from the registry-driven machine-sizing map (Part 2): a
# sports-backfill:api_football VM launches correctly-sized on the FIRST try
# (registry SSOT, not repeated OOM-escalation). Operator can still override with
# MACHINE_TYPE=...; the OOM-escalation actuator steps UP the same ladder on a 137.
DEFAULT_MACHINE_TYPE="$(
  PYTHONPATH="${REPO_ROOT}" "$PY" - <<'PYEOF' 2>/dev/null || true
from deployment_service.data_pipeline_monitors.launch_budget_registry import machine_type_for
print(machine_type_for(task="sports-backfill", venue="api_football"))
PYEOF
)"
[[ -n "$DEFAULT_MACHINE_TYPE" ]] || DEFAULT_MACHINE_TYPE="e2-standard-4"
MACHINE_TYPE="${MACHINE_TYPE:-$DEFAULT_MACHINE_TYPE}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

  # Preemption signal: write PREEMPTED blob so the exit-code fleet monitor
  # classifies a spot preemption as a benign relaunch (no DP_VM_GONE_NO_CAPTURE).
  # Was a hand-rolled inline duplicate of lc_write_preemption_signal_file's
  # own logic (unlike the 14 other launchers that already call the shared
  # helper) — a live sweep of every af-backfill preemption confirmed via
  # `gcloud compute operations list` found the marker missing 5/5 times
  # (issues/session_bound_vm_monitoring_reliability_gap_2026_07_26.md), so
  # switched to the shared, hardened helper: VM_NAME/PROJECT are already
  # known at this point in the launcher, so bake them in (skips 2 of 3
  # metadata round-trips inside the ~30s preemption grace period) and the
  # helper itself now uses a lightweight curl+retry upload instead of
  # shelling out to the gcloud CLI.
  lc_write_preemption_signal_file "$VM_NAME" "$PROJECT"
  SHUTDOWN_FILE="$PREEMPTION_SIGNAL_FILE"

  echo "Launching VM $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
      # This launcher calls `gcloud compute instances create` directly rather
      # than the lc_gcloud_create wrapper (Pattern-B, like 138 of 143 launchers
      # in this tree — the wrapper's own auto-check only covers 4 callers
      # today, not the "~80 launchers" its comment claims; filed as its own
      # finding, see issues/vm_launcher_setup_script_freshness_gap_2026_07_31.md),
      # so it never got the setup-script freshness guard automatically. Added
      # explicitly here: this VM's ENTIRE preemption-recovery story (the
      # fleet-wide uts-preemption-signal.service PLUS the shutdown-script
      # below) depends on `vm/setup-data-pipeline-vm.sh` being CURRENT on GCS
      # — a stale copy silently runs old startup logic with no error. Default
      # mode is "warn" (LC_SETUP_SCRIPT_FRESHNESS=enforce to block on drift).
      FULL_METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}"
      lc_verify_setup_script_freshness "$CODE_BUCKET" "$FULL_METADATA" \
          || { echo "ERROR: aborting launch on stale/missing setup script — see above" >&2; exit 1; }
  fi

  # SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
  # "Close the SPOT-preemption relaunch gap"): persist the EXACT window/entity
  # this VM was launched with so exit_code_fleet_monitor's PREEMPTED
  # auto_recover actuator (relaunch_backfill_vm.RelaunchPreemptedVm) can
  # re-invoke THIS launcher through the RESUME_* env fallback above instead of
  # a blind default (which would hit the usage error / relaunch nothing).
  # Best-effort, non-fatal.
  if $USE_ROLLING; then
    lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-api-football-backfill-vm.sh" \
        "RESUME_LOOKBACK=${LOOKBACK}" \
        "RESUME_LOOKAHEAD=${LOOKAHEAD}" \
        "RESUME_FORCE_WINDOW=${FORCE_WINDOW}" \
        "RESUME_ENTITY=${ENTITY}" \
        "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  else
    lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-api-football-backfill-vm.sh" \
        "RESUME_START_DATE=${START_DATE}" \
        "RESUME_END_DATE=${END_DATE}" \
        "RESUME_ENTITY=${ENTITY}" \
        "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="${FULL_METADATA}" \
    --metadata-from-file=shutdown-script="${SHUTDOWN_FILE}" \
    --labels=purpose=api-football-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
