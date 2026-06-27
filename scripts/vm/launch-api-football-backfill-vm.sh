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
# Writes to gs://instruments-store-sports-central-element-323112/
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
REMAINING_DAILY_QUOTA="${REMAINING_DAILY_QUOTA:-}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --force-window) FORCE_WINDOW=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    --fleet-vms) FLEET_VMS="$2"; shift 2 ;;
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
Usage: bash launch-api-football-backfill-vm.sh [--force] [--entity ENTITY] \\
         ( <START_DATE> <END_DATE> | --lookback N --lookahead M [--force-window] )

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  --lookback N / --lookahead M: rolling window resolved to [today-N..today+M] on the VM at boot (UTC).
  --force-window: disable skip-if-exists for the rolling window (forward-poll overwrite contract).
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
  RANGE_DESC="${START_DATE}..${END_DATE}"

  if $FORCE_WINDOW; then
    echo "WARNING: --force-window is only meaningful with --lookback/--lookahead; ignored in explicit-date mode." >&2
    FORCE_WINDOW=false
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# ── Singleton lock: API-Football rate-limits per-key ──
# Catches both af-backfill-* (this script) and af-audit-* (truth-set audit
# launcher) — they share the api_football key and must be mutually exclusive.
if ! $FORCE; then
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
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ...
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

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
if $USE_ROLLING; then
  METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
  METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
  $FORCE_WINDOW && METADATA="${METADATA},VM_FORCE_WINDOW=true"
else
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
fi
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
[[ -n "$ENTITY" ]] && METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY}"
[[ -n "$RECOVERY_FIXTURE_IDS" ]] && METADATA="${METADATA},VM_RECOVERY_FIXTURE_IDS=${RECOVERY_FIXTURE_IDS}"
$FORCE && METADATA="${METADATA},VM_FORCE=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
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

  echo "Launching VM $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
  SHUTDOWN_FILE=$(mktemp)
  cat > "$SHUTDOWN_FILE" <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
PREEMPTED_FLAG=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/preempted' 2>/dev/null || echo 'false')
[[ "$PREEMPTED_FLAG" == "true" ]] || exit 0
SELF_VM=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')
[[ -n "$SELF_VM" ]] || exit 0
echo "1" | gsutil -q cp - "gs://deployment-scripts-central-element-323112/vm-logs/${SELF_VM}/PREEMPTED" 2>/dev/null || true
SHUTDOWN_EOF
  # shellcheck disable=SC2086
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --metadata-from-file="shutdown-script=${SHUTDOWN_FILE}" \
    --labels=purpose=api-football-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
  rm -f "$SHUTDOWN_FILE"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
