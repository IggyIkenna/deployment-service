#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# GATED one-time historical `expected_unattempted` denominator backfill —
# floors the v2 enumerator's window at a real historical date instead of the
# daily recurring scheduler's rolling `today - 120d` window
# (deployment-service/terraform/gcp/expected_universe_v2_scheduler.tf). Chunks
# the floor-date..(rolling-boundary - 1 day) range into calendar-year windows
# and launches launch-expected-universe-v2-vm.sh --apply-write for each,
# SEQUENTIALLY (waits for each VM to reach a terminal state before launching
# the next) — this respects the child launcher's own singleton lock instead of
# bypassing it, and keeps concurrent GCS load on the manifest bucket bounded to
# one enumerator run at a time.
#
# Never auto-scheduled (no Cloud Scheduler / cron wiring here by design — the
# "gated" in the task name) — an operator/agent invokes this explicitly,
# once, per asset_group.
#
# Why the rolling-boundary cutoff (not "today"): the recurring daily job
# already re-seeds the trailing `today - 120d` window on every run (job (1),
# deployment-service@1d8ede9), so backfilling any date >= that boundary would
# just duplicate work the recurring job already covers on its next run.
#
# Safe/idempotent (per craft north-star + task_template.md finding O/T
# safe-idempotent-justification path): the v2 enumerator only ADDS
# `expected_unattempted` rows for (instrument, date, data_type) cells that
# currently have NO capture_status row at all — it never overwrites an
# existing captured/empty_confirmed/attempted_failed row. Each chunk run also
# writes to its OWN per-VM shard (MANIFEST_PER_VM_SHARDS=true, VM_NAME unique
# per launch), so a re-run of this script (or a partial retry after a failed
# chunk) is safe to repeat — it recomputes + rewrites only that chunk's shard.
#
# Usage:
#   bash launch-expected-universe-v2-historical-backfill-vm.sh sports
#   bash launch-expected-universe-v2-historical-backfill-vm.sh sports --dry-run
#   bash launch-expected-universe-v2-historical-backfill-vm.sh cefi --floor-date 2021-01-01
#   bash launch-expected-universe-v2-historical-backfill-vm.sh sports --env staging
#
# Args:
#   1. asset_group: cefi | defi | tradfi | prediction | sports (required)
#   --floor-date YYYY-MM-DD   backfill floor. Required for every asset_group
#                             EXCEPT sports, which defaults to 2020-06-06 (the
#                             codified sports data floor —
#                             codex/02-data/sports-2020-06-data-floor.md). No
#                             other asset_group has a codified historical floor
#                             yet (see this issue's job (3), a separate
#                             read-only measurement) — pass one explicitly.
#   --dry-run                 print the computed chunk plan; launches nothing
#   --env ENV                 prod (default) | staging | dev — forwarded to
#                             the child launcher
#
# Env overrides:
#   CHILD_LAUNCHER            override path to launch-expected-universe-v2-vm.sh
#                             (test hook)
#   POLL_INTERVAL_SECONDS     seconds between VM-status polls (default 60)
#   CHUNK_TIMEOUT_SECONDS     max seconds to wait for one chunk's VM to reach a
#                             terminal state before aborting (default 21600 = 6h
#                             — a full calendar year of enumeration is a larger
#                             cross-join than the recurring job's bounded
#                             120-day window)
#
# Execution ownership (Runbook SSOT):
#   execution:
#     owner: operator / dispatched data_engineering worker, one-shot per
#            asset_group (never recurring — see "gated" above)
#     cadence: one-time (per asset_group, until its full history is seeded)
#     verifier: `gcloud compute instances list --filter='name~"^expected-universe-v2-<ag>-"'`
#               shows each chunk VM TERMINATED, no FAILED lifecycle event in
#               gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#     last_executed: NEVER (see this script's own launch evidence in the
#                    dispatching issue doc's Progress Log for the first real run)
#
# References:
#   plans/active/issues/sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists_2026_07_27.md job (2)
#   codex/02-data/sports-2020-06-data-floor.md
#   deployment-service/terraform/gcp/expected_universe_v2_scheduler.tf
#   deployment-service/scripts/vm/launch-expected-universe-v2-vm.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHILD_LAUNCHER="${CHILD_LAUNCHER:-${SCRIPT_DIR}/launch-expected-universe-v2-vm.sh}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-60}"
CHUNK_TIMEOUT_SECONDS="${CHUNK_TIMEOUT_SECONDS:-21600}"
ZONE="asia-northeast1-c"

if [[ ! -f "$CHILD_LAUNCHER" ]]; then
    echo "ERROR: child launcher not found at $CHILD_LAUNCHER" >&2
    exit 1
fi

# Portable "days-back from today" / date-arithmetic — GNU date primary, BSD
# date fallback (matches _cefi-fts-launcher-lib.sh's existing convention).
_today_minus_days() {
    local days="$1"
    date -u -d "-${days} days" +%F 2>/dev/null || date -u -v-"${days}"d +%F
}
_date_minus_one_day() {
    date -u -d "${1} - 1 day" +%F 2>/dev/null || date -u -v-1d -j -f '%Y-%m-%d' "$1" +%F
}
_date_year() {
    date -u -d "$1" +%Y 2>/dev/null || date -u -j -f '%Y-%m-%d' "$1" +%Y
}

DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FLOOR_DATE=""

ASSET_GROUP="${1:-}"
if [[ -z "$ASSET_GROUP" ]]; then
    echo "ERROR: asset_group is required (cefi | defi | tradfi | prediction | sports)" >&2
    exit 2
fi
shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        --floor-date) FLOOR_DATE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) echo "ERROR: unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$FLOOR_DATE" ]]; then
    if [[ "$ASSET_GROUP" == "sports" ]]; then
        # codex/02-data/sports-2020-06-data-floor.md — the sports honest-coverage
        # base month; odds start 2020-06-06, pre-floor is fabrication-by-construction.
        FLOOR_DATE="2020-06-06"
    else
        echo "ERROR: --floor-date is required for asset_group=${ASSET_GROUP} (only sports has a codified historical floor — see this issue's job (3) for the other asset_groups' read-only measurement)" >&2
        exit 2
    fi
fi
if ! date -u -d "$FLOOR_DATE" +%F >/dev/null 2>&1 && ! date -u -j -f '%Y-%m-%d' "$FLOOR_DATE" +%F >/dev/null 2>&1; then
    echo "ERROR: --floor-date must be YYYY-MM-DD (got: $FLOOR_DATE)" >&2
    exit 2
fi

# Mirrors expected_universe_v2_scheduler.tf's local.expected_universe_start_date
# (today - 120d) — recomputed live, never a frozen literal, so a re-run of this
# script weeks later correctly narrows the historical range still needing a
# backfill.
ROLLING_BOUNDARY="$(_today_minus_days 120)"
END_BOUNDARY="$(_date_minus_one_day "$ROLLING_BOUNDARY")"

if [[ "$FLOOR_DATE" > "$END_BOUNDARY" ]]; then
    echo "Nothing to backfill: --floor-date ${FLOOR_DATE} is already inside the recurring job's rolling window (>= ${ROLLING_BOUNDARY})."
    exit 0
fi

# Build calendar-year chunks from FLOOR_DATE..END_BOUNDARY.
CHUNKS=()
floor_year="$(_date_year "$FLOOR_DATE")"
end_year="$(_date_year "$END_BOUNDARY")"
year="$floor_year"
current_start="$FLOOR_DATE"
while [[ "$year" -le "$end_year" ]]; do
    if [[ "$year" -eq "$end_year" ]]; then
        current_end="$END_BOUNDARY"
    else
        current_end="${year}-12-31"
    fi
    CHUNKS+=("${current_start}|${current_end}")
    year=$((year + 1))
    current_start="${year}-01-01"
done

echo "Historical expected_unattempted backfill plan for asset_group=${ASSET_GROUP}"
echo "  floor date:        ${FLOOR_DATE}"
echo "  rolling boundary:  ${ROLLING_BOUNDARY} (recurring daily job covers this forward)"
echo "  chunks (${#CHUNKS[@]}):"
for chunk in "${CHUNKS[@]}"; do
    echo "    ${chunk%%|*} .. ${chunk##*|}"
done

if $DRY_RUN; then
    echo ""
    echo "[DRY-RUN] No VMs launched."
    exit 0
fi

# --- sequential launch + wait-for-terminal loop -----------------------------
_wait_for_vm_terminal() {
    local vm_name="$1"
    local waited=0
    while true; do
        local status
        status="$(gcloud compute instances describe "$vm_name" --zone="$ZONE" --format='value(status)' 2>/dev/null || echo "GONE")"
        case "$status" in
            TERMINATED|GONE)
                echo "  -> ${vm_name}: ${status}"
                return 0
                ;;
            *)
                echo "  -> ${vm_name}: ${status} (waited ${waited}s)"
                ;;
        esac
        if [[ "$waited" -ge "$CHUNK_TIMEOUT_SECONDS" ]]; then
            echo "ERROR: ${vm_name} did not reach a terminal state within ${CHUNK_TIMEOUT_SECONDS}s" >&2
            return 1
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        waited=$((waited + POLL_INTERVAL_SECONDS))
    done
}

CHUNK_NUM=0
for chunk in "${CHUNKS[@]}"; do
    CHUNK_NUM=$((CHUNK_NUM + 1))
    chunk_start="${chunk%%|*}"
    chunk_end="${chunk##*|}"
    echo ""
    echo "=== Chunk ${CHUNK_NUM}/${#CHUNKS[@]}: ${chunk_start}..${chunk_end} ==="

    LAUNCH_OUTPUT="$(ENUM_START_DATE="$chunk_start" ENUM_END_DATE="$chunk_end" \
        bash "$CHILD_LAUNCHER" --env "$DEPLOYMENT_ENV" "$ASSET_GROUP" --apply-write 2>&1)"
    echo "$LAUNCH_OUTPUT"

    VM_NAME="$(grep -oE '^VM launched: .*' <<<"$LAUNCH_OUTPUT" | sed 's/^VM launched: //')"
    if [[ -z "$VM_NAME" ]]; then
        echo "ERROR: could not determine VM name from launcher output for chunk ${chunk_start}..${chunk_end}" >&2
        exit 1
    fi

    echo "Waiting for ${VM_NAME} to complete (poll every ${POLL_INTERVAL_SECONDS}s, timeout ${CHUNK_TIMEOUT_SECONDS}s)..."
    _wait_for_vm_terminal "$VM_NAME"
done

echo ""
echo "All ${#CHUNKS[@]} chunks launched + terminated for asset_group=${ASSET_GROUP}."
echo "Verify (NOT fire-and-forget — check each chunk's own run.log + lifecycle events):"
echo "  gcloud compute instances list --filter='name~\"^expected-universe-v2-${ASSET_GROUP}-\"'"
echo "  gsutil ls 'gs://deployment-scripts-central-element-323112/vm-logs/expected-universe-v2-${ASSET_GROUP}-*/run.log'"
