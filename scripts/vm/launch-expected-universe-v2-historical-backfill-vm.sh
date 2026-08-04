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
# This idempotency is also what makes the automatic same-chunk RETRY below
# safe: the enumerator discovers already-written per-VM shards via a live
# `_index/per_vm/*.parquet` glob (enumerate_expected_universe.py:~3575), so a
# retry of the exact same [start,end] window excludes rows the prior (halted)
# attempt already wrote and only seeds the remaining backlog — it converges,
# it does not restart from zero or double-write.
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
#   MAX_CHUNK_ATTEMPTS        max relaunches of the SAME chunk window before
#                             aborting the whole script (default 50). Needed
#                             because a full calendar-year window for a
#                             448K+-instrument catalog (sports) routinely
#                             exceeds the enumerator's own
#                             --max-writes-per-run halt-safety (default 1M,
#                             enumerate_expected_universe.py) on the FIRST
#                             attempt (confirmed live 2026-08-03: chunk
#                             2020-06-06..2020-12-31 tripped it in <1 min).
#                             VM-terminal status alone does NOT mean the
#                             enumerator succeeded — see the EXIT_STATUS check
#                             below.
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

# Pin the gcloud identity PER-INVOCATION — the ambient `gcloud config`
# active account is HOST-WIDE (shared by every concurrent slot, not
# per-slot), so a sibling slot's `gcloud config set account` can silently
# swap in a less-privileged identity between this script's own gcloud calls
# (instances describe / operations list / storage cat, used by the wait +
# EXIT_STATUS + preemption checks below). Same rationale + confirmed
# 2026-08-04 incident as launch-expected-universe-v2-vm.sh — see
# plans/active/issues/shared_host_gcloud_active_account_cross_slot_clobber_2026_08_04.md.
export CLOUDSDK_CORE_ACCOUNT="${CLOUDSDK_CORE_ACCOUNT:-unified-trading-sa@central-element-323112.iam.gserviceaccount.com}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHILD_LAUNCHER="${CHILD_LAUNCHER:-${SCRIPT_DIR}/launch-expected-universe-v2-vm.sh}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-60}"
CHUNK_TIMEOUT_SECONDS="${CHUNK_TIMEOUT_SECONDS:-21600}"
MAX_CHUNK_ATTEMPTS="${MAX_CHUNK_ATTEMPTS:-50}"
ZONE="asia-northeast1-c"
# Matches the hardcoded PROJECT the child launcher itself uses
# (launch-expected-universe-v2-vm.sh) — the vm-logs code bucket is always
# `deployment-scripts-<project>` (lib/launcher_common.sh's lc_code_bucket).
PROJECT="central-element-323112"

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
# _read_exit_status <vm_name>
# Reads the durable terminal EXIT_STATUS marker (lc_log_upload_trap_block,
# lib/launcher_common.sh) — the enumerator's OWN exit code, distinct from the
# GCE VM's compute lifecycle state. A VM reaching TERMINATED/GONE only means
# the compute resource stopped; it does NOT mean the enumerator run inside it
# succeeded (rc=0). Written to GCS by the VM's EXIT trap BEFORE it
# self-deletes, so this is safe to read immediately after
# _wait_for_vm_terminal returns. Empty output means the marker is missing
# (e.g. the whole VM unit was SIGKILLed before the trap could run) — treated
# as a failure, never as an implicit success.
_read_exit_status() {
    local vm_name="$1"
    gcloud storage cat "gs://deployment-scripts-${PROJECT}/vm-logs/${vm_name}/EXIT_STATUS" 2>/dev/null || echo ""
}

# _was_preempted <vm_name>
# A SPOT VM (every backfill VM defaults to SPOT per the HARD RULE) reclaimed
# by GCE before its startup script even reaches the log-upload trap leaves
# EMPTY EXIT_STATUS — indistinguishable, from that marker alone, from a
# genuine unknown failure. Confirmed live 2026-08-03: a manually-relaunched
# chunk-2 retry VM was preempted ~2 min after creation with zero logs/events
# written (compute.instances.preempted, verified via `gcloud compute
# operations list`). Preemption is routine/expected for SPOT, not a bug —
# check the operations log before treating a missing marker as fatal.
_was_preempted() {
    local vm_name="$1"
    gcloud compute operations list \
        --filter="targetLink~${vm_name} AND operationType=compute.instances.preempted" \
        --format='value(name)' 2>/dev/null | grep -q .
}

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

    attempt=0
    while true; do
        attempt=$((attempt + 1))
        if [[ "$attempt" -gt "$MAX_CHUNK_ATTEMPTS" ]]; then
            echo "ERROR: chunk ${chunk_start}..${chunk_end} still hit the max-writes-per-run halt-safety after ${MAX_CHUNK_ATTEMPTS} attempts — this is no longer the expected large-historical-window backlog, it may be a runaway candidate generator (the same bug class as the 2026-07-14 tradfi + 2026-08-01 defi incidents). STOPPING — do not raise MAX_CHUNK_ATTEMPTS or --max-writes-per-run to push through without investigating first." >&2
            exit 1
        fi
        if [[ "$attempt" -gt 1 ]]; then
            echo "--- Chunk ${CHUNK_NUM}/${#CHUNKS[@]} retry ${attempt}/${MAX_CHUNK_ATTEMPTS} (prior attempt hit the enumerator's max-writes-per-run halt-safety; rows it already wrote are excluded from this attempt's candidate count, so this only seeds the remaining backlog for the same window) ---"
        fi

        # set +e around the substitution: under `set -e`, a failing command
        # substitution in an assignment aborts the script AT THE ASSIGNMENT —
        # before the echo below ever runs — so a real child-launcher failure
        # (e.g. a PERMISSION_DENIED from a clobbered active gcloud identity)
        # would silently vanish with zero diagnostic output. Capture the exit
        # code explicitly instead so the failure is always visible.
        set +e
        LAUNCH_OUTPUT="$(ENUM_START_DATE="$chunk_start" ENUM_END_DATE="$chunk_end" \
            bash "$CHILD_LAUNCHER" --env "$DEPLOYMENT_ENV" "$ASSET_GROUP" --apply-write 2>&1)"
        LAUNCH_EXIT=$?
        set -e
        echo "$LAUNCH_OUTPUT"

        if [[ "$LAUNCH_EXIT" -ne 0 ]]; then
            echo "ERROR: child launcher exited ${LAUNCH_EXIT} for chunk ${chunk_start}..${chunk_end} (see output above) — aborting" >&2
            exit 1
        fi

        VM_NAME="$(grep -oE '^VM launched: .*' <<<"$LAUNCH_OUTPUT" | sed 's/^VM launched: //')"
        if [[ -z "$VM_NAME" ]]; then
            echo "ERROR: could not determine VM name from launcher output for chunk ${chunk_start}..${chunk_end}" >&2
            exit 1
        fi

        echo "Waiting for ${VM_NAME} to complete (poll every ${POLL_INTERVAL_SECONDS}s, timeout ${CHUNK_TIMEOUT_SECONDS}s)..."
        _wait_for_vm_terminal "$VM_NAME"

        # VM-terminal is a compute-lifecycle signal, not a success signal —
        # check the enumerator's OWN exit code before trusting this chunk.
        exit_status="$(_read_exit_status "$VM_NAME")"
        case "$exit_status" in
            0)
                echo "  -> ${VM_NAME}: enumerator EXIT_STATUS=0 (success)"
                break
                ;;
            5)
                echo "  -> ${VM_NAME}: enumerator EXIT_STATUS=5 (max-writes-per-run halt-safety — chunk partially seeded, safe to retry the same window)"
                continue
                ;;
            "")
                if _was_preempted "$VM_NAME"; then
                    echo "  -> ${VM_NAME}: no EXIT_STATUS, but confirmed preempted (compute.instances.preempted — normal SPOT reclaim) — retrying the same window"
                    continue
                fi
                echo "ERROR: ${VM_NAME} has no EXIT_STATUS marker and was NOT preempted (checked gcloud compute operations list) — a genuine unknown failure, not routine SPOT reclaim. Aborting the whole backfill; do NOT trust chunks after this one. Inspect gs://deployment-scripts-${PROJECT}/vm-logs/${VM_NAME}/run.log (may be empty/absent) and the VM's serial console." >&2
                exit 1
                ;;
            *)
                echo "ERROR: ${VM_NAME} enumerator EXIT_STATUS='${exit_status}' (expected 0=success or 5=retriable halt-safety) — aborting the whole backfill; do NOT trust chunks after this one. Inspect gs://deployment-scripts-${PROJECT}/vm-logs/${VM_NAME}/run.log" >&2
                exit 1
                ;;
        esac
    done
done

echo ""
echo "All ${#CHUNKS[@]} chunks launched + terminated for asset_group=${ASSET_GROUP}."
echo "Verify (NOT fire-and-forget — check each chunk's own run.log + lifecycle events):"
echo "  gcloud compute instances list --filter='name~\"^expected-universe-v2-${ASSET_GROUP}-\"'"
echo "  gsutil ls 'gs://deployment-scripts-central-element-323112/vm-logs/expected-universe-v2-${ASSET_GROUP}-*/run.log'"
