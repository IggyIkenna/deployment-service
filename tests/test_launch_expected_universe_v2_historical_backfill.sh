#!/usr/bin/env bash
# Shell test harness for launch-expected-universe-v2-historical-backfill-vm.sh
# — arg validation + chunk-plan computation (--dry-run, no gcloud calls) + the
# sequential launch/wait loop against stubbed gcloud + a stubbed child
# launcher (CHILD_LAUNCHER override hook).
#
# Companion to tests/test_launch_expected_universe_v2.sh (the child launcher's
# own test harness) — this harness covers the historical-backfill wrapper's
# OWN logic (chunking, floor-date defaulting/validation, rolling-boundary
# cutoff, sequential wait-for-terminal), not the child launcher's arg parsing.
#
# Usage:
#   bash tests/test_launch_expected_universe_v2_historical_backfill.sh
#   bash tests/test_launch_expected_universe_v2_historical_backfill.sh -v   # verbose
#
# Exits 0 on all-pass, 1 on any-fail.
#
# Execution ownership (Runbook SSOT):
#   execution:
#     owner: data_engineering worker (one-shot per plan close-out); CI in future
#     cadence: pre-deploy (wire into deployment-service quality-gates.sh)
#     verifier: stdout contains "ALL TESTS PASS" on success
#     last_executed: 2026-08-03

set -u

VERBOSE=false
if [[ "${1:-}" == "-v" ]]; then
    VERBOSE=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="${SCRIPT_DIR}/../scripts/vm/launch-expected-universe-v2-historical-backfill-vm.sh"

if [[ ! -f "$LAUNCHER" ]]; then
    echo "FATAL: launcher not found at $LAUNCHER" >&2
    exit 1
fi

# Mirror the launcher's OWN rolling-boundary computation (today - 120d, minus
# 1 day for END_BOUNDARY) so date-window assertions below don't hardcode a
# literal that silently goes stale the next time the wall-clock date rolls
# over (bit us live 2026-08-03 -> 2026-08-04 mid-session: a hardcoded
# "2026-04-04" broke the instant the boundary shifted to "2026-04-05").
_test_today_minus_days() {
    date -u -d "-${1} days" +%F 2>/dev/null || date -u -v-"${1}"d +%F
}
_test_date_minus_one_day() {
    date -u -d "${1} - 1 day" +%F 2>/dev/null || date -u -v-1d -j -f '%Y-%m-%d' "$1" +%F
}
END_BOUNDARY="$(_test_date_minus_one_day "$(_test_today_minus_days 120)")"

STUB_DIR="$(mktemp -d)"
trap 'rm -rf "$STUB_DIR"' EXIT

# gcloud stub: "instances describe" always reports TERMINATED so the wait loop
# exits immediately. "storage cat ...EXIT_STATUS" pops the next value off
# $STUB_DIR/exit_status_queue (one status per launch attempt); an empty/absent
# queue defaults to "0" (success) so pre-existing tests that never touch the
# queue keep seeing success, matching the original stub's behavior. A queued
# value of the literal "EMPTY" simulates a missing EXIT_STATUS marker (e.g. a
# preempted SPOT VM that never reached the log-upload trap) by printing
# nothing. "compute operations list" reports a fake preemption match when
# $STUB_DIR/simulate_preempted exists, else nothing.
export STUB_DIR
cat > "$STUB_DIR/gcloud" <<'STUB'
#!/usr/bin/env bash
case "${1:-}-${2:-}-${3:-}" in
    compute-instances-describe) echo "TERMINATED"; exit 0 ;;
    storage-cat-gs://*)
        QUEUE="${STUB_DIR}/exit_status_queue"
        if [[ -s "$QUEUE" ]]; then
            VAL="$(head -1 "$QUEUE")"
            tail -n +2 "$QUEUE" > "${QUEUE}.tmp" && mv "${QUEUE}.tmp" "$QUEUE"
            [[ "$VAL" != "EMPTY" ]] && echo "$VAL"
        else
            echo "0"
        fi
        exit 0
        ;;
    compute-operations-list)
        [[ -f "${STUB_DIR}/simulate_preempted" ]] && echo "op-preempted-stub"
        exit 0
        ;;
    *) exit 0 ;;
esac
STUB
chmod +x "$STUB_DIR/gcloud"

# Child-launcher stub: mimics launch-expected-universe-v2-vm.sh's stdout
# contract ("VM launched: <name>") and echoes the ENUM_START_DATE/END_DATE it
# was called with (captured to a file for assertions). If
# $STUB_DIR/child_should_fail exists, simulates a child-launcher failure (e.g.
# a PERMISSION_DENIED from a clobbered gcloud identity) instead — prints a
# diagnostic to stderr and exits 1, no "VM launched" line.
CHILD_CAPTURE_FILE="$STUB_DIR/child_calls.log"
cat > "$STUB_DIR/child_launcher_stub.sh" <<STUB
#!/usr/bin/env bash
echo "\${ENUM_START_DATE:-}|\${ENUM_END_DATE:-}|\${CLOUDSDK_CORE_ACCOUNT:-}|\$*" >> "${CHILD_CAPTURE_FILE}"
if [[ -f "${STUB_DIR}/child_should_fail" ]]; then
    echo "ERROR: (gcloud.compute.instances.create) Could not fetch resource: simulated PERMISSION_DENIED" >&2
    exit 1
fi
echo "VM launched: stub-vm-\${ENUM_START_DATE//-/}"
STUB
chmod +x "$STUB_DIR/child_launcher_stub.sh"

export PATH="$STUB_DIR:$PATH"

PASS=0
FAIL=0
FAILED_CASES=()

_run() {
    local name="$1"; shift
    local expected_exit="$1"; shift
    local match_pattern="${1:-}"
    [[ -n "$match_pattern" ]] && shift || true
    OUTPUT="$("$@" 2>&1)"
    ACTUAL_EXIT=$?
    OK=true
    if [[ "$ACTUAL_EXIT" != "$expected_exit" ]]; then
        OK=false
    fi
    if [[ -n "$match_pattern" ]] && ! grep -qE "$match_pattern" <<<"$OUTPUT"; then
        OK=false
    fi
    if $OK; then
        PASS=$((PASS + 1))
        $VERBOSE && echo "  ✅ $name"
    else
        FAIL=$((FAIL + 1))
        FAILED_CASES+=("$name (exit=$ACTUAL_EXIT expected=$expected_exit)")
        echo "  ❌ $name (exit=$ACTUAL_EXIT expected=$expected_exit)"
        $VERBOSE && echo "$OUTPUT" | head -20 | sed 's/^/     /'
    fi
}

echo "Running launch-expected-universe-v2-historical-backfill-vm.sh shell tests..."

# (a) missing asset_group exits 2
_run "(a) missing asset_group -> exit 2" 2 "asset_group is required" \
    bash "$LAUNCHER"

# (b) invalid asset_group exits 2
_run "(b) invalid asset_group -> exit 2" 2 "must be one of cefi/defi/tradfi" \
    bash "$LAUNCHER" cryptopunk --dry-run

# (c) invalid --env exits 1
_run "(c) invalid --env -> exit 1" 1 "must be one of prod/staging/dev" \
    bash "$LAUNCHER" sports --env weird --dry-run

# (d) invalid --floor-date format exits 2
_run "(d) invalid --floor-date -> exit 2" 2 "floor-date must be YYYY-MM-DD" \
    bash "$LAUNCHER" sports --floor-date not-a-date --dry-run

# (e) non-sports asset_group without --floor-date exits 2 (no codified floor yet)
_run "(e) cefi without --floor-date -> exit 2" 2 "floor-date is required for asset_group=cefi" \
    bash "$LAUNCHER" cefi --dry-run

# (f) sports defaults --floor-date to the codified 2020-06-06 floor
_run "(f) sports defaults floor to 2020-06-06" 0 "floor date:( *)2020-06-06" \
    bash "$LAUNCHER" sports --dry-run

# (g) --floor-date already inside the rolling window -> no chunks, exit 0
_run "(g) floor-date inside rolling window -> exit 0, nothing to backfill" 0 "Nothing to backfill" \
    bash "$LAUNCHER" sports --floor-date 2026-07-01 --dry-run

# (h) --dry-run never invokes gcloud/the child launcher (no "VM launched" line)
OUTPUT="$(bash "$LAUNCHER" sports --dry-run 2>&1)"
if ! grep -q "VM launched" <<<"$OUTPUT"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (h) --dry-run launches nothing"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(h) --dry-run unexpectedly launched a VM")
    echo "  ❌ (h) --dry-run unexpectedly launched a VM"
fi

# (i) real (non-dry-run) run against stubs: correct chunk count + each chunk's
# ENUM_START_DATE/ENUM_END_DATE reaches the child launcher, in order.
rm -f "$CHILD_CAPTURE_FILE"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2025-06-01 2>&1)"
EXIT=$?
if [[ "$EXIT" == "0" ]] && grep -q "All 2 chunks launched" <<<"$OUTPUT" \
    && [[ -f "$CHILD_CAPTURE_FILE" ]] \
    && [[ "$(wc -l < "$CHILD_CAPTURE_FILE")" == "2" ]] \
    && grep -q "^2025-06-01|2025-12-31|" "$CHILD_CAPTURE_FILE" \
    && grep -q "^2026-01-01|${END_BOUNDARY}|" "$CHILD_CAPTURE_FILE"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (i) sequential launch reaches child launcher with correct per-chunk dates"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(i) sequential launch chunk dates/count mismatch")
    echo "  ❌ (i) sequential launch chunk dates/count mismatch"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; sed 's/^/     capture: /' "$CHILD_CAPTURE_FILE" 2>/dev/null; }
fi

# (i2) CLOUDSDK_CORE_ACCOUNT is pinned to the ambient identity for every child
# launcher call (per-invocation account pin — see
# shared_host_gcloud_active_account_cross_slot_clobber_2026_08_04.md), not
# left to inherit whatever the shared host's active gcloud config happens to
# be at that moment.
if grep -qE '\|unified-trading-sa@[^|]+\.iam\.gserviceaccount\.com\|' "$CHILD_CAPTURE_FILE"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (i2) CLOUDSDK_CORE_ACCOUNT pinned for child launcher calls"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(i2) CLOUDSDK_CORE_ACCOUNT not pinned for child launcher calls")
    echo "  ❌ (i2) CLOUDSDK_CORE_ACCOUNT not pinned for child launcher calls"
    $VERBOSE && sed 's/^/     capture: /' "$CHILD_CAPTURE_FILE" 2>/dev/null
fi

# (j) enumerator halt-safety (EXIT_STATUS=5) on the first attempt triggers an
# automatic retry of the SAME window; the second attempt succeeds
# (EXIT_STATUS=0) -> the chunk completes with 2 launches, not 1.
rm -f "$CHILD_CAPTURE_FILE"
printf '5\n0\n' > "$STUB_DIR/exit_status_queue"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
if [[ "$EXIT" == "0" ]] && grep -q "All 1 chunks launched" <<<"$OUTPUT" \
    && grep -q "retry 2/50" <<<"$OUTPUT" \
    && [[ -f "$CHILD_CAPTURE_FILE" ]] \
    && [[ "$(wc -l < "$CHILD_CAPTURE_FILE")" == "2" ]]; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (j) halt-safety EXIT_STATUS=5 triggers same-window retry, then succeeds"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(j) halt-safety retry behavior mismatch")
    echo "  ❌ (j) halt-safety retry behavior mismatch"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

# (k) an unexpected/non-retriable EXIT_STATUS hard-fails the whole script
# instead of advancing to the next chunk (VM-terminal != enumerator success).
rm -f "$CHILD_CAPTURE_FILE"
printf '7\n' > "$STUB_DIR/exit_status_queue"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
if [[ "$EXIT" == "1" ]] && grep -qE "EXIT_STATUS='7'.*aborting the whole backfill" <<<"$OUTPUT"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (k) unexpected EXIT_STATUS hard-fails instead of advancing"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(k) unexpected EXIT_STATUS did not hard-fail as expected")
    echo "  ❌ (k) unexpected EXIT_STATUS did not hard-fail as expected"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

# (l) MAX_CHUNK_ATTEMPTS exhausted (every attempt keeps hitting halt-safety) ->
# hard-fail rather than retry forever.
rm -f "$CHILD_CAPTURE_FILE"
printf '5\n5\n5\n' > "$STUB_DIR/exit_status_queue"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 MAX_CHUNK_ATTEMPTS=2 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
if [[ "$EXIT" == "1" ]] && grep -q "still hit the max-writes-per-run halt-safety after 2 attempts" <<<"$OUTPUT"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (l) MAX_CHUNK_ATTEMPTS exhausted hard-fails"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(l) MAX_CHUNK_ATTEMPTS exhaustion did not hard-fail as expected")
    echo "  ❌ (l) MAX_CHUNK_ATTEMPTS exhaustion did not hard-fail as expected"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

# (m) a failing child launcher (e.g. a real PERMISSION_DENIED) surfaces its
# output and hard-aborts instead of silently dying at the `set -e`-guarded
# assignment with zero diagnostics (the exact failure mode hit live
# 2026-08-03: a sibling slot's `gcloud config set account` clobbered the
# shared host's active identity mid-run and the pre-fix script died silently
# on the very next chunk retry).
rm -f "$CHILD_CAPTURE_FILE"
touch "$STUB_DIR/child_should_fail"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
rm -f "$STUB_DIR/child_should_fail"
if [[ "$EXIT" == "1" ]] && grep -q "simulated PERMISSION_DENIED" <<<"$OUTPUT" \
    && grep -q "child launcher exited 1 for chunk 2026-02-01..${END_BOUNDARY}" <<<"$OUTPUT"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (m) failing child launcher surfaces output + hard-aborts"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(m) failing child launcher did not surface output / hard-abort as expected")
    echo "  ❌ (m) failing child launcher did not surface output / hard-abort as expected"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

# (n) missing EXIT_STATUS + confirmed preemption (compute.instances.preempted)
# -> retry the same window, exactly like halt-safety. Live 2026-08-03: a SPOT
# VM was preempted ~2min after creation with zero logs, and the pre-fix script
# treated that as a fatal unknown failure and aborted the whole backfill.
rm -f "$CHILD_CAPTURE_FILE"
touch "$STUB_DIR/simulate_preempted"
printf 'EMPTY\n0\n' > "$STUB_DIR/exit_status_queue"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
rm -f "$STUB_DIR/simulate_preempted"
if [[ "$EXIT" == "0" ]] && grep -q "confirmed preempted" <<<"$OUTPUT" \
    && grep -q "All 1 chunks launched" <<<"$OUTPUT" \
    && [[ -f "$CHILD_CAPTURE_FILE" ]] && [[ "$(wc -l < "$CHILD_CAPTURE_FILE")" == "2" ]]; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (n) missing EXIT_STATUS + confirmed preemption retries, then succeeds"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(n) preemption retry behavior mismatch")
    echo "  ❌ (n) preemption retry behavior mismatch"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

# (o) missing EXIT_STATUS with NO preemption evidence -> hard-fail (a genuine
# unknown failure must not be silently retried/absorbed as routine).
rm -f "$CHILD_CAPTURE_FILE"
printf 'EMPTY\n' > "$STUB_DIR/exit_status_queue"
OUTPUT="$(CHILD_LAUNCHER="$STUB_DIR/child_launcher_stub.sh" POLL_INTERVAL_SECONDS=0 CHUNK_TIMEOUT_SECONDS=5 \
    bash "$LAUNCHER" sports --floor-date 2026-02-01 2>&1)"
EXIT=$?
if [[ "$EXIT" == "1" ]] && grep -q "was NOT preempted" <<<"$OUTPUT"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (o) missing EXIT_STATUS without preemption hard-fails"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(o) missing EXIT_STATUS without preemption did not hard-fail as expected")
    echo "  ❌ (o) missing EXIT_STATUS without preemption did not hard-fail as expected"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; }
fi

echo ""
if [[ "$FAIL" == "0" ]]; then
    echo "ALL TESTS PASS ($PASS/$((PASS + FAIL)))"
    exit 0
fi
echo "FAILED: $FAIL of $((PASS + FAIL))"
for c in "${FAILED_CASES[@]}"; do
    echo "  - $c"
done
exit 1
