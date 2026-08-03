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

STUB_DIR="$(mktemp -d)"
trap 'rm -rf "$STUB_DIR"' EXIT

# gcloud stub: "instances describe" always reports TERMINATED so the wait loop
# exits immediately.
cat > "$STUB_DIR/gcloud" <<'STUB'
#!/usr/bin/env bash
case "${1:-}-${2:-}-${3:-}" in
    compute-instances-describe) echo "TERMINATED"; exit 0 ;;
    *) exit 0 ;;
esac
STUB
chmod +x "$STUB_DIR/gcloud"

# Child-launcher stub: mimics launch-expected-universe-v2-vm.sh's stdout
# contract ("VM launched: <name>") and echoes the ENUM_START_DATE/END_DATE it
# was called with (captured to a file for assertions).
CHILD_CAPTURE_FILE="$STUB_DIR/child_calls.log"
cat > "$STUB_DIR/child_launcher_stub.sh" <<STUB
#!/usr/bin/env bash
echo "\${ENUM_START_DATE:-}|\${ENUM_END_DATE:-}|\$*" >> "${CHILD_CAPTURE_FILE}"
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
    && grep -q "^2026-01-01|2026-04-04|" "$CHILD_CAPTURE_FILE"; then
    PASS=$((PASS + 1))
    $VERBOSE && echo "  ✅ (i) sequential launch reaches child launcher with correct per-chunk dates"
else
    FAIL=$((FAIL + 1))
    FAILED_CASES+=("(i) sequential launch chunk dates/count mismatch")
    echo "  ❌ (i) sequential launch chunk dates/count mismatch"
    $VERBOSE && { echo "$OUTPUT" | head -30 | sed 's/^/     /'; sed 's/^/     capture: /' "$CHILD_CAPTURE_FILE" 2>/dev/null; }
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
