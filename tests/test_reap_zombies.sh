#!/usr/bin/env bash
# Shell test harness for reap-zombies.sh — regression guard for the wrong
# GCS log-path bug (queried logs/ instead of the canonical vm-logs/, which
# made EVERY healthy VM read as "no run.log" and get reaped past the
# silence-threshold purely on age).
#
# Closes plans/active/issues/reap_zombies_wrong_log_path_kills_healthy_vms_2026_08_03.md
# todo 1.
#
# Test cases:
#   (a) healthy VM (recent run.log, no terminal rc=) at the CANONICAL
#       vm-logs/<instance>/run.log path is read correctly and skipped, not
#       reaped.
#   (b) regression guard — the script queries exactly .../vm-logs/<instance>/
#       run.log, never the old broken .../logs/<instance>/run.log.
#   (c) a VM whose run.log carries a terminal rc= line is reaped (delete is
#       invoked).
#   (d) --dry-run never calls delete even when a zombie is detected.
#
# Usage:
#   bash tests/test_reap_zombies.sh
#   bash tests/test_reap_zombies.sh -v   # verbose
#
# Exits 0 on all-pass, 1 on any-fail.
#
# Execution ownership (Runbook SSOT):
#   execution:
#     owner: infra craft (one-shot per plan close-out); CI in future
#     cadence: pre-deploy (wire into deployment-service quality-gates.sh)
#     verifier: stdout contains "ALL TESTS PASS" on success
#     last_executed: 2026-08-03

set -u

VERBOSE=false
if [[ "${1:-}" == "-v" ]]; then
    VERBOSE=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REAPER="${SCRIPT_DIR}/../scripts/vm/reap-zombies.sh"

if [[ ! -f "$REAPER" ]]; then
    echo "FATAL: reaper not found at $REAPER" >&2
    exit 1
fi

STUB_DIR="$(mktemp -d)"
trap 'rm -rf "$STUB_DIR"' EXIT

INSTANCE="backfill-defi-dex-swaps-20260803-092530"
PROJECT="test-project"
ZONE="asia-northeast1-c"
BUCKET="deployment-scripts-test-project"

CAT_PATH_CAPTURE="$STUB_DIR/cat_paths.log"
DELETE_CAPTURE="$STUB_DIR/delete_args.log"
: > "$CAT_PATH_CAPTURE"
: > "$DELETE_CAPTURE"

# gcloud stub — routes compute-instances {list,describe,delete} and
# storage {cat, objects describe} per the exact calls reap-zombies.sh makes.
cat > "$STUB_DIR/gcloud" <<STUB
#!/usr/bin/env bash
CAT_PATH_CAPTURE="$CAT_PATH_CAPTURE"
DELETE_CAPTURE="$DELETE_CAPTURE"
case "\${1:-}-\${2:-}" in
    compute-instances)
        case "\${3:-}" in
            list)
                echo "\${STUB_INSTANCES_LIST_OUT:-}"
                exit 0
                ;;
            describe)
                echo "\${STUB_CREATION_TS:-2026-08-03T09:00:00Z}"
                exit 0
                ;;
            delete)
                shift 3
                printf 'DELETE_ARGS=%q\n' "\$*" >> "\$DELETE_CAPTURE"
                exit 0
                ;;
            *) exit 0 ;;
        esac
        ;;
    storage-cat)
        shift 2
        QPATH="\$1"
        printf '%s\n' "\$QPATH" >> "\$CAT_PATH_CAPTURE"
        # Simulate real GCS: only the canonical vm-logs/ object actually
        # exists — any other path (e.g. the old buggy logs/ path) reads as
        # empty, matching the incident's confirmed "no objects matched".
        if [[ "\$QPATH" == *"/vm-logs/"* && -n "\${STUB_CAT_OUTPUT:-}" ]]; then
            printf '%s\n' "\$STUB_CAT_OUTPUT"
        fi
        exit 0
        ;;
    storage-objects)
        case "\${3:-}" in
            describe)
                QPATH="\${4:-}"
                if [[ "\$QPATH" == *"/vm-logs/"* ]]; then
                    echo "\${STUB_UPDATE_TIME:-2026-08-03T10:29:30Z}"
                fi
                exit 0
                ;;
            *) exit 0 ;;
        esac
        ;;
    *) exit 0 ;;
esac
STUB
chmod +x "$STUB_DIR/gcloud"

export PATH="$STUB_DIR:$PATH"

PASS=0
FAIL=0
FAILED_CASES=()

_check() {
    local name="$1" ok="$2"
    if [[ "$ok" == "true" ]]; then
        PASS=$((PASS + 1))
        $VERBOSE && echo "  ✅ $name"
    else
        FAIL=$((FAIL + 1))
        FAILED_CASES+=("$name")
        echo "  ❌ $name"
    fi
}

echo "Running reap-zombies.sh shell tests..."

# (a) + (b) healthy VM at the canonical vm-logs/ path — skipped, not reaped;
#     and the script queried exactly that path (not the old logs/ path).
: > "$CAT_PATH_CAPTURE"
: > "$DELETE_CAPTURE"
NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
OUTPUT="$(
    STUB_INSTANCES_LIST_OUT="$INSTANCE" \
    STUB_CAT_OUTPUT="PIPELINE_HEARTBEAT day=2023-01-12 source=vm-life-emitter" \
    STUB_UPDATE_TIME="$NOW_ISO" \
    bash "$REAPER" --project "$PROJECT" --zone "$ZONE" --bucket "$BUCKET" 2>&1
)"
EXIT=$?
set -e

_check "(a) healthy VM (recent, no rc=) → skip, exit 0" \
    "$([[ "$EXIT" == "0" && "$OUTPUT" == *"skip $INSTANCE — healthy"* && ! -s "$DELETE_CAPTURE" ]] && echo true || echo false)"

QUERIED_PATH="$(cat "$CAT_PATH_CAPTURE" 2>/dev/null || true)"
_check "(b) regression guard — queried canonical vm-logs/ path, not logs/" \
    "$([[ "$QUERIED_PATH" == "gs://${BUCKET}/vm-logs/${INSTANCE}/run.log" ]] && echo true || echo false)"

# (c) VM whose run.log carries a terminal rc= line → reaped (delete invoked).
: > "$CAT_PATH_CAPTURE"
: > "$DELETE_CAPTURE"
set +e
OUTPUT="$(
    STUB_INSTANCES_LIST_OUT="$INSTANCE" \
    STUB_CAT_OUTPUT=$'PIPELINE_HEARTBEAT day=2023-01-12\nDEPLOYMENT_COMPLETED rc=0' \
    bash "$REAPER" --project "$PROJECT" --zone "$ZONE" --bucket "$BUCKET" 2>&1
)"
EXIT=$?
set -e

_check "(c) terminal rc= present → reaped, delete invoked" \
    "$([[ "$EXIT" == "0" && "$OUTPUT" == *"REAP $INSTANCE"* && -s "$DELETE_CAPTURE" ]] && echo true || echo false)"

# (d) --dry-run never calls delete even for a detected zombie.
: > "$CAT_PATH_CAPTURE"
: > "$DELETE_CAPTURE"
set +e
OUTPUT="$(
    STUB_INSTANCES_LIST_OUT="$INSTANCE" \
    STUB_CAT_OUTPUT=$'DEPLOYMENT_COMPLETED rc=1' \
    bash "$REAPER" --project "$PROJECT" --zone "$ZONE" --bucket "$BUCKET" --dry-run 2>&1
)"
EXIT=$?
set -e

_check "(d) --dry-run detects zombie but never calls delete" \
    "$([[ "$EXIT" == "0" && "$OUTPUT" == *"(dry-run) would run"* && ! -s "$DELETE_CAPTURE" ]] && echo true || echo false)"

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
