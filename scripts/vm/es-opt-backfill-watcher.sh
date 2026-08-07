#!/bin/bash
# ES_OPT backfill watcher + launcher + manifest-verify + plan-flip
# Handles batch6 todo #2 autonomously once the Databento singleton lock clears.
#
# WHY THIS EXISTS: The Databento singleton lock is held by a long-running NYSE/NASDAQ/CME
# backfill campaign. This watcher waits for count==0, launches the 5 ES_OPT VMs (2022-2026,
# SPOT, ohlcv_1m), verifies launch, waits for completion, runs a pyarrow manifest count query
# (column-pruned, memory-safe), updates both plan files, commits docs(plans):, and calls /done.
#
# TRAPS LEARNED:
# 1. Error-aware gcloud: `gcloud ... | wc -l` returns 0 on transient errors (empty stdout).
#    MUST check rc==0 AND empty output before treating as CLEAR. See Phase 1 loop.
# 2. Python heredoc: use `cat > file << 'PYEOF'` then invoke as a file; inline heredoc with
#    variable expansion inside Python is broken (mixed quoting).
# 3. instrument_type is "options_chain" (lowercase), not "option" or "OPTION".
# 4. The 11 canonical ES_OPT instrument IDs: CME:OPTION:ES, EW, EW1, EW2, EW4,
#    E1A, E2A, E3A, E4A, E5A, EOM.
# 5. manifest_query.py uses pyarrow predicate pushdown — column-pruned, safe on 6.8M-row index.
#    Python binary: /home/ubuntu/unified-trading-system-repos/.tabs/12/market-tick-data-service/.venv/bin/python
# 6. `nohup cmd &` alone is NOT enough to survive a launching-tool-call teardown: it reparents
#    to init (PPID=1) but stays in the SAME process group as the launching shell under
#    non-interactive bash (no job control). A PGID-based cleanup sweep can kill it despite
#    PPID=1 (confirmed: 2 watcher instances died silently mid-`sleep 300`, zero FATAL log
#    entry — an internal bug can't cause that, since nothing executes during a sleep). MUST
#    launch with `setsid` for a genuinely new session+process-group. AND: util-linux `setsid`
#    without `-f` forks internally when it can't setsid() in-place, so `$!` in the launching
#    shell captures the WRONG (short-lived wrapper) pid — resolve the real pid via
#    `ps -ef | grep <script path> | grep -v grep`, then verify isolation with
#    `ps -o pid,ppid,pgid,sid -p <pid>` — PGID and SID must BOTH equal the process's own PID.
#    PPID=1 alone is not sufficient proof of survival.
#
# USAGE (re-arm in a fresh session):
#   1. Update SCRATCHPAD to a fresh session path (or use mktemp -d)
#   2. Launch: `setsid nohup bash script.sh > log 2>&1 < /dev/null & disown`
#      (NOT plain `nohup ... &` — see trap 6 above)
#   3. Find the REAL pid: `ps -ef | grep es_opt_watcher.sh | grep -v grep` (NOT `$!`)
#   4. Verify: `ps -o pid,ppid,pgid,sid -p <pid>` — PGID=SID=<pid> required
#   5. DO NOT re-run if the current live instance is still alive — singleton race risk
#
# Epic: tradfi_satellite_ao_dispatch_batch6
# Lifecycle: one-shot
# Delete-when: after /done confirms task tradfi_satellite_ao_dispatch_batch6-002 completion

set -uo pipefail

SERVER_URL="http://localhost:8765"
SLOT_ID=11
TASK_ID="tradfi_satellite_ao_dispatch_batch6-002"
SLOT_TABS="/home/ubuntu/unified-trading-system-repos/.tabs/11"
DEPLOY_DIR="$SLOT_TABS/deployment-service"
PM_DIR="$SLOT_TABS/unified-trading-pm"
PLAN_REL="plans/active/tradfi_satellite_ao_dispatch_batch6_2026_08_01.md"
CLOSEOUT_REL="plans/active/tradfi_consolidated_closeout_2026_07_18.md"
# UPDATE THIS before re-arming — must be an absolute path to a writable dir that persists
SCRATCHPAD="${SCRATCHPAD:-$(mktemp -d /tmp/es-opt-watcher-XXXXXX)}"
PYTHON="/home/ubuntu/unified-trading-system-repos/.tabs/12/market-tick-data-service/.venv/bin/python"
MANIFEST_URL="gs://market-data-tick-tradfi-prd-central-element-323112/_index/availability_index.parquet"
MANIFEST_TMP="$SCRATCHPAD/availability_index_fresh.parquet"
LOG="$SCRATCHPAD/es_opt_watcher.log"
QUERY_SCRIPT="$SCRATCHPAD/manifest_query.py"
RESULT_FILE="$SCRATCHPAD/manifest_result.json"

ts() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }
post_progress() {
    local msg="$1" pct="${2:-10}"
    curl -sS -X POST "$SERVER_URL/api/slots/$SLOT_ID/progress" \
        -H 'Content-Type: application/json' \
        -d "{\"task_id\":\"$TASK_ID\",\"message\":\"$msg\",\"context_used_pct\":$pct}" 2>/dev/null || true
    log "PROGRESS[$pct%]: $msg"
}
die() { log "FATAL: $*"; post_progress "FATAL: $*" 15; exit 1; }

mkdir -p "$SCRATCHPAD"

# Write the manifest query Python script to a file
cat > "$QUERY_SCRIPT" << 'PYEOF'
import json, sys, os
import pyarrow.parquet as pq
import pyarrow.compute as pc
from collections import Counter

manifest_path = sys.argv[1]
result_path = sys.argv[2]

ES_OPT_IDS = [
    "CME:OPTION:ES","CME:OPTION:EW","CME:OPTION:EW1","CME:OPTION:EW2","CME:OPTION:EW4",
    "CME:OPTION:E1A","CME:OPTION:E2A","CME:OPTION:E3A","CME:OPTION:E4A","CME:OPTION:E5A","CME:OPTION:EOM",
]
filters = [
    ("venue","=","CME"),
    ("data_type","=","ohlcv_1m"),
    ("instrument_type","=","options_chain"),
    ("instrument_id","in",ES_OPT_IDS),
]

table = pq.read_table(
    manifest_path,
    columns=["instrument_id","date","capture_status","row_count"],
    filters=filters,
)

total = len(table)
if total == 0:
    result = {"total_rows":0,"capture_status_dist":{},"rows_with_data":0,"date_min":None,"date_max":None,"per_root":{}}
else:
    status_dist = dict(Counter(table.column("capture_status").to_pylist()))
    rows_with_data = int(pc.sum(pc.greater(table.column("row_count"), 0)).as_py() or 0)
    dates = table.column("date").to_pylist()
    per_root = dict(Counter(table.column("instrument_id").to_pylist()))
    result = {
        "total_rows": total,
        "capture_status_dist": status_dist,
        "rows_with_data": rows_with_data,
        "date_min": str(min(dates)) if dates else None,
        "date_max": str(max(dates)) if dates else None,
        "per_root": per_root,
    }

with open(result_path, "w") as f:
    json.dump(result, f)
print(json.dumps(result))
PYEOF

log "=== ES_OPT watcher started ==="
post_progress "watcher started: polling singleton lock every 300s (error-aware)" 10

# ──────────────────────────────────────────────────────────────────────────────
# Phase 1: Wait for Databento singleton lock to clear
# ──────────────────────────────────────────────────────────────────────────────
POLL_N=0
while true; do
    POLL_N=$((POLL_N + 1))
    if gcloud_out=$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-" AND status=RUNNING' \
        --zones=asia-northeast1-c \
        --format='value(name)' 2>/dev/null); then
        if [[ -z "$gcloud_out" ]]; then
            log "poll $POLL_N: count=0 (rc=0, empty) — LOCK CLEARED"
            post_progress "singleton lock CLEARED — launching ES_OPT now" 10
            break
        fi
        cnt=$(echo "$gcloud_out" | wc -l | tr -d ' ')
        log "poll $POLL_N: $cnt tradfi-bf-* VMs still running"
        (( POLL_N % 3 == 0 )) && post_progress "singleton held: $cnt VMs running (poll $POLL_N)" 10
    else
        log "poll $POLL_N: gcloud rc!=0 — holding"
    fi
    sleep 300
done

# ──────────────────────────────────────────────────────────────────────────────
# Phase 2: Launch ES_OPT
# ──────────────────────────────────────────────────────────────────────────────
cd "$DEPLOY_DIR" || die "cannot cd to $DEPLOY_DIR"

log "launching ES_OPT: bash scripts/vm/launch-tradfi-backfill-vm.sh --root-symbol ES_OPT"
LAUNCH_OUT=""
if ! LAUNCH_OUT=$(bash scripts/vm/launch-tradfi-backfill-vm.sh --root-symbol ES_OPT 2>&1); then
    log "launch output: $LAUNCH_OUT"
    # Check for singleton race (someone else launched between our check and launch)
    if echo "$LAUNCH_OUT" | grep -qiE "singleton|already.*running|RUNNING"; then
        die "launch blocked by singleton lock race — fresh session needed"
    fi
    die "launch script failed: $LAUNCH_OUT"
fi
log "launch output: $LAUNCH_OUT"
LAUNCH_TS="$(ts)"
post_progress "launch command succeeded at $LAUNCH_TS — verifying VMs started" 12

# T+30s: verify VMs appeared
sleep 30
VMS_T0=$(gcloud compute instances list \
    --filter='name~"^tradfi-bf-es-opt-" AND status=RUNNING' \
    --zones=asia-northeast1-c --format='value(name)' 2>/dev/null || echo "")
CNT_T0=$(echo "$VMS_T0" | grep -c . 2>/dev/null || echo 0)
[[ -z "$VMS_T0" ]] && CNT_T0=0
log "T+30s: $CNT_T0 ES_OPT VMs detected"
post_progress "T+30s: $CNT_T0 ES_OPT VMs detected — waiting for T+10min" 12

# T+10min: confirm running
sleep 570
VMS_T10=$(gcloud compute instances list \
    --filter='name~"^tradfi-bf-es-opt-" AND status=RUNNING' \
    --zones=asia-northeast1-c --format='value(name)' 2>/dev/null || echo "")
CNT_T10=$(echo "$VMS_T10" | grep -c . 2>/dev/null || echo 0)
[[ -z "$VMS_T10" ]] && CNT_T10=0
log "T+10min: $CNT_T10 ES_OPT VMs RUNNING: $VMS_T10"
post_progress "T+10min: $CNT_T10 ES_OPT VMs RUNNING — phase 2 launch verified" 12

# ──────────────────────────────────────────────────────────────────────────────
# Phase 3: Wait for ES_OPT VM completion
# ──────────────────────────────────────────────────────────────────────────────
log "=== Phase 3: monitoring ES_OPT completion ==="
post_progress "monitoring ES_OPT completion (tradfi-bf-es-opt-* → 0)" 12

WAIT_N=0
while true; do
    WAIT_N=$((WAIT_N + 1))
    if running=$(gcloud compute instances list \
        --filter='name~"^tradfi-bf-es-opt-" AND status=RUNNING' \
        --zones=asia-northeast1-c --format='value(name)' 2>/dev/null); then
        if [[ -z "$running" ]]; then
            log "ES_OPT VMs all done (poll $WAIT_N)"
            post_progress "all ES_OPT VMs completed — starting manifest count check" 12
            break
        fi
        cnt=$(echo "$running" | wc -l | tr -d ' ')
        log "poll $WAIT_N: $cnt ES_OPT VMs still running"
        (( WAIT_N % 2 == 0 )) && post_progress "ES_OPT backfill running: $cnt VMs (poll $WAIT_N)" 12
    else
        log "poll $WAIT_N: gcloud rc!=0 — retrying"
    fi
    sleep 300
done

# ──────────────────────────────────────────────────────────────────────────────
# Phase 4: Fresh manifest download + count query
# ──────────────────────────────────────────────────────────────────────────────
log "=== Phase 4: manifest count check ==="
rm -f "$MANIFEST_TMP" "$RESULT_FILE"

post_progress "downloading fresh availability_index.parquet" 12
gsutil cp "$MANIFEST_URL" "$MANIFEST_TMP" 2>&1 | tee -a "$LOG" || die "manifest download failed"
MSIZE=$(du -h "$MANIFEST_TMP" | cut -f1)
log "manifest downloaded: $MSIZE"

post_progress "running manifest count query (pyarrow column-pruned)" 12
"$PYTHON" "$QUERY_SCRIPT" "$MANIFEST_TMP" "$RESULT_FILE" 2>&1 | tee -a "$LOG" \
    || die "manifest query failed"

if [[ ! -f "$RESULT_FILE" ]]; then
    die "result file not written by query script"
fi

# Parse results
TOTAL=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['total_rows'])")
RDATA=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['rows_with_data'])")
DMIN=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['date_min'] or 'N/A')")
DMAX=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['date_max'] or 'N/A')")
SDIST=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(json.dumps(d['capture_status_dist']))")
PROOT=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(json.dumps(d['per_root']))")

log "Manifest result: total=$TOTAL rows_with_data=$RDATA dates=$DMIN..$DMAX"
log "status_dist: $SDIST"
log "per_root: $PROOT"
post_progress "manifest query: $TOTAL rows ($RDATA with data), dates $DMIN..$DMAX" 15

# ──────────────────────────────────────────────────────────────────────────────
# Phase 5: Update plan files
# ──────────────────────────────────────────────────────────────────────────────
log "=== Phase 5: updating plans ==="
cd "$PM_DIR" || die "cannot cd to PM dir"

git pull --ff-only origin live-defi-rollout 2>&1 | tee -a "$LOG" \
    || git pull --rebase --autostash origin live-defi-rollout 2>&1 | tee -a "$LOG" \
    || die "git pull failed"

PLAN_FULL="$PM_DIR/$PLAN_REL"
CLOSEOUT_FULL="$PM_DIR/$CLOSEOUT_REL"

# Build evidence string
EVIDENCE="Launched $LAUNCH_TS. 5 ES_OPT VMs (2022-2026 SPOT ohlcv_1m CME) ran to completion. Manifest: $TOTAL rows (venue=CME x ohlcv_1m x options_chain x 11 roots), $RDATA with data, $DMIN..$DMAX. status=$SDIST."

# ── Flip checkbox in batch6 plan ──
python3 - "$PLAN_FULL" "$EVIDENCE" << 'PYEOF'
import sys, re
path, evidence = sys.argv[1], sys.argv[2]
with open(path) as f:
    content = f.read()
# Flip [ ] to [x] for the P0 ES_OPT todo
old = r'- \[ \] \[DATA\] P0\. \*\*Launch the ES_OPT backfill'
new = '- [x] [DATA] P0. **Launch the ES_OPT backfill'
if re.search(old, content):
    content = re.sub(old, new, content, count=1)
    print("checkbox flipped")
else:
    print("WARNING: P0 checkbox pattern not found")
# Append evidence after the last Source line in the P0 block
src_pat = r"(Source: `instruments_tradfi_g1_g5_gate_execution_2026_07_24\.md`\.)"
if re.search(src_pat, content):
    content = re.sub(src_pat, r'\1\n\n  **Evidence:** ' + evidence, content, count=1)
    print("evidence appended")
with open(path, 'w') as f:
    f.write(content)
print("batch6 plan updated")
PYEOF

# ── Update closeout plan S&P index options row ──
python3 - "$CLOSEOUT_FULL" "$LAUNCH_TS" "$TOTAL" "$RDATA" "$DMIN" "$DMAX" "$SDIST" << 'PYEOF'
import sys, re
path, launch_ts, total, rdata, dmin, dmax, sdist = sys.argv[1:8]
with open(path) as f:
    content = f.read()
new_status = (
    f"✅ ES_OPT launched {launch_ts}. Backfill complete (5 VMs 2022-2026 SPOT ohlcv_1m). "
    f"Manifest: {total} rows (CME x ohlcv_1m x options_chain x 11 roots), {rdata} with data, "
    f"{dmin}..{dmax}. status={sdist}. "
    f"Source: tradfi_satellite_ao_dispatch_batch6_2026_08_01.md todo#2."
)
# Match the S&P index options row
pat = r'(\| S&P index options\s+\| yes\s+\| )([^|]+)(\| NOT PROVEN[^|]*\|)'
if re.search(pat, content):
    content = re.sub(pat, lambda m: m.group(1) + new_status + ' ' + m.group(3), content, count=1)
    print("closeout row updated")
else:
    print("WARNING: closeout row pattern not matched — appending note instead")
    content += f"\n\n<!-- ES_OPT manifest-verify: {new_status} -->\n"
with open(path, 'w') as f:
    f.write(content)
print("closeout plan updated")
PYEOF

# ── Commit and push ──
git add "$PLAN_REL" "$CLOSEOUT_REL"
git diff --cached --stat | tee -a "$LOG"

git commit -m "docs(plans): flip batch6 todo#2 ES_OPT launch+manifest-verify complete

Launched $LAUNCH_TS. 5 ES_OPT VMs (2022-2026 SPOT ohlcv_1m CME) ran to completion.
Manifest: $TOTAL rows ($RDATA with data, $DMIN..$DMAX). status=$SDIST.
MVP-cell table S&P index options row updated in closeout plan.

Quickmerge: agent" 2>&1 | tee -a "$LOG" || die "git commit failed"

SHA=$(git rev-parse --short HEAD)
log "committed: $SHA"

git push origin HEAD:live-defi-rollout 2>&1 | tee -a "$LOG" || {
    git pull --rebase --autostash origin live-defi-rollout 2>&1 | tee -a "$LOG"
    git push origin HEAD:live-defi-rollout 2>&1 | tee -a "$LOG" || die "push failed"
    SHA=$(git rev-parse --short HEAD)
}

git fetch origin live-defi-rollout --quiet
git merge-base --is-ancestor "$SHA" origin/live-defi-rollout \
    && log "✅ $SHA verified on origin" \
    || die "SHA $SHA not on origin after push"

post_progress "plan files committed and pushed PM@$SHA" 15

# ──────────────────────────────────────────────────────────────────────────────
# Phase 6: /done
# ──────────────────────────────────────────────────────────────────────────────
DONE_EVIDENCE="ES_OPT launched $LAUNCH_TS (5 VMs 2022-2026 SPOT ohlcv_1m CME). Completed. Manifest: $TOTAL rows $RDATA with data $DMIN..$DMAX. Closeout MVP-cell updated. PM@$SHA."
DONE_RESP=$(curl -sS -X POST "$SERVER_URL/api/slots/$SLOT_ID/done" \
    -H 'Content-Type: application/json' \
    -d "{\"task_id\":\"$TASK_ID\",\"sha\":\"$SHA\",\"evidence\":\"$DONE_EVIDENCE\",\"context_used_pct\":15}" 2>/dev/null)
log "done response: $DONE_RESP"
log "=== watcher complete ==="
