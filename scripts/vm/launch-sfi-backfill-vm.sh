#!/usr/bin/env bash
# Launch a GCE VM that backfills SoccerFootball.info (SFI / SharpAPI) reference
# data — leagues + progressive stats — for a multi-year historical range.
# Sibling to `launch-sfi-forward-poll.sh` (which is T-1 / forward-poll only).
#
# **SFI has NO standings endpoint** — confirmed in
# `instruments-service/instruments_service/engine/orchestrator.py` L4365-4367
# (`_want_sfi_standings = False`). Pre-match league position comes from
# API-Football `STANDINGS`; do not pass `--entity SFI_STANDINGS` here, the
# instruments-service path will skip it.
#
# Cadence + publication rules: unified-trading-pm/codex/02-data/
# sports-scheduling-and-sharding.md §2.4 (Tier-1 every 6h for SFI_LEAGUES;
# Tier-4 T+24h for SFI_PROGRESSIVE_STATS — needs completed-matchday state).
#
# Single-VM mode
# --------------
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider SOCCER_FOOTBALL_INFO \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#   (or --sports-entity SFI_LEAGUES | SFI_PROGRESSIVE_STATS to scope)
#
# Chunk-parallel mode (`--chunks N`)
# ----------------------------------
#
# Plan `sfi_chunk_parallel_backfill_2026_04_22` Option 2 (independent chunks):
# split the date range into N disjoint sub-ranges and fire N sibling VMs,
# each running the same single-VM command on its slice. All siblings share a
# `run-id=<ts>` label so the singleton lock treats them as one logical job.
#
# Per-date writes land directly in canonical ``by_date/day={D}/entity={E}/``
# shards — chunks are naturally disjoint on date, no coordinator merge needed
# for the data parquets. The availability-index ``_index/availability_index.parquet``
# may see racey appends at chunk edges; run ``launch-sports-manifest-rescan-vm.sh``
# after all chunks complete to materialise empty_confirmed rows.
#
# Worker rate-limit budget: each chunk maintains its own 429 back-off state
# independently (Option 2). Worst-case wall-clock ≈ single-VM / N (measured
# 2026-04-22: ~1.4 dates/hour single-VM → ~17 days for 2300-day range with
# N=4 vs ~68 days single-VM).
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - `soccer-football-info-api-key` in Secret Manager
#
# Usage:
#   bash launch-sfi-backfill-vm.sh 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --entity SFI_LEAGUES 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --entity SFI_PROGRESSIVE_STATS 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --force 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --chunks 4 2020-01-01 2026-04-21
#   bash launch-sfi-backfill-vm.sh --dry-run --chunks 4 2020-01-01 2026-04-21
#
# --entity SFI_LEAGUES | SFI_PROGRESSIVE_STATS
#   Restricts the VM to a single manifest entity. Omit for all (default).
#
# --chunks N (N >= 2, N <= 4 recommended)
#   Fire N sibling VMs with disjoint date ranges + shared run-id label.
#   Without this flag the launcher runs in single-VM mode.
#
# --dry-run
#   Print the chunk plan + VM command without launching.
#
# Singleton lock: refuses to launch if ANY sfi-* VM is already running in the
# zone — UNLESS it's a sibling of the same `run-id` label, in which case the
# chunk fan-out is allowed (Plan 5 pattern). Pass --force to bypass the lock
# entirely for legitimate parallel investigations.
#
# Shared `soccer-football-info-api-key` rate limits: 10 concurrent SFI VMs
# thrashed on 429 during the 2026-04-19 incident — reference: codex §2.4 +
# singleton-locked-launchers memory.
set -euo pipefail

FORCE=false
ENTITY=""
CHUNKS=""
DRY_RUN=false
RECOVERY_FIXTURE_IDS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --chunks) CHUNKS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    *) break ;;
  esac
done

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
Usage: bash launch-sfi-backfill-vm.sh [--force] [--entity ENTITY] [--chunks N] [--dry-run] <START_DATE> <END_DATE>

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  ENTITY (optional): SFI_LEAGUES | SFI_PROGRESSIVE_STATS (default: both).
  --chunks N (optional, N >= 2): split range into N disjoint chunks + fire
            N sibling VMs with shared run-id. Target N=4 for 6.3-year range.
  --dry-run: print plan without launching.
  --force: bypass the sfi-* singleton lock (rate-limit hazard).

Examples:
  bash launch-sfi-backfill-vm.sh 2020-01-01 2026-04-21
  bash launch-sfi-backfill-vm.sh --entity SFI_LEAGUES 2020-01-01 2026-04-21
  bash launch-sfi-backfill-vm.sh --chunks 4 2020-01-01 2026-04-21
  bash launch-sfi-backfill-vm.sh --dry-run --chunks 4 2020-01-01 2026-04-21
EOF
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
  exit 1
fi

if [[ -n "$ENTITY" && "$ENTITY" != "SFI_LEAGUES" && "$ENTITY" != "SFI_PROGRESSIVE_STATS" ]]; then
  echo "ERROR: --entity must be SFI_LEAGUES or SFI_PROGRESSIVE_STATS (got $ENTITY)" >&2
  echo "       Note: SFI has no standings endpoint — do not pass SFI_STANDINGS." >&2
  exit 1
fi

if [[ -n "$CHUNKS" ]]; then
  if ! [[ "$CHUNKS" =~ ^[0-9]+$ ]] || [[ "$CHUNKS" -lt 2 ]]; then
    echo "ERROR: --chunks must be an integer >= 2 (use single-VM mode for N=1)" >&2
    exit 1
  fi
  if [[ "$CHUNKS" -gt 4 ]]; then
    cat >&2 <<EOF
WARNING: --chunks $CHUNKS > 4 — recommended ceiling is 4 for SFI.

  Rationale: the shared soccer-football-info-api-key per-minute quota is
  ~14 calls/min (measured 2026-04-22). At N=4 each chunk averages ~3.5
  calls/min sustained, roughly at the per-key floor. N > 4 causes 429
  thrash without a shared rate-limit coordinator (Plan 5 Option 1, TBD).
  Proceed with --force if you have a specific reason.
EOF
    if ! $FORCE; then
      echo "ERROR: use --force to override the N<=4 recommendation." >&2
      exit 1
    fi
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# --- Singleton lock (sibling-aware when --chunks is used) ---
RUN_ID=""
if ! $FORCE; then
  RUN_ID="$(date +%Y%m%d-%H%M%S)"
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sfi-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name,labels.run-id)' 2>/dev/null | head -20)"
  if [[ -n "$EXISTING" ]]; then
    # With --chunks we only accept same-run-id siblings (launcher is single-shot,
    # so any sibling match means the user is adding chunks mid-fire which we
    # refuse). Without --chunks, any running SFI VM blocks.
    NAME="$(echo "$EXISTING" | head -1 | awk '{print $1}')"
    EXISTING_RUN_ID="$(echo "$EXISTING" | head -1 | awk '{print $2}')"
    cat >&2 <<EOF
ERROR: SFI VM already running in $ZONE: $NAME (run-id=${EXISTING_RUN_ID:-<none>})
Refusing duplicate launch — shared API key rate limits
(reference: 2026-04-19 thundering-herd incident).

Options:
  Inspect:   gcloud compute ssh $NAME --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${NAME}/run.log
  Stop:      gcloud compute instances delete $NAME --zone=$ZONE --quiet
  Force:     bash $0 --force ...
EOF
    exit 1
  fi
fi

# --- Helper: launch a single VM with the provided date range + chunk label ---
launch_one_vm() {
  local vm_name="$1"
  local start_date="$2"
  local end_date="$3"
  local run_id="$4"
  local chunk_id="${5:-}"

  local entity_desc="all SFI entities"
  [[ -n "$ENTITY" ]] && entity_desc="entity=$ENTITY only"

  local metadata="VM_TASK=sports-backfill"
  metadata="${metadata},VM_SERVICE=instruments_service"
  metadata="${metadata},VM_OPERATION=instruments"
  metadata="${metadata},VM_ASSET_GROUP=SPORTS"
  metadata="${metadata},VM_START_DATE=${start_date}"
  metadata="${metadata},VM_END_DATE=${end_date}"
  metadata="${metadata},VM_SPORTS_PROVIDER=SOCCER_FOOTBALL_INFO"
  [[ -n "$ENTITY" ]] && metadata="${metadata},VM_SPORTS_ENTITY=${ENTITY}"
  [[ -n "$RECOVERY_FIXTURE_IDS" ]] && metadata="${metadata},VM_RECOVERY_FIXTURE_IDS=${RECOVERY_FIXTURE_IDS}"
  $FORCE && metadata="${metadata},VM_FORCE=true"
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"

  local labels="purpose=sfi-backfill,run-id=${run_id}"
  [[ -n "$chunk_id" ]] && labels="${labels},chunk=${chunk_id}"

  if $DRY_RUN; then
    echo "[dry-run] ${vm_name}: ${start_date}..${end_date} (${entity_desc}) run-id=${run_id} chunk=${chunk_id:-single}"
    return 0
  fi

  echo "Launching $vm_name: SOCCER_FOOTBALL_INFO backfill ${start_date}..${end_date} (${entity_desc}) chunk=${chunk_id:-single}"

  gcloud compute instances create "$vm_name" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-2 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${metadata}" \
    --labels="$labels"
}

# --- Chunked fan-out (Option 2: independent chunks) ---
if [[ -n "$CHUNKS" ]]; then
  if [[ -z "$RUN_ID" ]]; then
    RUN_ID="$(date +%Y%m%d-%H%M%S)"
  fi
  echo "Chunked SFI backfill: N=${CHUNKS} | run-id=${RUN_ID} | range=[${START_DATE}..${END_DATE}]"

  # Inline Python splitter — mirror launch-sports-manifest-rescan-vm.sh shape
  # (front-loaded remainder, bash 3.2 compatible: no readarray).
  CHUNK_OUTPUT="$(python3 - "$START_DATE" "$END_DATE" "$CHUNKS" <<'PY'
import sys
from datetime import date, timedelta

d0 = date.fromisoformat(sys.argv[1])
d1 = date.fromisoformat(sys.argv[2])
chunks = int(sys.argv[3])
total = (d1 - d0).days + 1
if chunks > total:
    chunks = total
size = total // chunks
rem = total % chunks
cursor = d0
for i in range(chunks):
    span = size + (1 if i < rem else 0)
    cs = cursor
    ce = cursor + timedelta(days=span - 1)
    print(f"{cs.isoformat()}\t{ce.isoformat()}")
    cursor = ce + timedelta(days=1)
PY
)"
  CHUNK_LINES=()
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    CHUNK_LINES+=("$line")
  done <<< "$CHUNK_OUTPUT"

  echo "Chunk boundaries (${#CHUNK_LINES[@]}):"
  for line in "${CHUNK_LINES[@]}"; do
    echo "  ${line}"
  done
  echo ""

  i=0
  total_chunks=${#CHUNK_LINES[@]}
  for line in "${CHUNK_LINES[@]}"; do
    i=$((i + 1))
    IFS=$'\t' read -r cs ce <<< "$line"
    CHUNK_ID="${i}-of-${total_chunks}"
    VM_NAME="sfi-backfill-chunk-${i}of${total_chunks}-${RUN_ID}"
    launch_one_vm "$VM_NAME" "$cs" "$ce" "$RUN_ID" "$CHUNK_ID"
  done

  echo ""
  echo "Launched ${total_chunks} SFI chunk VMs with run-id=${RUN_ID}"
  if $DRY_RUN; then
    echo "[dry-run] No VMs were actually created."
  fi
  echo ""
  echo "Poll for completion:"
  echo "  gcloud compute instances list --filter='labels.run-id=${RUN_ID} AND status=RUNNING' --zones=${ZONE}"
  echo ""
  echo "After all chunks finish, materialise empty_confirmed manifest rows:"
  echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
  exit 0
fi

# --- Single-VM mode (default) ---
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
VM_NAME="sfi-backfill-${RUN_ID}"
launch_one_vm "$VM_NAME" "$START_DATE" "$END_DATE" "$RUN_ID" ""

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
