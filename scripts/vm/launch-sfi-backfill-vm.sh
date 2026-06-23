#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# Machine type override (default e2-standard-2). The per-league skip-check used
# to re-read the 6.5 GB sports availability index, OOM-killing e2-standard-2;
# fixed in instruments-service@505dcd9 (single index read). e2-standard-8
# (32 GB) is the safe default for large full-range sports backfills.
# SSOT: plans/active/sports_reference_backfill_oom_2026_06_22.md
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

FORCE=false
ENTITY=""
CHUNKS=""
DRY_RUN=false
RECOVERY_FIXTURE_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --chunks) CHUNKS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

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

if [[ -n "$CHUNKS" && "$CHUNKS" != "1" ]]; then
  # Chunk-parallelism is BANNED for SFI: the soccer-football-info RapidAPI key's
  # rate limit (4 req/s, max tier 6, 100k/day — operator 2026-06-19) is
  # PER-ACCOUNT, not per-VM. Running N chunks against the ONE shared key multiplies
  # the request rate by N (each chunk self-paces independently) → 4×2.94≈11.8 req/s
  # vs the 4/s account cap → constant 429 collisions → 60s back-off sleeps →
  # aggregate throughput WORSE than a single clean stream (incident 2026-06-19:
  # 4 chunks each 429-thrashing). A single stream at <4/s saturates the 100k/day
  # ceiling (the true binding constraint) in ~9.4h with zero collisions.
  # The sfi_chunk_parallel_backfill_2026_04_22 premise (independent per-chunk rate
  # budgets) is INVALID for a shared key and is superseded.
  cat >&2 <<EOF
ERROR: --chunks is not supported for SFI (got --chunks $CHUNKS).
  The soccer-football-info RapidAPI key rate limit is PER-ACCOUNT (4 req/s, 100k/day),
  so N chunks sharing the one key multiply the rate by N → 429 collision storms that
  make throughput WORSE, not better. Run a SINGLE stream instead (omit --chunks):
      bash $0 ${FORCE:+--force }<START_DATE> <END_DATE>
  One stream paces under 4/s and saturates the 100k/day ceiling cleanly. To genuinely
  parallelize you would need a SECOND RapidAPI account/key + a shared-budget coordinator.
EOF
  exit 1
fi
CHUNKS=""  # force single-stream mode

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

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
  metadata="${metadata},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"
  # Per-shard progress watchdog (backfill_vm_silent_worker_stall_watchdog P1): `league` appears in
  # every per-date line ("league mapping cache hit for date=…" + "Fetched N leagues"), so the stall
  # timer resets each date the worker advances — an empty-match date still resets it, but the
  # 2026-06-19 hang (frozen mid-fetch, no new line) trips fast. Verified vs a live SFI run.log;
  # =/space/comma-free (metadata-safe). SFI raises STALL_TIMEOUT_SEC for empty-date gaps; this lets
  # that stay tight without false-killing.
  metadata="${metadata},STALL_PROGRESS_REGEX=league"

  local labels="purpose=sfi-backfill,env=${DEPLOYMENT_ENV},run-id=${run_id}"
  [[ -n "$chunk_id" ]] && labels="${labels},chunk=${chunk_id}"

  if $DRY_RUN; then
    echo "[dry-run] ${vm_name}: ${start_date}..${end_date} (${entity_desc}) run-id=${run_id} chunk=${chunk_id:-single}"
    return 0
  fi

  echo "Launching $vm_name: SOCCER_FOOTBALL_INFO backfill ${start_date}..${end_date} (${entity_desc}) chunk=${chunk_id:-single}"

  gcloud compute instances create "$vm_name" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
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
