#!/usr/bin/env bash
# Launch GCE VMs that rescan the SPORTS FIXTURES availability manifest with
# canonical-league mapping.
#
# Purpose: close the Phase 5 FIXTURES per-league undercount (EPL=5 rows / 6
# seasons, BRASILEIRAO=2, SERIE_A=15) by re-reading each
# ``sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet`` on GCS,
# joining the ``af_league_id`` numeric column to canonical ``league_id`` via
# UAC ``get_league_by_api_football_id``, grouping by canonical league, and
# emitting one v5 manifest row per ``(date, league_id)`` to
# ``gs://instruments-store-sports-central-element-323112/_index/availability_index.parquet``.
#
# Runs the rescan script committed in
# ``instruments-service/scripts/rescan_sports_fixtures_canonical.py`` —
# dispatched to a GCE VM per ``feedback_migrations_backfills_run_on_vm_production_software``
# (never laptop-executed over the multi-year window).
#
# Three launch shapes (see ``codex/02-data/chunk-safe-manifest-migrations.md``):
#
#   1) Single VM (default) — one VM writes the canonical index directly.
#        Safe for small / interactive backfills.
#   2) Worker fan-out (``--chunks N``) — launch N worker VMs, each scanning a
#        disjoint date range and writing to
#        ``_index/partial/<run-id>/<chunk-id>.parquet``. No canonical writes.
#   3) Coordinator (``--coordinate --run-id <stamp>``) — single VM merges all
#        partials into the canonical index and deletes the partial shards.
#        Run exactly once after all workers finish.
#
# Prerequisites:
#   - Tarballs refreshed: ``bash create-code-tarballs.sh --asset-group SPORTS``
#     (need UAC + instruments-service tarballs at least).
#   - ADC available on the VM — no external API calls, just GCS reads/writes.
#
# Usage:
#   # Single-VM shapes
#   bash launch-sports-manifest-rescan-vm.sh                            # full rescan (all dates)
#   bash launch-sports-manifest-rescan-vm.sh --date 2024-09-01          # single date (smoke test)
#   bash launch-sports-manifest-rescan-vm.sh --dry-run                  # scan + report, no index write
#   bash launch-sports-manifest-rescan-vm.sh --workers 32               # higher parallelism
#
#   # Chunked parallel backfill — N workers + 1 coordinator
#   bash launch-sports-manifest-rescan-vm.sh \\
#     --chunks 10 --date-start 2018-01-01 --date-end 2026-04-20
#   # ...wait for all workers to finish (shutdown labels show RUN_ID), then:
#   bash launch-sports-manifest-rescan-vm.sh --coordinate --run-id 20260421-120000
#
#   # Escape hatch
#   bash launch-sports-manifest-rescan-vm.sh --force                    # bypass singleton lock
#
# Cost: e2-standard-4 for ~30-60min on the full multi-year window (~2600 dates,
# single VM). With ``--chunks 10`` each worker does ~260 dates → ~5-8min wall
# clock each + ~1min coordinator merge.
#
# Singleton lock:
#   - Default / ``--coordinate``: refuses to launch if any
#     ``sports-manifest-rescan-*`` VM is RUNNING (canonical-write contention).
#   - ``--chunks N``: allowed when the only RUNNING VMs are workers from the
#     SAME run-id (same chunked job). Cross-job concurrency is still blocked.
#   - ``--force``: bypass (use only for dry-run overlap or debugging).
set -euo pipefail

FORCE=false
DRY_RUN=""
DATE_FLAG=""
DATE_START=""
DATE_END=""
WORKERS="16"
CHUNKS=""
COORDINATE=false
RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    --date) DATE_FLAG="--date $2"; shift 2 ;;
    --date-start) DATE_START="$2"; shift 2 ;;
    --date-end) DATE_END="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --chunks) CHUNKS="$2"; shift 2 ;;
    --coordinate) COORDINATE=true; shift ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 1 ;;
  esac
done

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Mode validation ─────────────────────────────────────────────────────
if [[ -n "$CHUNKS" && "$COORDINATE" == "true" ]]; then
  echo "ERROR: --chunks and --coordinate are mutually exclusive" >&2
  exit 1
fi
if [[ -n "$CHUNKS" ]]; then
  if [[ -z "$DATE_START" || -z "$DATE_END" ]]; then
    echo "ERROR: --chunks requires both --date-start and --date-end" >&2
    exit 1
  fi
  if ! [[ "$CHUNKS" =~ ^[0-9]+$ ]] || [[ "$CHUNKS" -lt 2 ]]; then
    echo "ERROR: --chunks must be an integer >= 2 (use single-VM mode for N=1)" >&2
    exit 1
  fi
fi
if [[ "$COORDINATE" == "true" && -z "$RUN_ID" ]]; then
  echo "ERROR: --coordinate requires --run-id <stamp>" >&2
  exit 1
fi

# ── Singleton lock ──────────────────────────────────────────────────────
# - default / --coordinate: any RUNNING sports-manifest-rescan-* blocks.
# - --chunks: RUNNING workers from the same run-id are allowed (sibling
#   workers in this job), everything else blocks.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sports-manifest-rescan-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name,labels.run_id)' 2>/dev/null || true)"
  if [[ -n "$EXISTING" ]]; then
    BLOCKER=""
    while IFS=$'\t' read -r name existing_run_id; do
      [[ -z "$name" ]] && continue
      if [[ -n "$CHUNKS" && -n "$RUN_ID" && "$existing_run_id" == "$RUN_ID" ]]; then
        continue  # sibling worker in the same chunked job — allowed
      fi
      BLOCKER="$name"
      break
    done <<< "$EXISTING"
    if [[ -n "$BLOCKER" ]]; then
      cat >&2 <<EOF
ERROR: sports-manifest-rescan VM already running in $ZONE: $BLOCKER
Refusing to launch — concurrent rescans race on the manifest index parquet at
gs://instruments-store-sports-central-element-323112/_index/availability_index.parquet.

Options:
  Inspect:   gcloud compute ssh $BLOCKER --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${BLOCKER}/run.log
  Stop:      gcloud compute instances delete $BLOCKER --zone=$ZONE --quiet
  Force:     bash $0 --force <flags>
EOF
      exit 1
    fi
  fi
fi

# ── Helper: launch one VM with a prepared rescan command ────────────────
launch_vm() {
  local vm_name="$1"
  local rescan_cmd="$2"
  local run_id_label="$3"
  local chunk_label="${4:-}"

  local metadata="VM_TASK=sports-manifest-rescan"
  metadata="${metadata},VM_SERVICE=instruments_service"
  metadata="${metadata},VM_MIGRATION_CMD=${rescan_cmd}"
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"

  local labels="purpose=sports-manifest-rescan,run-id=${run_id_label}"
  if [[ -n "$chunk_label" ]]; then
    labels="${labels},chunk=${chunk_label}"
  fi

  echo "  → Launching $vm_name"
  echo "    cmd: $rescan_cmd"
  gcloud compute instances create "$vm_name" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${metadata}" \
    --labels="$labels" \
    >/dev/null
}

# ── Mode: coordinator ───────────────────────────────────────────────────
if [[ "$COORDINATE" == "true" ]]; then
  RUN_TS="$(date +%Y%m%d-%H%M%S)"
  VM_NAME="sports-manifest-rescan-coord-${RUN_TS}"
  RESCAN_CMD="python scripts/rescan_sports_fixtures_canonical.py --coordinate --run-id ${RUN_ID}"
  if [[ -n "$DRY_RUN" ]]; then
    RESCAN_CMD="${RESCAN_CMD} ${DRY_RUN}"
  fi
  echo "Coordinator launch (run-id=${RUN_ID})"
  launch_vm "$VM_NAME" "$RESCAN_CMD" "$RUN_ID" "coordinator"
  echo ""
  echo "Coordinator VM: $VM_NAME"
  echo "Tail log: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-manifest-rescan.log'"
  echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
  echo ""
  echo "Verify after completion:"
  echo "  curl -sS 'http://<deployment-api>/api/data-status/manifest?service=instruments-service' | \\"
  echo "    jq '.categories.SPORTS.venues.FIXTURES.leagues | to_entries | sort_by(-.value.completion_pct)[:10]'"
  exit 0
fi

# ── Mode: chunked worker fan-out ───────────────────────────────────────
if [[ -n "$CHUNKS" ]]; then
  if [[ -z "$RUN_ID" ]]; then
    RUN_ID="$(date +%Y%m%d-%H%M%S)"
  fi
  echo "Chunked rescan: N=${CHUNKS} | run-id=${RUN_ID} | range=[${DATE_START}..${DATE_END}]"

  # Compute chunk boundaries with a minimal inline splitter. Kept byte-identical
  # to the script's _split_date_range helper (size = total // N, remainder
  # distributed front-loaded). bash 3.2 compatible (no readarray).
  CHUNK_OUTPUT="$(python3 - "$DATE_START" "$DATE_END" "$CHUNKS" <<'PY'
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
    VM_NAME="sports-manifest-rescan-chunk-${i}of${total_chunks}-${RUN_ID}"
    RESCAN_CMD="python scripts/rescan_sports_fixtures_canonical.py --workers ${WORKERS} --chunk-id ${CHUNK_ID} --run-id ${RUN_ID} --date-start ${cs} --date-end ${ce}"
    if [[ -n "$DRY_RUN" ]]; then
      RESCAN_CMD="${RESCAN_CMD} ${DRY_RUN}"
    fi
    launch_vm "$VM_NAME" "$RESCAN_CMD" "$RUN_ID" "$CHUNK_ID"
  done

  echo ""
  echo "Launched ${total_chunks} worker VMs with run-id=${RUN_ID}"
  echo ""
  echo "Poll for completion:"
  echo "  gcloud compute instances list --filter='labels.run-id=${RUN_ID} AND status=RUNNING' --zones=${ZONE}"
  echo ""
  echo "After all workers finish, run the coordinator:"
  echo "  bash $0 --coordinate --run-id ${RUN_ID}"
  exit 0
fi

# ── Mode: single-VM (default, historical) ──────────────────────────────
RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="sports-manifest-rescan-${RUN_TS}"

RESCAN_CMD="python scripts/rescan_sports_fixtures_canonical.py --workers ${WORKERS}"
if [[ -n "$DATE_FLAG" ]]; then
  RESCAN_CMD="${RESCAN_CMD} ${DATE_FLAG}"
fi
if [[ -n "$DRY_RUN" ]]; then
  RESCAN_CMD="${RESCAN_CMD} ${DRY_RUN}"
fi

echo "Launching $VM_NAME: SPORTS manifest rescan (single-VM)"
launch_vm "$VM_NAME" "$RESCAN_CMD" "$RUN_TS" ""

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-manifest-rescan.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify after completion:"
echo "  curl -sS 'http://<deployment-api>/api/data-status/manifest?service=instruments-service' | \\"
echo "    jq '.categories.SPORTS.venues.FIXTURES.leagues | to_entries | sort_by(-.value.completion_pct)[:10]'"
