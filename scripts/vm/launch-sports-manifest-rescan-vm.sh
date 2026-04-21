#!/usr/bin/env bash
# Launch a GCE VM that rescans the SPORTS FIXTURES availability manifest with
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
# Invocation inside the VM (assembled from VM_MIGRATION_CMD metadata):
#   python scripts/rescan_sports_fixtures_canonical.py --workers 16
#
# Prerequisites:
#   - Tarballs refreshed: ``bash create-code-tarballs.sh --category SPORTS``
#     (need UAC + instruments-service tarballs at least).
#   - ADC available on the VM — no external API calls, just GCS reads/writes.
#
# Usage:
#   bash launch-sports-manifest-rescan-vm.sh                      # full rescan (all dates)
#   bash launch-sports-manifest-rescan-vm.sh --date 2024-09-01    # single date (smoke test)
#   bash launch-sports-manifest-rescan-vm.sh --dry-run            # scan + report, no index write
#   bash launch-sports-manifest-rescan-vm.sh --workers 32         # higher parallelism
#   bash launch-sports-manifest-rescan-vm.sh --force              # bypass singleton lock
#
# Cost: e2-standard-4 for ~30-60min on the full multi-year window (~2600 dates).
#
# Singleton lock: refuses to launch if any sports-manifest-rescan-* VM is
# already running in the zone. Two concurrent rescans would race on the
# manifest index parquet write and potentially produce inconsistent output.
# Pass --force for legitimate parallel investigations on different date
# windows (each run writes the full index, so only the last-writer wins —
# only use --force for dry-run overlap).
set -euo pipefail

FORCE=false
DRY_RUN=""
DATE_FLAG=""
WORKERS="16"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    --date) DATE_FLAG="--date $2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 1 ;;
  esac
done

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock — concurrent rescans race on the manifest index ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^sports-manifest-rescan-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: sports-manifest-rescan VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — concurrent rescans race on the manifest
index parquet write at
gs://instruments-store-sports-central-element-323112/_index/availability_index.parquet.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force <flags>
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="sports-manifest-rescan-${RUN_TS}"

# Rescan script command — runs inside $WORKSPACE/instruments on the VM
# (setup-data-pipeline-vm.sh cd's there for VM_TASK=sports-manifest-rescan).
RESCAN_CMD="python scripts/rescan_sports_fixtures_canonical.py --workers ${WORKERS}"
if [[ -n "$DATE_FLAG" ]]; then
  RESCAN_CMD="${RESCAN_CMD} ${DATE_FLAG}"
fi
if [[ -n "$DRY_RUN" ]]; then
  RESCAN_CMD="${RESCAN_CMD} ${DRY_RUN}"
fi

echo "Launching $VM_NAME: SPORTS manifest rescan"
echo "Command: $RESCAN_CMD"

METADATA="VM_TASK=sports-manifest-rescan"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${RESCAN_CMD}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=sports-manifest-rescan,run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-manifest-rescan.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify after completion:"
echo "  curl -sS 'http://<deployment-api>/api/data-status/manifest?service=instruments-service' | \\"
echo "    jq '.categories.SPORTS.venues.FIXTURES.leagues | to_entries | sort_by(-.value.completion_pct)[:10]'"
