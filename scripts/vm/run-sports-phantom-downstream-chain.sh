#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Sequential downstream entity backfill chain for sports phantom recovery.
#
# After the FIXTURES backfill VM (af-backfill-{ts}) auto-shuts on completion,
# this script cycles through the 5 remaining api_football per-fixture entities
# (PLAYER_STATS / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS / INJURIES),
# launching one VM per entity and waiting for STOPPED/FAILED before launching
# the next. Singleton lock on `af-backfill-` prefix sequences these naturally.
#
# Pre-req: `instruments-service/scripts/delete_phantom_rows_from_shards.py
# --apply` has been run for the entity's data type rows. (For FIXTURES this
# was done 2026-05-06 12:07 UTC; for the 5 downstream entities the deletion
# was done in the same pass.)
#
# Each VM auto-shuts on completion; this script polls the events bucket every
# 60s for STOPPED/FAILED and launches the next entity when terminal.
#
# Usage:
#   bash deployment-service/scripts/vm/run-sports-phantom-downstream-chain.sh \
#     --start-date 2020-06-06 --end-date 2026-05-04 \
#     [--entities PLAYER_STATS,FIXTURE_STATS,FIXTURE_EVENTS,FIXTURE_LINEUPS,INJURIES]
#
# Notes:
# - This script blocks for the full chain (~3-5h). Run from a long-lived
#   shell (tmux / screen / nohup), not a bash subshell that may exit.
# - If a VM FAILED instead of STOPPED, the script aborts so the operator can
#   diagnose before continuing.
# - The launcher itself prepends `--force` is NOT needed because phantom rows
#   were already deleted from the manifest — orchestrator sees them as missing.

set -euo pipefail

ENTITIES_DEFAULT="PLAYER_STATS,FIXTURE_STATS,FIXTURE_EVENTS,FIXTURE_LINEUPS,INJURIES"
START_DATE=""
END_DATE=""
ENTITIES="$ENTITIES_DEFAULT"
RECOVERY_FIXTURE_IDS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date) END_DATE="$2"; shift 2 ;;
    --entities) ENTITIES="$2"; shift 2 ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  cat >&2 <<EOF
Usage: bash $0 --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--entities CSV] \\
              [--recovery-fixture-ids gs://path/to/fixtures_recovery_set.parquet]

Defaults:
  --entities $ENTITIES_DEFAULT
EOF
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
EVENTS_BUCKET="${PROJECT}-events"
LAUNCHER="$(dirname "$0")/launch-api-football-backfill-vm.sh"

if [[ ! -x "$LAUNCHER" && ! -f "$LAUNCHER" ]]; then
  echo "ERROR: launcher not found at $LAUNCHER" >&2
  exit 1
fi

echo "Sports phantom downstream chain"
echo "  date range: $START_DATE .. $END_DATE"
echo "  entities  : $ENTITIES"
echo ""

IFS=',' read -ra ENTITY_LIST <<< "$ENTITIES"

for entity in "${ENTITY_LIST[@]}"; do
  entity_trimmed="$(echo "$entity" | xargs)"
  if [[ -z "$entity_trimmed" ]]; then continue; fi

  echo "=== Entity: $entity_trimmed ==="
  echo "Launching VM..."

  # Capture VM name from launcher output ("VM launched: <name>")
  LAUNCH_ARGS=(--entity "$entity_trimmed")
  if [[ -n "$RECOVERY_FIXTURE_IDS" ]]; then
    LAUNCH_ARGS+=(--recovery-fixture-ids "$RECOVERY_FIXTURE_IDS")
  fi
  LAUNCH_OUT="$(bash "$LAUNCHER" "${LAUNCH_ARGS[@]}" "$START_DATE" "$END_DATE" 2>&1)"
  VM_NAME="$(echo "$LAUNCH_OUT" | grep -oE 'VM launched: af-backfill-[0-9-]+' | awk '{print $3}' | head -1)"

  if [[ -z "$VM_NAME" ]]; then
    echo "ERROR: could not determine VM name from launcher output:" >&2
    echo "$LAUNCH_OUT" >&2
    exit 1
  fi

  echo "VM: $VM_NAME"
  echo "Waiting for STOPPED or FAILED event..."

  # Poll the events bucket every 60s for terminal event.
  # Use today's date because backfill VMs typically finish same-day; if this
  # changes, extend the loop to scan multiple date partitions.
  TODAY="$(date -u +%Y-%m-%d)"
  while true; do
    EVENTS_PATH="gs://${EVENTS_BUCKET}/events/instruments-service/${TODAY}/${VM_NAME}/hour=*/*.jsonl"
    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's
    # active account (a short-lived WIF token in an interactive AO slot can't
    # refresh unattended), while `gcloud storage` resolves via ADC, which stays
    # valid. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    TERMINAL="$(gcloud storage cat "$EVENTS_PATH" 2>/dev/null | grep -E '"event": "(STOPPED|FAILED)"' | tail -1 || true)"
    if [[ -n "$TERMINAL" ]]; then
      if echo "$TERMINAL" | grep -q '"event": "FAILED"'; then
        echo "FAILED event detected for $VM_NAME — aborting chain. Diagnose:" >&2
        echo "$TERMINAL" >&2
        echo "  gsutil cat gs://deployment-scripts-${PROJECT}/vm-logs/${VM_NAME}/run.log | tail -50" >&2
        exit 1
      fi
      echo "STOPPED event detected for $VM_NAME"
      break
    fi
    # Also exit the loop if VM is gone (auto-shutdown deletes the instance)
    if ! gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --format='value(name)' >/dev/null 2>&1; then
      echo "VM $VM_NAME no longer exists; assuming clean STOPPED + auto-shutdown."
      break
    fi
    sleep 60
  done

  echo "Entity $entity_trimmed completed."
  echo ""
done

echo "All entities done."
echo ""
echo "Next steps:"
echo "  1. Run audit-and-flip-stale-empties script (drift between FIXTURES truth + per-fixture empties)"
echo "  2. Clear deployment-UI /turbo cache + verify SPORTS coverage jump"
