#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: temporary
# Delete-when: after the Track S2 TEAMS full-history backfill completes (2026-08-05)
# Launch a SPOT GCE VM that runs the Track S2 TEAMS full-history backfill
# (`instruments-service/scripts/backfill_teams_full_history_2026_08_05.py`).
#
# The script reads the canonical availability manifest, computes every
# api_football TEAMS (league, date) cell in `expected_unattempted` state
# (322 leagues / 67,782 cells as of 2026-08-05, spanning 2021-09-18..2026-07-23),
# fetches the ONE live `/teams` roster per league, then writes + manifest-records
# that roster across every missing historical cell — the same data model as the
# 61-league backfill (`backfill_teams_61_leagues_2026_07_13.py`) + the daily
# orchestrator.
#
# Dispatch: `VM_TASK=sports-gap-fill` (setup-data-pipeline-vm.sh:1381) runs a
# standalone instruments-service script via `VM_MIGRATION_CMD`, substituting
# `python` → `$VENV/bin/python`. The script is manifest-idempotent: a re-run
# recomputes missing cells from the live manifest, so a preempted SPOT VM
# relaunches cleanly and resumes from measured progress (no date replay).
#
# VM naming: instr-backfill-sports-teams-{ts} (registered watchdog prefix
# "instr-backfill-sports", EPHEMERAL_BATCH, bucket = instruments-store-sports-prd-*)
#
# Usage (from workspace root):
#   bash deployment-service/scripts/vm/launch-sports-teams-full-history-backfill-vm.sh
#   bash deployment-service/scripts/vm/launch-sports-teams-full-history-backfill-vm.sh --limit-leagues 2  # smoke test
#   bash deployment-service/scripts/vm/launch-sports-teams-full-history-backfill-vm.sh --concurrency 16
#   bash deployment-service/scripts/vm/launch-sports-teams-full-history-backfill-vm.sh --dry-run
#   bash deployment-service/scripts/vm/launch-sports-teams-full-history-backfill-vm.sh --on-demand
#
# Prerequisites (mandatory):
#   - Tarballs refreshed so the VM ships the NEW backfill script:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#     (lc_verify_tarball_freshness below aborts the launch otherwise.)
set -euo pipefail

# Pin the gcloud identity PER-INVOCATION so a sibling slot's
# `gcloud config set account` cannot swap in a weaker identity mid-run
# (issues/shared_host_gcloud_active_account_cross_slot_clobber_2026_08_04.md).
export CLOUDSDK_CORE_ACCOUNT="${CLOUDSDK_CORE_ACCOUNT:-unified-trading-sa@central-element-323112.iam.gserviceaccount.com}"

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ON_DEMAND=false
CONCURRENCY=32
LIMIT_LEAGUES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --force)         FORCE=true; shift ;;
    --on-demand)     ON_DEMAND=true; shift ;;
    --concurrency)   CONCURRENCY="$2"; shift 2 ;;
    --limit-leagues) LIMIT_LEAGUES="$2"; shift 2 ;;
    --env)           DEPLOYMENT_ENV="$2"; shift 2 ;;
    -h|--help)       sed -n '1,45p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ "$CONCURRENCY" != *[!0-9]* ]]; then :; else echo "ERROR: --concurrency must be a positive integer (got: $CONCURRENCY)" >&2; exit 1; fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="instr-backfill-sports-teams-${RUN_TS}"

# Build the python invocation. The `python` literal is rewritten to
# `$VENV/bin/python` by setup-data-pipeline-vm.sh's sports-gap-fill dispatch.
SCRIPT_ARGS="--apply --concurrency ${CONCURRENCY}"
(( LIMIT_LEAGUES > 0 )) && SCRIPT_ARGS="${SCRIPT_ARGS} --limit-leagues ${LIMIT_LEAGUES}"
MIGRATION_CMD="python scripts/backfill_teams_full_history_2026_08_05.py ${SCRIPT_ARGS}"

echo "Launching $VM_NAME"
echo "  Command: ${MIGRATION_CMD}"
echo "  Provisioning: $([[ $ON_DEMAND == true ]] && echo on-demand || echo SPOT)"
echo "  Tarball:  gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"

# ── Singleton lock: api_football rate-limits per-key ──
# This VM makes only 322 `/teams` calls (one per league), a negligible slice of
# the shared key — but a concurrent FIXTURES/fill-missing run still competes for
# the same per-key quota, so refuse to stack onto a live api-football VM unless
# --force (genuinely disjoint task).
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^fill-missing-player-stats-" OR name~"^instr-backfill-sports-teams-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    echo "ERROR: an api_football VM is already running in $ZONE: $EXISTING" >&2
    echo "Refusing to launch a duplicate — api_football rate-limits per-key." >&2
    echo "Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE" >&2
    echo "Force:     bash $0 --force ..." >&2
    exit 1
  fi
fi

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: ${VM_NAME}"
  echo "[DRY-RUN] Metadata: VM_TASK=sports-gap-fill VM_SERVICE=instruments_service"
  echo "[DRY-RUN] VM_MIGRATION_CMD=${MIGRATION_CMD}"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
  exit 0
fi

lc_verify_tarball_freshness "$CODE_BUCKET" \
    instruments-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=sports-gap-fill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_SPORTS_PROVIDER=API_FOOTBALL"
METADATA="${METADATA},VM_MIGRATION_CMD=${MIGRATION_CMD}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# instruments-store-sports-prd's consolidator merge cycle regularly takes
# 400-460s (>3x the reader's 120s default) — see
# plans/active/issues/manifest_consolidator_stale_sports_bucket_2026_07_21.md
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=1800"
# The write phase (67,782 cells at concurrency=32) runs ~25-60 min; a 4h stall
# window covers fetch + write + manifest drain without false-positive pages.
METADATA="${METADATA},STALL_TIMEOUT_SEC=14400"

# SPOT by default (workspace HARD RULE); --on-demand / ON_DEMAND=true forces standard.
PROVISIONING_FLAGS=""
if $ON_DEMAND; then
  PROVISIONING_FLAGS="--provisioning-model=STANDARD"
else
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
fi

# Preemption signal: write PREEMPTED blob so the exit-code fleet monitor
# classifies a spot preemption as a benign relaunch (no DP_VM_GONE_NO_CAPTURE),
# and persist launch params so RelaunchPreemptedVm can re-invoke this launcher.
# The script is manifest-idempotent, so a relaunch resumes from measured
# progress (no date replay).
lc_write_preemption_signal_file "$VM_NAME" "$PROJECT"
SHUTDOWN_FILE="$PREEMPTION_SIGNAL_FILE"
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-sports-teams-full-history-backfill-vm.sh" \
    "CONCURRENCY=${CONCURRENCY}" \
    "LIMIT_LEAGUES=${LIMIT_LEAGUES}" \
    "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
  --zone="$ZONE" \
  --machine-type=e2-standard-4 \
  --scopes=cloud-platform \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --metadata="${METADATA}" \
  --metadata-from-file=shutdown-script="${SHUTDOWN_FILE}" \
  --labels=purpose=sports-teams-full-history-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-gap-fill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "T+10min check (mandatory):"
echo "  gcloud compute instances describe $VM_NAME --zone=$ZONE --project=$PROJECT --format='value(status)'"
