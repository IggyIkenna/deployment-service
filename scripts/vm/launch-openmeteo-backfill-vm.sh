#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that backfills OpenMeteo weather (venue hourly) for sports
# fixtures. No API key; weather endpoints tolerate concurrent reads.
#
# Historical window: ERA5 archive back to 1940 for past dates; forecast branch
# for dates > today (see codex §2.5).
#
# Cadence + forecast vs archive: codex/02-data/sports-scheduling-and-sharding.md
# §2.5 — forecast for future dates; observed ERA5 at T+1h post-kickoff for dates ≤ today.
#
# **No singleton-lock** — concurrent VMs are safe (no shared API key quota).
#
# Two invocation shapes:
#
#   1. Rolling:
#        bash launch-openmeteo-backfill-vm.sh --lookback 1 --lookahead 7
#        bash launch-openmeteo-backfill-vm.sh --entity WEATHER --lookback 1 --lookahead 7 --force-window
#
#   2. Explicit historical range:
#        bash launch-openmeteo-backfill-vm.sh 2018-01-01 2019-01-15
#        bash launch-openmeteo-backfill-vm.sh --entity WEATHER 2024-09-01 2024-09-01
#
# Entity is singular: WEATHER (optional; default is WEATHER-only provider scope).
#
# Invocation inside the VM:
#   python -m instruments_service \
#     --operation instruments --mode batch --asset-group SPORTS \
#     --sports-provider OPEN_METEO \
#     --sports-entity WEATHER ...
#
# Prerequisites:
#   - Tarballs: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#
# Usage:
#   bash launch-openmeteo-backfill-vm.sh 2018-01-01 2019-01-15
#   bash launch-openmeteo-backfill-vm.sh --entity WEATHER 2024-09-01 2024-09-01
#   bash launch-openmeteo-backfill-vm.sh --lookback 1 --lookahead 7
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

ENTITY=""
LOOKBACK=""
LOOKAHEAD=""
FORCE_WINDOW=false
RECOVERY_FIXTURE_IDS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND="${ON_DEMAND:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --lookback) LOOKBACK="$2"; shift 2 ;;
    --lookahead) LOOKAHEAD="$2"; shift 2 ;;
    --force-window) FORCE_WINDOW=true; shift ;;
    --recovery-fixture-ids) RECOVERY_FIXTURE_IDS="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

USE_ROLLING=false
if [[ -n "$LOOKBACK" || -n "$LOOKAHEAD" ]]; then
  USE_ROLLING=true
fi

if $USE_ROLLING; then
  if [[ $# -ne 0 ]]; then
    cat >&2 <<EOF
ERROR: cannot combine --lookback/--lookahead with positional <start> <end> dates.
Pick one mode: rolling (--lookback N --lookahead M) OR explicit (<start> <end>).
EOF
    exit 1
  fi
  [[ -z "$LOOKBACK" ]] && LOOKBACK=0
  [[ -z "$LOOKAHEAD" ]] && LOOKAHEAD=0
  if ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]] || ! [[ "$LOOKAHEAD" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --lookback / --lookahead must be non-negative integers (got lookback=$LOOKBACK, lookahead=$LOOKAHEAD)" >&2
    exit 1
  fi
  RANGE_DESC="rolling [today-${LOOKBACK}..today+${LOOKAHEAD}] UTC"
else
  if [[ $# -ne 2 ]]; then
    cat >&2 <<EOF
Usage: bash launch-openmeteo-backfill-vm.sh [--entity WEATHER] \\
         ( <START_DATE> <END_DATE> | --lookback N --lookahead M [--force-window] )

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).
  ENTITY: WEATHER (optional; OpenMeteo provider is weather-only).

Examples:
  bash launch-openmeteo-backfill-vm.sh 2018-01-01 2019-01-15
  bash launch-openmeteo-backfill-vm.sh --entity WEATHER 2024-09-01 2024-09-01
  bash launch-openmeteo-backfill-vm.sh --lookback 1 --lookahead 7
EOF
    exit 1
  fi

  START_DATE="$1"
  END_DATE="$2"

  if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
    exit 1
  fi
  RANGE_DESC="${START_DATE}..${END_DATE}"

  if $FORCE_WINDOW; then
    echo "WARNING: --force-window is only meaningful with --lookback/--lookahead; ignored in explicit-date mode." >&2
    FORCE_WINDOW=false
  fi
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# 32GB default: the IS fixtures catalogue + per-fixture footprint OOM-kills
# e2-standard-2 (8GB). Env-overridable. SSOT: plans/active/sports_reference_backfill_oom_2026_06_22.md
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="weather-backfill-${RUN_TS}"

METADATA="VM_TASK=sports-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=instruments"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
if $USE_ROLLING; then
  METADATA="${METADATA},VM_LOOKBACK_DAYS=${LOOKBACK}"
  METADATA="${METADATA},VM_LOOKAHEAD_DAYS=${LOOKAHEAD}"
  $FORCE_WINDOW && METADATA="${METADATA},VM_FORCE_WINDOW=true"
else
  METADATA="${METADATA},VM_START_DATE=${START_DATE}"
  METADATA="${METADATA},VM_END_DATE=${END_DATE}"
fi
METADATA="${METADATA},VM_SPORTS_PROVIDER=OPEN_METEO"
METADATA="${METADATA},VM_SPORTS_ENTITY=${ENTITY:-WEATHER}"
[[ -n "$RECOVERY_FIXTURE_IDS" ]] && METADATA="${METADATA},VM_RECOVERY_FIXTURE_IDS=${RECOVERY_FIXTURE_IDS}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

ENTITY_DESC="entity=WEATHER"
[[ -n "$ENTITY" ]] && ENTITY_DESC="entity=$ENTITY"
echo "Launching $VM_NAME: OPEN_METEO backfill ${RANGE_DESC} ($ENTITY_DESC) [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # Preemption signal: write PREEMPTED blob so the exit-code fleet monitor
  # classifies a spot preemption as a benign relaunch (no DP_VM_GONE_NO_CAPTURE).
  SHUTDOWN_FILE=$(mktemp)
  trap 'rm -f "$SHUTDOWN_FILE"' EXIT
  cat > "$SHUTDOWN_FILE" <<'SHUTDOWN_EOF'
#!/usr/bin/env bash
PREEMPTED=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/preempted' 2>/dev/null || echo 'false')
[[ "$PREEMPTED" == "true" ]] || exit 0
VM_NAME=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo "")
PROJECT=$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/project/project-id' 2>/dev/null || echo "")
[[ -n "$VM_NAME" && -n "$PROJECT" ]] || exit 0
echo "preempted" | gcloud storage cp - \
  "gs://deployment-scripts-${PROJECT}/vm-logs/${VM_NAME}/PREEMPTED" --quiet 2>/dev/null || true
echo "[preemption-shutdown] wrote PREEMPTED signal for ${VM_NAME}" >&2
SHUTDOWN_EOF

  # shellcheck disable=SC2086
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --metadata-from-file=shutdown-script="${SHUTDOWN_FILE}" \
    --labels=purpose=openmeteo-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "After completion, rerun the rescan to materialise empty_confirmed rows:"
echo "  bash $(dirname "$0")/launch-sports-manifest-rescan-vm.sh"
