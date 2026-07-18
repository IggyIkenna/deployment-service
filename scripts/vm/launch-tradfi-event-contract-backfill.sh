#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch instruments-service TradFi backfill VM for the 9 CME event-contract roots:
#   ECES / ECBTC / ECRTY / ECYM / ECGC / ECCL / ECNG / EC6E / ECNQ
#
# These are listed on Databento as GLBX.MDP3 parent instruments with .OPT suffix
# (e.g. ECES.OPT). Databento coverage start: 2025-09-28. The adapter classifies
# them as EVENT_CONTRACT via the BAG stype / EC* prefix logic.
#
# Writes to:
#   gs://instruments-store-tradfi-{env}-{project}/_index/availability_index.parquet
#   (per-VM shard: _index/per_vm/{VM_NAME}.parquet, merged by manifest-consolidator)
#
# Phase 0 of the CME x Polymarket arb plan (tradfi_cme_event_contract_backfill plan).
# Unblocks Phases 1-5 of the archived cme_polymarket_arb_2026_05_08 sub-plan.
#
# Usage:
#   bash launch-tradfi-event-contract-backfill.sh                     # 2025-09-28 → today
#   bash launch-tradfi-event-contract-backfill.sh 2025-09-28 2026-01-01   # explicit window
#   bash launch-tradfi-event-contract-backfill.sh --dry-run           # no VM created
#   bash launch-tradfi-event-contract-backfill.sh --force             # bypass singleton lock
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group TRADFI
#   - databento-api-key in Secret Manager (shared with other tradfi backfill VMs)
#
# Cost: e2-standard-4 ~30-90 min for [2025-09-28, today] (~9 months at 30d chunks).
#
# Singleton lock: refuses to launch if a tradfi-event-contract-backfill-* VM is RUNNING.
# Databento account is shared across all TradFi backfill VMs — concurrent runs risk
# rate-limit errors. Use --force to bypass.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

_positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --force)     FORCE=true; shift ;;
    --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand) ON_DEMAND=true; shift ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ $# -ge 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  START_DATE="2025-09-28"
  END_DATE="$(date -u +%Y-%m-%d)"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(~/google-cloud-sdk/bin/gcloud compute instances list \
    --filter='name~"^tradfi-event-contract-backfill-" AND status=RUNNING' \
    --zones="$ZONE" \
    --project="$PROJECT" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: tradfi-event-contract-backfill VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Databento rate limits are per-IP.

Options:
  Inspect:   ~/google-cloud-sdk/bin/gcloud compute ssh $EXISTING --zone=$ZONE --project=$PROJECT
  GCS log:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ${START_DATE} ${END_DATE}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/GCS
log above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  ~/google-cloud-sdk/bin/gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="tradfi-event-contract-backfill-${RUN_TS}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  service:     instruments-service"
echo "  asset_group: TRADFI"
echo "  venue:       CME (EC* event contracts)"
echo "  range:       ${START_DATE} → ${END_DATE}"
echo "  zone:        $ZONE"

METADATA="VM_TASK=instruments-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=TRADFI"
METADATA="${METADATA},VM_VENUE=CME"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_CHUNK_DAYS=30"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN] Metadata: $METADATA"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  ~/google-cloud-sdk/bin/gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=tradfi-event-contract-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Shard:       gs://instruments-store-tradfi-prd-${PROJECT}/_index/per_vm/${VM_NAME}.parquet"
echo "Delete:      ~/google-cloud-sdk/bin/gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify STARTED (T+10min):"
echo "  ~/google-cloud-sdk/bin/gcloud compute instances describe $VM_NAME --zone=$ZONE --project=$PROJECT --format='value(status)'"
