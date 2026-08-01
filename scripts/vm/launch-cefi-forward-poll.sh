#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a short-lived GCE VM that forward-polls CeFi venues (Tardis fleet).
#
# Purpose: ingest a single day of CeFi market-data ticks for the operator-
# expected coverage set in EXPECTED_COVERAGE_BY_ASSET_GROUP['cefi']
# (BINANCE-SPOT/FUTURES, BYBIT, BYBIT-SPOT, OKX, DERIBIT, UPBIT,
# COINBASE-SPOT/FUTURES, HYPERLIQUID, ASTER) covering the data_types declared per venue
# (trades / book_snapshot_5 / derivative_ticker / liquidations / options_chain
# / futures_chain).
#
# Same code path as launch-cefi-sharded-backfill.sh — the sharded launcher
# fans out historical multi-year ranges across 95 VMs. This launcher does
# the per-day rolling pass on a single VM with the standard MTDS CLI.
#
# Writes to:
#   gs://market-data-tick-cefi-prd-central-element-323112/
#     by_date/day={D}/category=cefi/venue={V}/instrument_type={IT}/
#     data_type={DT}/instrument_id={ID}.parquet
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from metadata):
#   python -m market_tick_data_service \
#     --operation backfill --mode batch --asset-group CEFI \
#     --start-date $VM_START_DATE --end-date $VM_END_DATE
#
# Default: yesterday only (T-1).
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group CEFI
#   - tardis-machine-api-key in Secret Manager
#   - Instrument index parquet up-to-date in instruments-store-cefi-* bucket
#
# Usage:
#   bash launch-cefi-forward-poll.sh                       # yesterday (T-1)
#   bash launch-cefi-forward-poll.sh 2026-04-15 2026-04-18 # explicit window
#   bash launch-cefi-forward-poll.sh --force ...           # bypass singleton lock
#
# Cost: e2-standard-8 ~30-60 min per run depending on day breadth.
#
# Singleton lock: refuses to launch if any cefi-fwd-* VM is already running
# in the zone. Tardis API has per-key request budgets; multiple concurrent
# VMs sharing one key thrash on 429 and waste tarball download cost.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

_positional=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ $# -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
  END_DATE="$START_DATE"
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^cefi-fwd-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: CeFi VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Tardis API rate-limits per-key and
multiple concurrent VMs thrash on 429s.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ${START_DATE} ${END_DATE}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
    exit 1
  fi
fi

# Tardis concurrent-VM cap (cefi_completion_program_2026_07_15.md, operator-approved
# 2026-07-16 "Scope the Tardis cap to AUTHENTICATED batch consumers only"): this
# forward-poll hits the SAME datasets.tardis.dev authenticated path a long-running
# backfill uses (`tardis-machine-api-key` from Secret Manager), so it contends for
# the single licensed IP slot. Asymmetric priority — the backfill is the long-lived
# winner (its own range already covers the days T+1 would fill), so a forward-poll
# QUEUES/refuses behind a running backfill rather than preempting it (refusing here
# mirrors every other integration of this guard — none of them block-and-wait).
# shellcheck source=./tardis-concurrency-guard.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tardis-concurrency-guard.sh"
if [[ "${DRY_RUN:-false}" != "true" ]]; then
  tardis_concurrency_guard 1 "$ZONE" "$PROJECT" || exit 1
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="cefi-fwd-${RUN_TS}"

echo "Launching $VM_NAME: CeFi market-data ${START_DATE}..${END_DATE}"

METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
# Self-declaring Tardis-cap model — this launcher ALWAYS opens the AUTHENTICATED
# datasets.tardis.dev path (tardis-machine-api-key), so it always contends.
METADATA="${METADATA},VM_TARDIS_CONSUMER=1"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=cefi-forward-poll,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
