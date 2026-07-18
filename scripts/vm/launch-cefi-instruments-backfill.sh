#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch CeFi instruments-service backfill VM(s).
#
# Purpose: capture per-(date, venue) instrument reference data into
#   gs://instruments-store-cefi-central-element-323112/
#     instrument_availability/by_date/day={D}/venue={V}/instruments.parquet
# AND register the shard in the canonical availability manifest at
#   gs://instruments-store-cefi-.../_index/availability_index.parquet
# (via _index/per_vm/{VM_NAME}.parquet, merged by manifest-consolidator).
#
# Distinct from launch-cefi-* scripts which all target market_tick_data_service
# (price ticks via Tardis). This is the instruments-service path which fetches
# instrument metadata via per-venue REST APIs (Binance, OKX, Bybit, Coinbase,
# Deribit, Bitfinex, Bitget, Hyperliquid, Aster, Upbit).
#
# Skip-existing: instruments-service orchestrator pre-flight checks the
# manifest and skips (date, venue) shards already CAPTURED. Re-running with
# overlapping ranges no-ops on completed shards. Force overrides.
#
# Per-VM shard isolation: setup-data-pipeline-vm.sh exports
#   MANIFEST_PER_VM_SHARDS=true + VM_NAME=$(GCE instance name)
# automatically, so every VM writes to its own _index/per_vm/{vm}.parquet —
# no CAS races on the canonical, the consolidator merges them every 60s.
#
# Singleton lock: refuses to launch if an cefi-instr-* VM is already running
# in the zone. Many CeFi venues have shared rate-limit budgets per source-IP
# (binance, okx, deribit). Multiple concurrent VMs hitting the same venue
# from the same NAT egress thrash on 429s. Use --force to bypass for
# multi-venue fan-out where you've manually distributed venues across VMs.
#
# Usage:
#   bash launch-cefi-instruments-backfill.sh                              # last 7 days, all venues
#   bash launch-cefi-instruments-backfill.sh 2026-04-15 2026-05-04        # explicit window, all venues
#   bash launch-cefi-instruments-backfill.sh 2026-04-15 2026-05-04 BINANCE-SPOT  # one venue
#   bash launch-cefi-instruments-backfill.sh --force ...                  # bypass singleton lock
#
# Cost: e2-standard-4 ~5-15 min per (venue, day) range; instruments are
# small (REST GET + parse, no Tardis $ cost).
#
# Prerequisites:
#   - Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group CEFI
#   - Per-venue API keys in Secret Manager (most CeFi REST endpoints don't
#     require auth, but ApiKeyReloader expects entries to exist)
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

_positional=()
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Args: [start_date end_date [venue]]
if [[ $# -ge 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
  VENUE="${3:-}"
else
  END_DATE="$(date -u -d "yesterday" +%Y-%m-%d)"
  START_DATE="$(date -u -d "7 days ago" +%Y-%m-%d)"
  VENUE=""
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^cefi-instr-" AND status=RUNNING' \
    --zones="$ZONE" \
    --project="$PROJECT" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: CeFi instruments VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — venue REST APIs share rate limits
per source-IP and concurrent VMs from the same NAT egress thrash on 429s.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE --project=$PROJECT
  GCS log:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ${START_DATE} ${END_DATE} ${VENUE:-}

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

RUN_TS="$(date +%Y%m%d-%H%M%S)"
# VM name suffix carries venue when scoped, "all" otherwise. GCE caps name
# at 63 chars; venue strings (BINANCE-FUTURES = 15) fit comfortably.
SUFFIX_RAW="${VENUE:-all}"
NAME_SUFFIX=$(echo "$SUFFIX_RAW" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed 's/--*/-/g; s/^-//; s/-$//')
VM_NAME="cefi-instr-${NAME_SUFFIX}-${RUN_TS}"

echo "Launching $VM_NAME"
echo "  service:    instruments-service"
echo "  asset_group: CEFI"
echo "  range:      ${START_DATE} → ${END_DATE}"
echo "  venue:      ${VENUE:-<all CeFi venues>}"
echo "  zone:       $ZONE"

METADATA="VM_TASK=cefi-instruments-backfill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=download"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
[[ -n "$VENUE" ]] && METADATA="${METADATA},VM_VENUE=${VENUE}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

  echo "Launching VM $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
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

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
    # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
    # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
    # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
    # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
    # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=cefi-instruments-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "GCS log tail:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Per-VM shard:    gs://instruments-store-cefi-${PROJECT}/_index/per_vm/${VM_NAME}.parquet"
echo "Captured data:   gs://instruments-store-cefi-${PROJECT}/instrument_availability/by_date/day={D}/venue={V}/instruments.parquet"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
