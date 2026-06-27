#!/usr/bin/env bash
# Epic: tradfi_master
# Lifecycle: campaign
# Delete-when: tradfi IS-definition catalogue reaches 2026-06-19 across CME/NASDAQ/NYSE/CBOE and tradfi-IS _index is 100% v9 (instruments_mtds_subset_consistency_remediation_2026_06_17.md done)
#
# Fan out the TRADFI instrument-DEFINITION backfill across N parallel GCE VMs to
# replace the serial date-by-date local runner (the long pole gating tradfi-IS
# v9 + catalogue). Each VM is one (venue, disjoint date-chunk) shard with a
# UNIQUE VM_NAME + MANIFEST_PER_VM_SHARDS=true, so the manifest consolidator
# merges _index/per_vm/<VM_NAME>.parquet without collision. Reuses the proven
# `instruments-backfill` task in setup-data-pipeline-vm.sh (chunked
# `instruments-service --operation instruments --mode batch --asset-group tradfi
# --venues <V> --start-date CS --end-date CE`). The per-date freshness pre-flight
# fast-skips already-captured dates and re-fetches only stale ones, so sharding
# parallelizes the genuine work (CBOE 2024/2025 are thin XCBF.PITCH re-source).
#
# Venues = the 3 paid Databento datasets ONLY (CME=GLBX.MDP3, NASDAQ+NYSE=DBEQ.BASIC,
# CBOE=XCBF.PITCH). ICE/FX are EXCLUDED — off the billing allowlist (would raise
# DatabentoDatasetNotAllowedError). SSOT: databento_subscription_allowlist.py +
# tradfi_databento_subscription_universe_lockdown_2026_06_18.md.
#
# Usage:
#   bash launch-tradfi-is-defs-sharded.sh --dry-run     # print the shard plan
#   bash launch-tradfi-is-defs-sharded.sh               # launch the fleet
#   bash launch-tradfi-is-defs-sharded.sh --force       # bypass freshness skip (re-fetch every date)
#
# No fire-and-forget: each VM self-shuts-down on completion
# (VM_SHUTDOWN_ON_COMPLETION=true); verify T+10min with `gcloud compute
# instances describe`. Prefix `instr-backfill-tradfi-` is registered in
# vm_zombie_watchdog.VM_PREFIX_TO_BUCKET (EPHEMERAL_BATCH).
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
CHUNK_DAYS="${CHUNK_DAYS:-30}"
DRY_RUN=false
FORCE=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
RUN_TS="$(date -u +%Y%m%d-%H%M%S)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force)   FORCE=true; shift ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --zone)    ZONE="$2"; shift 2 ;;
    --env)     DEPLOYMENT_ENV="$2"; shift 2 ;;
    --chunk-days) CHUNK_DAYS="$2"; shift 2 ;;
    --on-demand)  ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

# Shard plan: name-suffix|venue|start|end (disjoint per venue).
# CBOE gets 3 shards (thin 2024/2025 XCBF.PITCH re-source is the worst pole);
# NASDAQ/NYSE/CME get 2 each (mostly captured → fast-skip + tail refresh).
declare -a SHARDS=(
  "cboe-a|CBOE|2010-06-19|2017-12-31"
  "cboe-b|CBOE|2018-01-01|2022-12-31"
  "cboe-c|CBOE|2023-01-01|2026-06-19"
  "nasdaq-a|NASDAQ|2010-06-19|2019-12-31"
  "nasdaq-b|NASDAQ|2020-01-01|2026-06-19"
  "nyse-a|NYSE|2010-06-19|2019-12-31"
  "nyse-b|NYSE|2020-01-01|2026-06-19"
  "cme-a|CME|2010-06-19|2019-12-31"
  "cme-b|CME|2020-01-01|2026-06-19"
)

echo "============================================================"
echo "TRADFI IS-definitions SHARDED backfill fleet"
echo "  Project: ${PROJECT_ID}  Zone: ${ZONE}  Machine: ${MACHINE_TYPE}"
echo "  Shards:  ${#SHARDS[@]}   Chunk: ${CHUNK_DAYS}d   Force: ${FORCE}"
echo "  Tarball: gs://${CODE_BUCKET}/code/instruments-service-code.tar.gz"
echo "  Run-ts:  ${RUN_TS}"
echo "============================================================"

for SHARD in "${SHARDS[@]}"; do
  IFS='|' read -r TAG VENUE CS CE <<< "$SHARD"
  VM_NAME="instr-backfill-tradfi-${TAG}-${RUN_TS}"

  METADATA="startup-script-url=${STARTUP}"
  METADATA="${METADATA},VM_TASK=instruments-backfill"
  METADATA="${METADATA},VM_SERVICE=instruments_service"
  METADATA="${METADATA},VM_ASSET_GROUP=TRADFI"
  METADATA="${METADATA},VM_VENUE=${VENUE}"
  METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
  METADATA="${METADATA},VM_NAME=${VM_NAME}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_CHUNK_DAYS=${CHUNK_DAYS}"
  METADATA="${METADATA},VM_START_DATE=${CS}"
  METADATA="${METADATA},VM_END_DATE=${CE}"
  $FORCE && METADATA="${METADATA},VM_FORCE=true"

  echo ""
  echo "--- ${VM_NAME}: ${VENUE} ${CS} → ${CE} ---"
  if $DRY_RUN; then
    echo "  [DRY RUN] would create (venue=${VENUE} window=${CS}..${CE})"
    continue
  fi

  # SPOT by default (--no-restart-on-failure already passed below); --on-demand clears it.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi
  echo "  launching [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
  # shellcheck disable=SC2086
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --labels="purpose=instruments-backfill,asset-group=tradfi,venue=$(echo "${VENUE}" | tr '[:upper:]' '[:lower:]'),env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
    --metadata="${METADATA}"
  echo "  created. log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
  sleep 2
done

echo ""
echo "============================================================"
echo "Launched ${#SHARDS[@]} shard VMs (unless --dry-run)."
echo "  Monitor: gcloud compute instances list --filter='name~instr-backfill-tradfi' --project=${PROJECT_ID} --zones=${ZONE}"
echo "============================================================"
