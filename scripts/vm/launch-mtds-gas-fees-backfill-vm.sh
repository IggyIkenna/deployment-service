#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that backfills per-chain gas-fee history via the
# unified MTDS collect-gas-fees CLI.
#
# Purpose: ingest per-block base + priority gas fees for every chain in
# DEFAULT_GAS_FEE_CHAINS. Writes to the SHARED DeFi tick bucket — gas_fees is a
# data_type WITHIN the overall DeFi manifest, not a dedicated bucket (the old
# gas-fees-{project} bucket was retired in the 2026-07 dedicated->shared migration):
#   gs://market-data-tick-defi-{env}-{project}/raw_tick_data/by_date/day={D}/
#     pipeline_mode=batch_onchain_rpc/asset_group=defi/venue={CHAIN}/chain={CHAIN}/
#     instrument_type=spot_asset/data_type=gas_fees/...
#
# Chain coverage (gas_fee_handler.DEFAULT_GAS_FEE_CHAINS):
#   Tier 1+2 (Alchemy):  ETHEREUM, OPTIMISM, BSC, POLYGON, BASE, ARBITRUM,
#                        AVALANCHE, LINEA  (8 chains)
#   Tier 4 (public RPC): FANTOM, METIS, MOONBEAM, MANTLE, CELO, AURORA
#                        (6 chains, defi_pipeline_extension_followups Phase 5)
#
# Per-chain mainnet-genesis dates clip pre-existing days (UAC SSOT
# GAS_FEE_CHAIN_START_DATES). Date-range smaller than any chain's start
# is skipped without burning RPC quota.
#
# Usage:
#   bash launch-mtds-gas-fees-backfill-vm.sh                       # T-1 single day
#   bash launch-mtds-gas-fees-backfill-vm.sh 2025-01-01 2025-01-31 # explicit window
#   bash launch-mtds-gas-fees-backfill-vm.sh --force ...           # bypass singleton
#   bash launch-mtds-gas-fees-backfill-vm.sh --chunks 4 2021-01-01 2026-06-19   # fan-out
#   bash launch-mtds-gas-fees-backfill-vm.sh --dry-run --chunks 4 2021-01-01 2026-06-19
#
# Cost: e2-standard-8 + 50GB. ~30s/chain/day for Tier 1+2 (block-fees endpoint
# fast on Alchemy archival). Tier-4 public RPCs are slower (~60-120s each).
# Single-day across 14 chains: ~5-15min.
#
# CHUNK-PARALLELISM (`--chunks N`, backfill_vm_silent_worker_stall_watchdog P2):
#   The full 2021→2026 range on ONE VM is multi-WEEK (~5 days/2.5h observed
#   2026-06-19). `--chunks N` splits the date range into N DISJOINT sub-windows
#   and fires N sibling VMs (shared `run-id` label; per-VM manifest shards →
#   chunks are naturally disjoint on date, no coordinator merge). The
#   sibling-aware singleton treats one run-id as ONE logical job (a SECOND
#   `--chunks` launch with a NEW run-id is refused while siblings run).
#
#   ⚠️ ALCHEMY SHARED-CU CAVEAT (read before raising N): the 8 Tier-1+2 chains
#   share ONE Alchemy key's compute-unit budget, so N VMs do NOT give a clean N×
#   on those chains — they share the per-key CU/s ceiling. The 6 Tier-4 public-RPC
#   chains hit their OWN endpoints and DO parallelise freely. The 2026-06-19 stall
#   was LATENCY-bound (a degraded POLYGON RPC stacking 30s timeouts), NOT CU
#   exhaustion (no 429/CU-limit in the log), so a modest N=2–4 is a real speedup;
#   if 429 / "compute units per second" errors appear in the chunk logs, REDUCE N
#   (or provision a 2nd Alchemy key + a per-chunk key assignment — the genuine
#   path to a clean N×, same as SFI's "second key" unblock). This is why `--chunks`
#   is OPT-IN (default single-stream) and unbounded N is NOT auto-selected.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
DRY_RUN=false
CHUNKS=""
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)     DRY_RUN=true; shift ;;
        --force)       FORCE=true; shift ;;
        --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
        --chunks)      CHUNKS="$2"; shift 2 ;;
        --on-demand)   ON_DEMAND=true; shift ;;
        --preemptible) shift ;;  # deprecated no-op: SPOT is now the default
        --) shift; while [[ $# -gt 0 ]]; do POSITIONAL+=("$1"); shift; done; break ;;
        -*) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ ${#POSITIONAL[@]} -eq 2 ]]; then
    START_DATE="${POSITIONAL[0]}"
    END_DATE="${POSITIONAL[1]}"
elif [[ ${#POSITIONAL[@]} -eq 0 ]]; then
    START_DATE="$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
    END_DATE="$START_DATE"
else
    cat >&2 <<EOF
Usage: $0 [--force] [--env prod|staging|dev] [--chunks N] [START_DATE END_DATE]

Defaults to yesterday (T-1). Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').
Pass --chunks N (N >= 2) to split the range into N disjoint sibling VMs
  (recommended N<=4 — the 8 Alchemy chains share one key's CU budget; see the
  ALCHEMY SHARED-CU CAVEAT in this script's header before raising N).
EOF
    exit 1
fi

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
    exit 1
fi

if [[ -n "$CHUNKS" ]] && ! [[ "$CHUNKS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --chunks must be a positive integer (got: $CHUNKS)" >&2
    exit 1
fi
# N=1 (or unset) is single-stream; normalise.
[[ "$CHUNKS" == "1" ]] && CHUNKS=""

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# --- Singleton lock (sibling-aware when --chunks is used) ---
RUN_ID=""
if ! $FORCE; then
    RUN_ID="$(date +%Y%m%d-%H%M%S)"
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-gas-fees-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: gas-fees VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — Alchemy compute-units are shared per-key
and concurrent VMs with a NEW run-id burn budget without speedup. (A --chunks
fan-out launches all its siblings in ONE invocation under a single run-id; a
LATER launch while they run is the duplicate this guard refuses.)

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
[[ -z "$RUN_ID" ]] && RUN_ID="$(date +%Y%m%d-%H%M%S)"

# --- Helper: launch a single VM with the provided date range + chunk label ---
launch_one_vm() {
  local vm_name="$1"
  local start_date="$2"
  local end_date="$3"
  local run_id="$4"
  local chunk_id="${5:-}"

  # Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
  # routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly (the task
  # name is misleadingly category-specific; the routing is category-agnostic).
  local metadata="VM_TASK=cefi-backfill"
  metadata="${metadata},VM_SERVICE=market_tick_data_service"
  metadata="${metadata},VM_OPERATION=collect-gas-fees"
  metadata="${metadata},VM_ASSET_GROUP=DEFI"
  metadata="${metadata},VM_START_DATE=${start_date}"
  metadata="${metadata},VM_END_DATE=${end_date}"
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"
  metadata="${metadata},MANIFEST_PER_VM_SHARDS=true"
  metadata="${metadata},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
  metadata="${metadata},VM_NAME=${vm_name}"
  metadata="${metadata},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # Per-shard progress watchdog (backfill_vm_silent_worker_stall_watchdog P1): reset the stall
  # timer only on a real progress line. `sampled` = each per-block-batch gas sample (the 2026-06-19
  # incident froze MID block-sampling on a degraded POLYGON RPC); `Wrote` = a per-chain-per-date
  # parquet write. Markers verified against a live gas-fees run.log; =/space/comma-free (metadata-safe).
  metadata="${metadata},STALL_PROGRESS_REGEX=sampled|Wrote"

  local labels="purpose=mtds-gas-fees-backfill,env=${DEPLOYMENT_ENV},run-id=${run_id},managed-by=deployment-service"
  [[ -n "$chunk_id" ]] && labels="${labels},chunk=${chunk_id}"

  if $DRY_RUN; then
    echo "[dry-run] ${vm_name}: ${start_date}..${end_date} run-id=${run_id} chunk=${chunk_id:-single}"
    return 0
  fi

  # SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
  PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
  if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi
  echo "Launching $vm_name: gas-fees ${start_date}..${end_date} chunk=${chunk_id:-single} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$vm_name" \
      --project="$PROJECT" \
      --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      ${PROVISIONING_FLAGS} \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${metadata}" \
      --labels="$labels"
}

print_poll_hints() {
  local run_id="$1"
  echo ""
  echo "Poll completion: gcloud compute instances list --filter='labels.run-id=${run_id} AND status=RUNNING' --zones=${ZONE}"
  echo "GCS logs:        gsutil cat gs://${CODE_BUCKET}/vm-logs/<vm-name>/run.log"
  echo "Manifest:        gsutil cp gs://market-data-tick-defi-prd-central-element-323112/_index/availability_index.parquet /tmp/g.parquet  # shared DeFi manifest (all data_types); filter gas_fees below"
  echo "Inspect:         python -c \"import pandas as pd; df = pd.read_parquet('/tmp/g.parquet'); print(df[df.data_type=='gas_fees'].groupby('chain')['capture_status'].value_counts())\""
}

# --- Chunked fan-out (disjoint date windows; mirrors launch-sfi-backfill-vm.sh) ---
if [[ -n "$CHUNKS" ]]; then
  echo "Chunked gas-fees backfill: N=${CHUNKS} | run-id=${RUN_ID} | range=[${START_DATE}..${END_DATE}]"

  # Inline Python splitter — front-loaded remainder, bash 3.2 compatible (no readarray).
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
  for line in "${CHUNK_LINES[@]}"; do echo "  ${line}"; done
  echo ""

  i=0
  total_chunks=${#CHUNK_LINES[@]}
  for line in "${CHUNK_LINES[@]}"; do
    i=$((i + 1))
    IFS=$'\t' read -r cs ce <<< "$line"
    CHUNK_ID="${i}-of-${total_chunks}"
    VM_NAME="mtds-gas-fees-chunk-${i}of${total_chunks}-${RUN_ID}"
    launch_one_vm "$VM_NAME" "$cs" "$ce" "$RUN_ID" "$CHUNK_ID"
  done

  echo ""
  echo "Launched ${total_chunks} gas-fees chunk VMs with run-id=${RUN_ID}"
  $DRY_RUN && echo "[dry-run] No VMs were actually created."
  print_poll_hints "$RUN_ID"
else
  # --- Single-stream (default) ---
  VM_NAME="mtds-gas-fees-${RUN_ID}"
  launch_one_vm "$VM_NAME" "$START_DATE" "$END_DATE" "$RUN_ID"
  echo ""
  echo "VM launched: $VM_NAME"
  echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
  print_poll_hints "$RUN_ID"
  echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
fi
