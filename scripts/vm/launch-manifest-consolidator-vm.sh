#!/usr/bin/env bash
# Launch a long-lived GCE VM that polls the UTL manifest consolidator on
# every asset_group bucket every N seconds.
#
# Purpose: keep ``_index/availability_index.parquet`` fresh in each
# instruments-store-* bucket so the UTL reader's 120s freshness fallback
# doesn't truncate readers (deployment-api data-status, FSS reader, etc.)
# to a per-VM-only view. Replaces the manual ``consolidate(bucket)`` calls
# we'd otherwise have to run by hand during active backfill operations.
#
# Architecture note (manifest-429 phase 6/7): VMs writing to the manifest
# write to ``_index/per_vm/<vm_name>.parquet`` shards (avoids canonical
# write contention). A consolidator periodically reads canonical + all
# per_vm shards, dedups on the row-key tuple, writes back to canonical.
# Without a scheduled consolidator the canonical drifts stale and the
# UTL reader's freshness fallback kicks in (returns per_vm shards only,
# missing the rest of history). This VM closes that gap.
#
# Why VM not Cloud Run Job: Plans 12 + 13 blockers on deployment-service
# image build + UTL base image. VM path uses the tarball infra (UAC + UTL +
# deployment-service already on GCS) and runs the consolidator's CLI
# (``python -m unified_trading_library.manifest_consolidator --bucket X --once``)
# in a bash poll loop.
#
# SSOT:
#   - codex/02-data/sports-schema-paths.md "Manifest consolidator + per_vm shard merge mechanics"
#   - codex/05-infrastructure/vm-tarball-deployment.md
#   - memory: reference_manifest_consolidator_per_vm_merge.md
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from
# VM_TASK=manifest-consolidator-poll):
#
#   while true; do
#     for bucket in $BUCKETS; do
#       python -m unified_trading_library.manifest_consolidator --bucket $bucket --once
#     done
#     sleep $POLL_INTERVAL
#   done
#
# Default buckets: instruments-store-{sports,cefi,defi,tradfi,prediction}-PROJECT_ID
# Default poll interval: 60 seconds (matches the UTL reader freshness threshold).
#
# Prerequisites:
#   - UAC + UTL tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --all
#     (UTL needs the 2026-04-29 BlobMetadata fix — pre-fix the consolidator
#     reports shards_scanned: 0 and is a no-op.)
#   - Service account on the VM has storage.objectAdmin on every target bucket.
#
# Usage:
#   bash launch-manifest-consolidator-vm.sh                                      # all 5 asset_group buckets, 60s
#   bash launch-manifest-consolidator-vm.sh --interval 30                        # 30s poll
#   bash launch-manifest-consolidator-vm.sh --buckets instruments-store-sports-...  # custom subset
#   bash launch-manifest-consolidator-vm.sh --force                              # bypass singleton lock
#   bash launch-manifest-consolidator-vm.sh --dry-run                            # show what would launch, no-op
#
# Cost: e2-standard-2 24/7 ≈ $25/mo (asia-northeast1). e2-small (~$12/mo)
# was tried 2026-04-29 but OOM'd on the sports manifest (1.97M rows merged
# in pandas exceeds 2 GB RAM). e2-standard-2 has 4 GB which handles the
# largest current manifest (sports / market-data-tick-cefi). If a future
# bucket exceeds e2-standard-2, bump to e2-standard-4 (~$50/mo).
#
# Singleton lock: refuses to launch if any manifest-consolidator-* VM is
# already RUNNING in the zone. Multiple consolidators would race on the
# sentinel-lock blob; the lock makes them mostly correct (only one wins
# per bucket per cycle) but wastes API calls and adds tail latency. One
# VM per zone is sufficient.
set -euo pipefail

FORCE=false
DRY_RUN=false
POLL_INTERVAL=60
BUCKETS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --interval) POLL_INTERVAL="$2"; shift 2 ;;
    --buckets) BUCKETS="$2"; shift 2 ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 1 ;;
  esac
done

if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || [[ "$POLL_INTERVAL" -lt 30 ]]; then
  echo "ERROR: --interval must be integer >= 30 (got $POLL_INTERVAL)" >&2
  exit 1
fi

PROJECT="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"
ZONE="${VM_ZONE:-asia-northeast1-c}"
CODE_BUCKET="${CODE_BUCKET:-deployment-scripts-${PROJECT}}"

# Default buckets: every bucket family that uses ManifestWriter v5 — reference
# data (instruments-store-*), market tick + processed data (market-data-tick-*),
# per-data_type DeFi buckets (dex-pools / dex-swaps / evm-defi / gas-fees /
# lending-indices / liquidations / lst-rates / oracle-prices / perp-funding /
# solana-defi — these are exactly the 10 keys in deployment-api's
# _BUCKET_CATEGORY_OVERRIDES + _MTDS_DEFI_SUB_DIMENSIONS, each written via
# get_write_bucket_name(<kind>) in the corresponding MTDS DeFi handler),
# strategy stores (strategy-store-*), and derived features (features-sports-*).
# Buckets without an ``_index/availability_index.parquet`` are no-ops at the
# consolidator's first probe; safe to include broadly. Add new bucket families
# here when they adopt the manifest pattern.
if [[ -z "$BUCKETS" ]]; then
  BUCKETS="instruments-store-sports-${PROJECT}"
  BUCKETS="${BUCKETS},instruments-store-cefi-${PROJECT}"
  BUCKETS="${BUCKETS},instruments-store-defi-${PROJECT}"
  BUCKETS="${BUCKETS},instruments-store-tradfi-${PROJECT}"
  BUCKETS="${BUCKETS},instruments-store-prediction-${PROJECT}"
  BUCKETS="${BUCKETS},market-data-tick-sports-${PROJECT}"
  BUCKETS="${BUCKETS},market-data-tick-cefi-${PROJECT}"
  BUCKETS="${BUCKETS},market-data-tick-defi-${PROJECT}"
  BUCKETS="${BUCKETS},market-data-tick-tradfi-${PROJECT}"
  BUCKETS="${BUCKETS},market-data-tick-prediction-${PROJECT}"
  # Per-data_type DeFi buckets — deployment-api's _BUCKET_CATEGORY_OVERRIDES
  # routes each DeFi data_type to its own dedicated bucket, distinct from the
  # asset-group canonical market-data-tick-defi-*. The full set (= the 10 keys
  # in _BUCKET_CATEGORY_OVERRIDES + _MTDS_DEFI_SUB_DIMENSIONS) is:
  # dex-pools / dex-swaps / evm-defi / gas-fees / lending-indices / liquidations
  # / lst-rates / oracle-prices / perp-funding / solana-defi. Each is written
  # via get_write_bucket_name(<kind>) in the matching MTDS handler
  # (dex_pools_handler.py / dex_swaps_handler.py / evm_defi_handler.py /
  # gas_fee_handler.py / lending_indices_handler.py / etc.). Without these in
  # the poll list, those buckets' canonical _index/availability_index.parquet
  # drifts stale relative to per-VM shards → data-status shows wrong DeFi
  # coverage (e.g. 2026-05-08 lending-indices VM run captured LINEA/BSC AAVEV3
  # post-launch + flipped pre-launch days to empty_confirmed in its per-VM shard,
  # but the canonical kept showing attempted_failed because nothing consolidated).
  # First 8 added 2026-05-11 (slot 3, defi_master Priority #5); dex-pools +
  # liquidations added 2026-05-11 (slot 6, consolidator poll-list completeness
  # audit — they were in _BUCKET_CATEGORY_OVERRIDES + written by MTDS but missed
  # from the poll list). solana-defi is legacy (the Solana handler now writes to
  # dex_pools/perp_funding/lst_rates buckets per check_solana_defi_paths.py) —
  # kept as a no-op probe for any orphaned legacy data.
  BUCKETS="${BUCKETS},dex-pools-${PROJECT}"
  BUCKETS="${BUCKETS},dex-swaps-${PROJECT}"
  BUCKETS="${BUCKETS},evm-defi-${PROJECT}"
  BUCKETS="${BUCKETS},gas-fees-${PROJECT}"
  BUCKETS="${BUCKETS},lending-indices-${PROJECT}"
  BUCKETS="${BUCKETS},liquidations-${PROJECT}"
  BUCKETS="${BUCKETS},lst-rates-${PROJECT}"
  BUCKETS="${BUCKETS},oracle-prices-${PROJECT}"
  BUCKETS="${BUCKETS},perp-funding-${PROJECT}"
  BUCKETS="${BUCKETS},solana-defi-${PROJECT}"
  BUCKETS="${BUCKETS},features-sports-${PROJECT}"
  BUCKETS="${BUCKETS},strategy-store-cefi-${PROJECT}"
  BUCKETS="${BUCKETS},strategy-store-sports-${PROJECT}"
  BUCKETS="${BUCKETS},strategy-store-defi-${PROJECT}"
fi

# Singleton lock — one consolidator per zone is enough.
EXISTING=$(gcloud compute instances list \
  --project="$PROJECT" \
  --zones="$ZONE" \
  --filter='name~"^manifest-consolidator-" AND status=RUNNING' \
  --format='value(name)' 2>/dev/null || true)
if [[ -n "$EXISTING" ]] && ! $FORCE; then
  cat <<EOF >&2
ERROR: manifest-consolidator already running in $ZONE: $EXISTING
A single consolidator daemon is sufficient — multiple would just wait on the
sentinel-lock and waste API calls. Pass --force only for legitimate parallel
investigation (and stop the existing one when done).

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force
EOF
  exit 1
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="manifest-consolidator-${RUN_TS}"

echo "Launching $VM_NAME: manifest consolidator daemon (poll=${POLL_INTERVAL}s)"
echo "  buckets:"
IFS=',' read -ra _BUCKET_ARRAY <<<"$BUCKETS"
for b in "${_BUCKET_ARRAY[@]}"; do
  echo "    - $b"
done

if $DRY_RUN; then
  echo ""
  echo "DRY-RUN: would create e2-small VM in $ZONE with metadata:"
  echo "  VM_TASK=manifest-consolidator-poll"
  echo "  VM_POLL_INTERVAL=$POLL_INTERVAL"
  echo "  VM_BUCKETS=$BUCKETS"
  exit 0
fi

# Encode buckets with ':' separator since gcloud --metadata uses ',' to split
# top-level key=value pairs. The setup script converts ':' back to spaces.
BUCKETS_COLON="${BUCKETS//,/:}"

# Use the alternate separator syntax (^|^) so we can include commas inside
# the BUCKETS value. See: gcloud topic escaping.
METADATA="^|^startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA}|VM_TASK=manifest-consolidator-poll"
METADATA="${METADATA}|VM_POLL_INTERVAL=${POLL_INTERVAL}"
METADATA="${METADATA}|VM_BUCKETS=${BUCKETS_COLON}"
# No VM_SHUTDOWN_ON_COMPLETION — this is a daemon.

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --scopes=cloud-platform \
  --metadata="${METADATA}" \
  --labels=purpose=manifest-consolidator,run-ts="${RUN_TS}",tier=daemon

echo ""
echo "VM launched: $VM_NAME"
echo "Zone: $ZONE"
echo "Machine type: e2-small (~\$12/mo)"
echo ""
echo "Logs:"
echo "  SSH tail:   gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f ~/logs/manifest-consolidator.log'"
echo "  GCS tail:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "First consolidation cycle expected within ~3 min of boot (VM setup ~2 min + first poll)."
echo ""
echo "Stop daemon:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
