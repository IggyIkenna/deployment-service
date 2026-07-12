#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that backfills DeFi lending rate indices via
# the unified MTDS collect-lending-indices CLI.
#
# Purpose: re-ingest Aave V3 / Spark / Compound V3 rate indices from The
# Graph subgraphs for a date window. Writes to:
#   gs://lending-indices-central-element-323112/
#     raw_tick_data/by_date/day={D}/asset_group=defi/venue={V}/...
#
# Why this exists: 2026-05-05 schema fix (MTDS commit 2a9b638 / fba1afe)
# added ``liquidity_rate``, ``reserve_factor``, ``optimal_utilization``,
# ``variable_rate_slope1``, ``variable_rate_slope2``,
# ``base_variable_borrow_rate`` to the Aave V3 GraphQL query and parser.
# Historical lending_indices parquets pre-fix lack these columns;
# downstream lending_rates feature group falls back to UAC governance
# defaults until the historical window is re-backfilled with the new
# columns.
#
# Default: yesterday only (T-1). Pass two dates for an explicit window.
#
# Usage:
#   bash launch-mtds-lending-indices-backfill-vm.sh                       # T-1 single day
#   bash launch-mtds-lending-indices-backfill-vm.sh 2026-03-15 2026-04-14 # 30-day window
#   bash launch-mtds-lending-indices-backfill-vm.sh 2022-01-01 2026-04-14 # full history
#   bash launch-mtds-lending-indices-backfill-vm.sh --force ...           # bypass singleton
#   bash launch-mtds-lending-indices-backfill-vm.sh --lending-protocols morpho 2022-01-01 2026-04-14
#                                                                         # single-protocol scoped run
#
# Cost: e2-standard-4 + 50GB. ~5s/day per (protocol × chain) shard.
# The Graph paginates 1000 items per request; days with >1000 rate-index
# events take longer. 30-day backfill ~10-15min, full ~3 years × 3 chains
# × 3 protocols ~3-6hr.
#
# Singleton lock: refuses to launch if another mtds-lending-indices-* VM
# is RUNNING in the zone. The Graph API key is shared per-account and
# concurrent VMs split rate budget without speedup. --force bypasses for
# legitimate parallel runs.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
DRY_RUN=false
# Scopes the run to a protocol subset (CLI --lending-protocols, nargs='+').
# Empty = full _DEFAULT_PROTOCOLS list (unchanged default behavior).
LENDING_PROTOCOLS=""
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)     DRY_RUN=true; shift ;;
        --force)       FORCE=true; shift ;;
        --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand)   ON_DEMAND=true; shift ;;
        --lending-protocols) LENDING_PROTOCOLS="$2"; shift 2 ;;
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
Usage: $0 [--force] [--env prod|staging|dev] [--lending-protocols P1;P2] [START_DATE END_DATE]

Defaults to yesterday (T-1). Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').
Pass --lending-protocols to scope the run (semicolon-separated, e.g. 'morpho').
Default (unset) runs the full _DEFAULT_PROTOCOLS list.
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="50"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^mtds-lending-indices-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: lending-indices VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — The Graph API key budget is shared and
concurrent VMs split rate budget without speedup.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force ${START_DATE} ${END_DATE}
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="mtds-lending-indices-${RUN_TS}"

_SCOPE_MSG="${START_DATE}..${END_DATE}"
[[ -n "$LENDING_PROTOCOLS" ]] && _SCOPE_MSG="${_SCOPE_MSG} (protocols: ${LENDING_PROTOCOLS})"
echo "Launching $VM_NAME: DeFi lending indices ${_SCOPE_MSG}"

# Metadata follows the cefi-backfill convention — setup-data-pipeline-vm.sh
# routes VM_TASK=cefi-backfill through the generic MTDS CLI assembly.
METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-lending-indices"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
if [[ -n "$LENDING_PROTOCOLS" ]]; then
    METADATA="${METADATA},VM_LENDING_PROTOCOLS=${LENDING_PROTOCOLS}"
fi

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      --no-restart-on-failure \
      ${PROVISIONING_FLAGS} \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=mtds-lending-indices-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/market-tick-data-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Manifest:    gsutil cp gs://lending-indices-${PROJECT}/_index/availability_index.parquet /tmp/d.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/d.parquet'); print(df[df.data_type=='lending_indices']['capture_status'].value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
