#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a short-lived GCE VM that backfills governance proposals via the
# MTDS collect-governance-proposals CLI.
#
# Purpose: ingest Aave V3 / Compound V3 / Spark / Lido governance proposals
# from TheGraph subgraphs + Snapshot API for a date window. Writes to:
#   gs://market-data-tick-defi-{project}/
#     raw_tick_data/by_date/day={D}/category=defi/venue={PROTOCOL}-ETHEREUM/
#     instrument_type=spot_asset/data_type=governance_proposals/ticks.parquet
#
# Default: yesterday only (T-1). Pass two dates for an explicit window.
#
# Usage:
#   bash launch-governance-backfill-vm.sh                       # T-1 single day
#   bash launch-governance-backfill-vm.sh 2024-01-01 2026-05-17 # 2yr history
#   bash launch-governance-backfill-vm.sh --force ...           # bypass singleton
#
# Cost: e2-standard-4 + 50GB. The Graph API key shared per-account — singleton
# lock prevents accidental parallel launches that would split rate budget.
#
# Bucket-naming SSOT: env-aware shape per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
POSITIONAL=()
DRY_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --force) FORCE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        --on-demand)   ON_DEMAND=true; shift ;;
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
Usage: $0 [--force] [--env prod|staging|dev] [START_DATE END_DATE]

Defaults to yesterday (T-1). Pass two YYYY-MM-DD dates for an explicit window.
Pass --force to bypass the singleton lock.
Pass --env to override the env tier (default: \$DEPLOYMENT_ENV or 'prod').
EOF
    exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^governance-backfill-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: governance backfill VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — The Graph API key budget is shared and
concurrent VMs split rate budget without speedup.

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

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="governance-backfill-${RUN_TS}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME: governance proposals ${START_DATE}..${END_DATE} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"

METADATA="VM_TASK=cefi-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=collect-governance-proposals"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      ${PROVISIONING_FLAGS} \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
      # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
      # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
      # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
      # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
      # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
      --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=governance-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gsutil cp gs://market-data-tick-defi-${PROJECT}/_index/availability_index.parquet /tmp/d.parquet"
echo "Inspect:     python -c \"import pandas as pd; df = pd.read_parquet('/tmp/d.parquet'); print(df[df.data_type=='governance_proposals']['capture_status'].value_counts())\""
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
