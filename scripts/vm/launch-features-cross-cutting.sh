#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch the SINGLETON features-cross-cutting VM that subscribes to every
# asset_group's `streaming.{ag}.features_computed` Redis Stream + emits
# cross-instrument features (e.g. `cross_instrument.lst_yield_vs_eth_spot` for
# `carry_staked_basis`, `cross_instrument.perp_funding_vs_spot_basis` for
# `ARBITRAGE_PRICE_DISPERSION`).
#
# Purpose: cross-cutting features need inputs from MULTIPLE asset_groups
# aligned at a common period_end boundary. Per
# `unified-trading-pm/codex/05-infrastructure/live-pipeline-architecture.md`
# § "Trigger cascade", the CrossCuttingFeaturesRunner uses WatermarkAlignmentFanin
# (UTL@`858f3c84`) with a default 500ms intra-zone grace window — emits when
# `min(stream_watermarks) > target_window_close + grace`, propagates degraded
# / STALE per Phase 6.2 worst-of rules.
#
# Bucket-naming SSOT: this launcher uses the (b+) env-aware shape codified
# 2026-05-11 per `bucket_name_ssot_canonicalisation_2026_05_10.md`.
# Cross-cutting features write to `features-cross-instrument-{env}-{pid}` +
# `features-multi-timeframe-{env}-{pid}` buckets (aliased shorter kind names
# per Q5 resolution: `features-xinstr` / `features-mtf` to fit AWS 63-char
# limit). Resolver: `unified_trading_library.cloud_interface.bucket_naming.resolve_bucket_name(
# cloud=, kind=, asset_group=, env=)`.
#
# Singleton-locked: refuses to launch if a same-prefix VM (`features-xc-*`) is
# already RUNNING in the zone. The cross-cutting consumer-group is intentionally
# singleton — every VM with a unique group-name would see every event, which
# multiplies compute without adding redundancy (the redundancy comes from VM
# auto-restart, not parallel consumers).
#
# Operational launch boundary: Phase 15 of
# `unified-trading-pm/plans/active/live_pipeline_mtds_mdps_features_2026_05_08.md`
# is the named runner for ACTUAL live VM bootstrap. This script ships code-
# ready as part of Phase 13.
#
# Usage:
#   bash launch-features-cross-cutting.sh                 # prod
#   bash launch-features-cross-cutting.sh --env staging   # staging cluster
#   bash launch-features-cross-cutting.sh --force         # bypass singleton lock
#
# Cost: e2-standard-8 ~24/7 — live consumer; auto-shutdown on STOPPED event only.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    *) echo "ERROR: unknown flag '$1'" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^features-xc-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: features-cross-cutting VM already running in $ZONE: $EXISTING
Singleton VM — only one cross-cutting consumer per environment.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --env ${DEPLOYMENT_ENV} --force

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
VM_NAME="features-xc-${RUN_TS}"

echo "Launching $VM_NAME: features-cross-cutting (singleton) env=${DEPLOYMENT_ENV}"

METADATA="VM_TASK=features-cross-cutting"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=cross_cutting_live"
METADATA="${METADATA},VM_MODE=live"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=false"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=features-cross-cutting,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/live.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gcloud storage ls gs://${PROJECT}-events/events/features_service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
