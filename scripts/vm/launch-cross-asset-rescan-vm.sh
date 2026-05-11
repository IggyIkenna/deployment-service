#!/usr/bin/env bash
# Launch a same-region GCE VM that runs the cross-asset manifest rescan +
# class-A auto-flips (per `manifest_cross_asset_rescan_design_2026_05_08.md`
# Phase 3.A + `manifest_schema_final_gate_2026_05_09.md` Phase 3.A).
#
# Purpose: walk every asset_group's availability manifest, detect drift
# between manifest rows + on-disk parquets (the 5 phantom-audit drift axes
# from 2026-05-04), auto-flip class A rows (single-source-of-truth manifest
# vs disk drift), and route class C rows (genuine dual-shape ambiguity) to
# the operator triage queue at gs://{pid}-rescan-triage/{run_id}/triage.jsonl.
#
# Why a same-region VM: cross-region GCS listing is 18× slower (~12 prefixes/sec
# from laptop vs 222/sec on asia-northeast1-c per CLAUDE.md "phantom audit"
# recipe). For the full cross-asset walk (~300-400 buckets × ~years of dates)
# this is the difference between an overnight job and a multi-day grind.
#
# Singleton lock (per CLAUDE.md "Singleton-locked launchers"): refuses to
# launch if any cross-asset-rescan-* VM is already running in the zone unless
# --force is passed. Two concurrent rescans race on the GCS list() pagination
# + the per-VM shard write-time CAS — second VM produces phantom-of-phantoms
# noise that takes manual cleanup. Same shape as launch-sfi-forward-poll.sh.
#
# Invocation inside the VM (assembled by setup-data-pipeline-vm.sh from
# metadata):
#   python -m instruments_service \
#     --operation cross_asset_rescan --mode batch \
#     --asset-group $VM_ASSET_GROUP \
#     [--apply-flips]  # default --dry-run; pass --apply for class-A auto-fix
#
# Output:
#   gs://{pid}-rescan-triage/{RUN_TS}/triage.jsonl — class C rows for operator
#   gs://{pid}-events/events/instruments-service/{date}/{vm-name}/ — progress
#
# Prerequisites:
#   - Tarballs uploaded to gs://deployment-scripts-{pid}/code/
#   - VM service account has read access to every asset_group bucket
#   - VM service account has write access to gs://{pid}-rescan-triage/
#   - instruments-service/scripts/cross_asset_rescan.py shipped (Phase 3.D)
#
# Usage:
#   bash launch-cross-asset-rescan-vm.sh                    # cross-asset-all, dry-run (default)
#   bash launch-cross-asset-rescan-vm.sh cefi                # cefi only, dry-run
#   bash launch-cross-asset-rescan-vm.sh --apply cefi        # cefi, apply flips
#   bash launch-cross-asset-rescan-vm.sh --force cefi        # bypass singleton lock
#   bash launch-cross-asset-rescan-vm.sh --tarball-from-local cefi  # use local working tree
#
# Cost: e2-standard-4 for ~2-8 hours depending on asset_group + apply mode.
#
# VM_PREFIX_TO_BUCKET registration: ensure `cross-asset-rescan-` prefix is in
# `deployment-service/scripts/vm/vm_zombie_watchdog.py` VM_PREFIX_TO_BUCKET
# before launching — without it the watchdog can't see this VM and zombie
# state goes undetected.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
# Rescan operates on env-tiered buckets — passing `--env staging` rescans only
# staging-tier manifests; default prod.
set -euo pipefail

FORCE=false
APPLY=false
TARBALL_MODE="prod"  # prod | local
ASSET_GROUP="cross_asset_all"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=true
      shift
      ;;
    --apply)
      APPLY=true
      shift
      ;;
    --tarball-from-local)
      TARBALL_MODE="local"
      shift
      ;;
    --env)
      DEPLOYMENT_ENV="$2"
      shift 2
      ;;
    cefi|defi|tradfi|sports|prediction|cross_asset_all)
      ASSET_GROUP="$1"
      shift
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      echo "Usage: $0 [--force] [--apply] [--tarball-from-local] [--env prod|staging|dev] [cefi|defi|tradfi|sports|prediction|cross_asset_all]" >&2
      exit 1
      ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# ── Singleton lock: concurrent rescans race on GCS list() pagination ──
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^cross-asset-rescan-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: cross-asset-rescan VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — concurrent rescans race on GCS list()
pagination + manifest CAS, producing phantom-of-phantoms noise that
takes manual cleanup.

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Stop:      gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:     bash $0 --force [args]
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="cross-asset-rescan-${RUN_TS}"

# Use unique VM_NAME for per-VM shard isolation (workspace rule per CLAUDE.md
# "Per-VM shard isolation for concurrent backfills"). The consolidator daemon
# merges _index/per_vm/{vm_name}.parquet into the canonical manifest with
# last-writer-wins on identical row_key — same machinery as normal MTDS / MDPS
# per-VM shard writes.
MODE_LABEL="dry-run"
if $APPLY; then
  MODE_LABEL="apply"
fi
echo "Launching $VM_NAME: cross-asset-rescan asset_group=${ASSET_GROUP} mode=${MODE_LABEL} tarball=${TARBALL_MODE}"

# ── Optional: refresh tarballs from local working tree ──
# CLAUDE.md "VM tarball deployment" rule — tarball-from-local for developer
# path with uncommitted edits; default uses tarballs already in GCS (built
# from origin/live-defi-rollout).
if [[ "$TARBALL_MODE" == "local" ]]; then
  echo "[--tarball-from-local] refreshing tarballs from local working tree..."
  REPO_ROOT="$(dirname "$(dirname "$(dirname "${BASH_SOURCE[0]}")")")/.."
  if [[ ! -x "${REPO_ROOT}/scripts/vm/create-code-tarballs.sh" ]]; then
    echo "ERROR: create-code-tarballs.sh not found at ${REPO_ROOT}/scripts/vm/" >&2
    exit 1
  fi
  bash "${REPO_ROOT}/scripts/vm/create-code-tarballs.sh" --all
fi

METADATA="VM_TASK=cross-asset-rescan"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=cross_asset_rescan"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# Per-VM shard isolation env vars (CLAUDE.md workspace rule). Without these,
# the writer's MultiWorkerWithoutShardIsolationError guard fires on launch.
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
# Concurrency tuning (per design doc + CLAUDE.md "phantom audit" rule:
# HTTP_POOL_SIZE = 2 × WORKERS to avoid the default-10 silent truncation
# under 64-worker concurrency).
METADATA="${METADATA},WORKERS=64"
METADATA="${METADATA},HTTP_POOL_SIZE=128"
# Apply mode toggle — the rescan script reads VM_APPLY_FLIPS at runtime
# to decide whether to flip class-A drift rows or just produce the triage
# report. Default false = dry-run; explicit --apply enables flips.
if $APPLY; then
  METADATA="${METADATA},VM_APPLY_FLIPS=true"
else
  METADATA="${METADATA},VM_APPLY_FLIPS=false"
fi
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=cross-asset-rescan,asset-group="${ASSET_GROUP}",mode="${MODE_LABEL}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Asset group: $ASSET_GROUP"
echo "Mode: $MODE_LABEL"
echo ""
echo "Logs: gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events: gsutil ls gs://${PROJECT}-events/events/instruments-service/\$(date +%Y-%m-%d)/${VM_NAME}/"
echo "Triage output (when complete): gsutil cat gs://${PROJECT}-rescan-triage/${RUN_TS}/triage.jsonl"
echo "Delete when done: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
