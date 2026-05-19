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
#   bash launch-cross-asset-rescan-vm.sh                         # cross-asset-all, dry-run
#   bash launch-cross-asset-rescan-vm.sh cefi                    # cefi only, dry-run
#   bash launch-cross-asset-rescan-vm.sh --apply cefi            # cefi, sequential 4-pass apply
#   bash launch-cross-asset-rescan-vm.sh --apply --pass 1 cefi   # cefi, pass 1 only (resume)
#   bash launch-cross-asset-rescan-vm.sh --force cefi            # bypass singleton lock
#   bash launch-cross-asset-rescan-vm.sh --tarball-from-local cefi  # use local working tree
#
# Pass ordering (--apply without --pass launches all 4 sequentially with SERIAL gate):
#   --pass 1  instruments,venue_trading_calendar (SERIAL root — must complete before pass 2)
#   --pass 2  MTDS raw market data (parallel across asset_groups, serial after pass 1)
#   --pass 3  MDPS processed outputs (serial after pass 2)
#   --pass 4  features services (serial after pass 3)
# Dry-run mode (default): no ordering enforced; --pass still scopes the data_types filter.
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
PASS="all"  # all | 1 | 2 | 3 | 4

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
    --pass)
      PASS="$2"
      case "$PASS" in
        1|2|3|4|all) ;;
        *) echo "ERROR: --pass must be 1|2|3|4|all (got: $PASS)" >&2; exit 1 ;;
      esac
      shift 2
      ;;
    cefi|defi|tradfi|sports|prediction|cross_asset_all)
      ASSET_GROUP="$1"
      shift
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      echo "Usage: $0 [--force] [--apply] [--pass 1|2|3|4|all] [--tarball-from-local] [--env prod|staging|dev] [cefi|defi|tradfi|sports|prediction|cross_asset_all]" >&2
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
CODE_BUCKET="deployment-scripts-${PROJECT}"

# Poll until the given VM reaches TERMINATED (self-deleted or completed).
# Used for sequential multi-pass orchestration — pass N+1 must not start
# until pass N's VM has stopped (SERIAL gate per reconciliation ordering).
# Max wait 8h covers the largest cefi run observed (~55 min at 41 prefix/sec).
wait_for_vm_stopped() {
  local vm="$1"
  local deadline=$(( $(date +%s) + 28800 ))
  echo "Waiting for $vm to reach TERMINATED (serial gate, max 8h)..."
  while [[ $(date +%s) -lt $deadline ]]; do
    local status
    status=$(gcloud compute instances describe "$vm" \
      --zone="$ZONE" --project="$PROJECT" \
      --format="value(status)" 2>/dev/null) || {
        echo "$vm is gone (deleted after completion) — continuing."
        return 0
    }
    if [[ "$status" == "TERMINATED" ]]; then
      echo "$vm TERMINATED — serial gate cleared."
      return 0
    fi
    echo "  $vm status=$status — sleeping 30s..."
    sleep 30
  done
  echo "ERROR: $vm did not TERMINATE within 8h — aborting sequential run." >&2
  return 1
}

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

MODE_LABEL="dry-run"
if $APPLY; then
  MODE_LABEL="apply"
fi

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

APPLY_ARG=""
if $APPLY; then
  APPLY_ARG=" --apply"
fi
# Direct script invocation via VM_BACKFILL_CMD — bypasses the
# `python -m instruments_service --operation X` CLI dispatch, which only
# registers the `instruments` operation. The rescan is a one-shot orchestrator
# on top of the existing phantom-audit reconciler, not a payload-processor in
# the UnifiedServiceHandler shape — so the launcher invokes the script entry
# point directly via setup-data-pipeline-vm.sh's VM_BACKFILL_CMD branch
# (which rewrites `python ` to `$VENV/bin/python ` then runs the cmd verbatim).
# Path: instruments-service tarball extracts to $WORKSPACE/instruments per
# TARBALL_DIRS["instruments-service-code"]="instruments"; the tarball is
# created with `-C $repo_path .` so scripts/ ends up at
# $WORKSPACE/instruments/scripts/. Same pattern as launch-defi-phantom-recon-vm.sh.
# Fix shipped 2026-05-11 after `cross-asset-rescan-20260511-153940` failed at
# argparse with `--operation: invalid choice: 'cross_asset_rescan'`.
RESCAN_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/cross_asset_rescan.py"
BACKFILL_CMD="python ${RESCAN_SCRIPT} --asset-group ${ASSET_GROUP}${APPLY_ARG}"

# ── Single-VM launch helper (used by both single-pass and sequential modes) ──
# Args: $1=pass_num (1|2|3|4|all)  $2=vm_name (pre-computed unique tag)
launch_single_vm() {
  local pass_num="$1"
  local vm_name="$2"

  # Use unique VM_NAME for per-VM shard isolation (workspace rule per CLAUDE.md
  # "Per-VM shard isolation for concurrent backfills"). The consolidator daemon
  # merges _index/per_vm/{vm_name}.parquet into the canonical manifest with
  # last-writer-wins on identical row_key — same machinery as normal MTDS / MDPS
  # per-VM shard writes.
  local metadata="VM_TASK=cross-asset-rescan"
  metadata="${metadata},VM_SERVICE=instruments_service"
  metadata="${metadata},VM_BACKFILL_CMD=${BACKFILL_CMD}"
  metadata="${metadata},VM_ASSET_GROUP=${ASSET_GROUP}"
  metadata="${metadata},VM_NAME=${vm_name}"
  metadata="${metadata},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # RESCAN_PASS is read by cross_asset_rescan.py to pass --data-types to the
  # reconciler (pass ordering per manifest_cross_asset_rescan_design §"ordering").
  metadata="${metadata},RESCAN_PASS=${pass_num}"
  # Per-VM shard isolation env vars (CLAUDE.md workspace rule). Without these,
  # the writer's MultiWorkerWithoutShardIsolationError guard fires on launch.
  metadata="${metadata},MANIFEST_PER_VM_SHARDS=true"
  # Concurrency tuning (per design doc + CLAUDE.md "phantom audit" rule:
  # HTTP_POOL_SIZE = 2 × WORKERS to avoid the default-10 silent truncation
  # under 64-worker concurrency).
  metadata="${metadata},WORKERS=64"
  metadata="${metadata},HTTP_POOL_SIZE=128"
  # Apply mode toggle — the rescan script reads VM_APPLY_FLIPS at runtime
  # to decide whether to flip class-A drift rows or just produce the triage
  # report. Default false = dry-run; explicit --apply enables flips. The
  # env var is read by the script (script docstring lines 35-37) in addition
  # to the --apply CLI flag set in VM_BACKFILL_CMD above; both ways agree.
  if $APPLY; then
    metadata="${metadata},VM_APPLY_FLIPS=true"
  else
    metadata="${metadata},VM_APPLY_FLIPS=false"
  fi
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"

  local run_ts="${vm_name##*-}"  # last segment of vm_name is the timestamp
  echo "Launching $vm_name (pass=${pass_num} asset_group=${ASSET_GROUP} mode=${MODE_LABEL})"
  gcloud compute instances create "$vm_name" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${metadata}" \
    --labels=purpose=cross-asset-rescan,asset-group="${ASSET_GROUP}",mode="${MODE_LABEL}",env="${DEPLOYMENT_ENV}",pass="${pass_num}"

  echo ""
  echo "VM launched: $vm_name"
  echo "Logs: gcloud compute ssh $vm_name --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/backfill.log'"
  echo "GCS log tail: gsutil cat gs://${CODE_BUCKET}/vm-logs/${vm_name}/run.log"
  echo "Events: gsutil ls gs://${PROJECT}-events/events/instruments-service/\$(date +%Y-%m-%d)/${vm_name}/"
  echo "Triage output (when complete): gsutil cat gs://${PROJECT}-rescan-triage/${run_ts}/triage.jsonl"
  echo "Delete when done: gcloud compute instances delete $vm_name --zone=$ZONE --quiet"
  echo ""
}

# ── Orchestration: sequential 4-pass (apply) or single VM (dry-run / --pass N) ──
#
# Serial gate (manifest_cross_asset_rescan_design_2026_05_08.md §"ordering"):
#   "dry-run reads commute; --apply-flips requires strict ordering 1→2→3→4."
if $APPLY && [[ "$PASS" == "all" ]]; then
  echo "Sequential 4-pass --apply run: asset_group=${ASSET_GROUP} env=${DEPLOYMENT_ENV}"
  echo "SERIAL gate: each pass waits for TERMINATED before starting the next."
  echo ""
  for p in 1 2 3 4; do
    vm_ts="$(date +%Y%m%d-%H%M%S)"
    vm_name="cross-asset-rescan-p${p}-${vm_ts}"
    launch_single_vm "$p" "$vm_name"
    if [[ $p -lt 4 ]]; then
      wait_for_vm_stopped "$vm_name"
    fi
  done
  echo "All 4 passes complete."
else
  # Single VM: dry-run (no ordering needed) or manual --pass N resume/override.
  vm_ts="$(date +%Y%m%d-%H%M%S)"
  if [[ "$PASS" == "all" ]]; then
    vm_name="cross-asset-rescan-${vm_ts}"
  else
    vm_name="cross-asset-rescan-p${PASS}-${vm_ts}"
  fi
  launch_single_vm "$PASS" "$vm_name"
fi
