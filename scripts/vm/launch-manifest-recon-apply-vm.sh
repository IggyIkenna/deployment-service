#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11.
#
# Launch a short-lived GCE VM that runs THREE manifest reconciliation scripts
# in APPLY mode (writes back to manifest) for a given asset_group:
#
#   1. reconcile_phantom_manifest_rows_all.py      --unphantom
#   2. reconcile_expected_absence_reasons.py       --apply-flips
#   3. reconcile_legacy_blank_to_typed_reason.py   --apply-flips
#   4. (defi only) reconcile_phantom_manifest_rows_all.py --report-pyth-oracle-prices-ghost-failures --apply
#      — deletes confirmed PYTH oracle_prices stale day-level ghost attempted_failed rows
#      (pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md). Run the dry-run launcher
#      first and confirm the predicate before launching this.
#
# Script 3 was previously excluded due to a classifier API kwarg mismatch
# (classify_blank_reason_row() fixture_manifest kwarg) — resolved 2026-05-14,
# issue archived at plans/archive/issues/classify_blank_reason_fixture_manifest_kwarg_2026_05_13.md.
#
# Run on same-region (asia-northeast1-c) so GCS manifest reads are fast
# (CLAUDE.md "Manifest phantom audit" — cross-region listing is 18× slower).
#
# Requires:
#   - MANIFEST_PER_VM_SHARDS=true (per reconcile_expected_absence_reasons output)
#   - VM_NAME set to unique tag (used by reconcilers for per-VM shard isolation)
#   Both are inlined in BACKFILL_CMD at launch time (known before VM starts).
#
# Singleton lock is per-asset-group (matching prefix manifest-recon-apply-{asset_group}-)
# so different asset_groups can run in parallel safely.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Post-completion copy: gs://deployment-scripts-{pid}/recon-logs/YYYY-MM-DD/{vm_name}.log
#   - Script 2 CSV report (written locally on VM to /tmp/):
#       reconcile_expected_absence_reasons       → /tmp/recon-reasons-{ag}-{ts}.csv
#   - Script 3 CSV report (written locally on VM to /tmp/):
#       reconcile_legacy_blank_to_typed_reason   → /tmp/recon-legacy-typed-{ag}-{ts}.csv
#
# Gate dependencies:
#   - Gate 1 (expected_unattempted_propagation_chain Phase 3+4+2.A) must be COMPLETE.
#     Gate 1 fired 2026-05-13 07:30 UTC per _agent_pings.md.
#   - Dry-run (Gate 3) must be COMPLETE. Completed 2026-05-13 ~09:00 UTC.
#
# Usage:
#   bash launch-manifest-recon-apply-vm.sh cefi
#   bash launch-manifest-recon-apply-vm.sh defi
#   bash launch-manifest-recon-apply-vm.sh tradfi
#   bash launch-manifest-recon-apply-vm.sh --force cefi   # bypass singleton lock
#   bash launch-manifest-recon-apply-vm.sh --env staging cefi
#
# Cost: e2-standard-4 + 50GB (defi defaults to e2-highmem-8/64GB instead — see
#   launch-manifest-recon-all-vm.sh's Cost comment for the 2026-07-28 OOM/stall
#   finding and the still-not-guaranteed-sufficient caveat; override via
#   MACHINE_TYPE=...). Estimated runtime (apply mode, same-region):
#   cefi: ~30-60 min (2,223 phantom flips + 3,146 null-reason stamps)
#   defi: ~15-20 min (1,298 phantom flips, 0 null-reason)
#   tradfi: ~20-30 min (3,976 phantom flips, 0 null-reason, 5,212 legacy-blank flips)
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

ASSET_GROUP="${1:-cefi}"

case "$ASSET_GROUP" in
    cefi|defi|tradfi) ;;
    sports|prediction) echo "ERROR: sports/prediction apply-flips blocked pending Gate 4 + axis extension. Only cefi/defi/tradfi authorized." >&2; exit 2 ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# defi's manifest read materializes the full wide schema and has OOM-killed at
# 16GB (e2-standard-4) — default it to a bigger box per
# reconcile_phantom_manifest_rows_all_defi_memory_footprint_2026_07_28.md; other
# asset_groups are unaffected and keep the smaller default.
_DEFAULT_MACHINE_TYPE="e2-standard-4"
[ "$ASSET_GROUP" = "defi" ] && _DEFAULT_MACHINE_TYPE="e2-highmem-8"
MACHINE_TYPE="${MACHINE_TYPE:-$_DEFAULT_MACHINE_TYPE}"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# Singleton check per-asset-group (different asset_groups may run in parallel).
SINGLETON_PREFIX="manifest-recon-apply-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: manifest-recon-apply VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP}

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If, after that check, it is genuinely stale, construct the delete
command yourself — do not copy-paste one from this message — per infra.md
STEP 0.65's 3-signal staleness check (heartbeat age, run.log tail, manifest
mtime); when unsure, escalate instead of deleting.
EOF
        exit 1
    fi
fi

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"
RECON_DATE="$(date -u +%Y-%m-%d)"

# Python-substitution discipline (critical — learned from first run 2026-05-13):
# setup-data-pipeline-vm.sh runs:
#   FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
# This SINGLE-OCCURRENCE substitution matches the first `python ` in the string.
#
# cmd1: bare `python` prefix → setup-script substitution handles it correctly.
# cmd2+: use `\$PYTHON_BIN` (literal dollar-sign-PYTHON_BIN in metadata).
#         _launch_with_tee exports PYTHON_BIN="$VENV/bin/python" before calling
#         `bash -c "$FULL_CMD"`, so $PYTHON_BIN expands at runtime to the right
#         venv python. No setup-script substitution fires.
#
# MANIFEST_PER_VM_SHARDS + VM_NAME: inlined in cmd1 — the reconciler reads these
# env vars to enable per-VM shard isolation during apply-flips. VM_NAME is the
# unique identifier for this run (known at launch time, substituted here).
SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"
RECON_LOGS="gs://${CODE_BUCKET}/recon-logs/${RECON_DATE}"

# cmd1: phantom flip (Script 1) — bare `python`, MANIFEST_PER_VM_SHARDS inlined.
# Note: reconcile_phantom_manifest_rows_all.py uses --unphantom (not --apply) for apply mode.
BACKFILL_CMD="MANIFEST_PER_VM_SHARDS=true VM_NAME=${VM_NAME} python ${SCRIPTS}/reconcile_phantom_manifest_rows_all.py --asset-group ${ASSET_GROUP} --unphantom"
# cmd2: null-reason stamp (Script 2) — \$PYTHON_BIN expanded at runtime.
BACKFILL_CMD="${BACKFILL_CMD} && MANIFEST_PER_VM_SHARDS=true VM_NAME=${VM_NAME} \$PYTHON_BIN ${SCRIPTS}/reconcile_expected_absence_reasons.py --asset-group ${ASSET_GROUP} --apply-flips"
# cmd3: legacy-blank reclassification (Script 3) — classifier kwarg issue resolved 2026-05-14.
BACKFILL_CMD="${BACKFILL_CMD} && MANIFEST_PER_VM_SHARDS=true VM_NAME=${VM_NAME} \$PYTHON_BIN ${SCRIPTS}/reconcile_legacy_blank_to_typed_reason.py --asset-group ${ASSET_GROUP} --apply-flips"
# cmd4 (defi-only): PYTH oracle_prices stale day-level ghost attempted_failed
# rows (pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md) — APPLY
# (deletes the confirmed ghost set); the flag itself hard-requires
# --asset-group defi. Only launch after a dry-run pass has confirmed the
# predicate on this same asset_group.
if [[ "$ASSET_GROUP" == "defi" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} && MANIFEST_PER_VM_SHARDS=true VM_NAME=${VM_NAME} \$PYTHON_BIN ${SCRIPTS}/reconcile_phantom_manifest_rows_all.py --asset-group defi --report-pyth-oracle-prices-ghost-failures --apply"
fi
# Upload combined log to recon-logs/ after all scripts complete (no-fail).
BACKFILL_CMD="${BACKFILL_CMD} && { gsutil cp /home/ikennaigboaka/logs/phantom-recon.log ${RECON_LOGS}/${VM_NAME}.log || true; }"

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=phantom-recon-apply"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching $VM_NAME: apply-flips manifest recon (Scripts 1+2+3) for asset_group=${ASSET_GROUP}"
echo "  Script 1: reconcile_phantom_manifest_rows_all.py --unphantom"
echo "  Script 2: reconcile_expected_absence_reasons.py  --apply-flips"
echo "  Script 3: reconcile_legacy_blank_to_typed_reason.py --apply-flips"
echo "  Log dest: ${RECON_LOGS}/${VM_NAME}.log"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=manifest-recon-apply,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Final log:   gsutil cat gs://${CODE_BUCKET}/recon-logs/${RECON_DATE}/${VM_NAME}.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
