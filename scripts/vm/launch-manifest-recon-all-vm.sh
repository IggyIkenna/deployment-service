#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11.
#
# Launch a short-lived GCE VM that runs ALL THREE manifest reconciliation
# scripts (scan-only / dry-run) for a given asset_group in sequence:
#
#   1. reconcile_phantom_manifest_rows_all.py      --dry-run
#   2. reconcile_expected_absence_reasons.py       (scan-only = omit --apply-flips)
#   3. reconcile_legacy_blank_to_typed_reason.py   (scan-only = omit --apply-flips)
#   4. (defi only) reconcile_phantom_manifest_rows_all.py --report-pyth-oracle-prices-ghost-failures
#      — dry-run report of PYTH oracle_prices stale day-level ghost attempted_failed rows
#      (pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md).
#
# Run on same-region (asia-northeast1-c) so GCS manifest reads are fast
# (CLAUDE.md "Manifest phantom audit" — cross-region listing is 18× slower).
#
# Why all three in one VM: the three reconcilers cover orthogonal dimensions
# of the manifest health check. Running them together yields a single
# per-asset-group audit snapshot instead of three separate VMs.
#   * Script 1: identifies captured rows whose parquet is missing (phantoms)
#   * Script 2: finds empty_confirmed rows with NULL error_reason (legacy pre-Phase-2.E)
#   * Script 3: finds empty_confirmed rows stamped with default reason that can now be
#               upgraded to a more-specific EXPECTED_* reason (Wave 3.X SSOTs added)
#
# Singleton lock is per-asset-group (matching prefix manifest-recon-{asset_group}-)
# so different asset_groups can run in parallel safely.
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Post-completion copy: gs://deployment-scripts-{pid}/recon-logs/YYYY-MM-DD/{vm_name}.log
#   - Per-script CSV reports (written locally on VM to /tmp/):
#       reconcile_phantom_manifest_rows_all  → no CSV (dry-run to stdout)
#       reconcile_expected_absence_reasons   → /tmp/recon-reasons-{ag}-{ts}.csv
#       reconcile_legacy_blank_to_typed_reason → /tmp/recon-legacy-typed-{ag}-{ts}.csv
#
# Usage:
#   bash launch-manifest-recon-all-vm.sh cefi
#   bash launch-manifest-recon-all-vm.sh defi
#   bash launch-manifest-recon-all-vm.sh tradfi
#   bash launch-manifest-recon-all-vm.sh sports
#   bash launch-manifest-recon-all-vm.sh prediction
#   bash launch-manifest-recon-all-vm.sh --force cefi     # bypass singleton lock
#   bash launch-manifest-recon-all-vm.sh --env staging cefi
#
# Cost: e2-standard-4 + 50GB (defi defaults to e2-highmem-8/64GB instead — see
#   below; override either via MACHINE_TYPE=... — the
#   merge_canonical_with_outstanding_shards manifest read scales with the CURRENT
#   count of outstanding per-VM shards across the fleet, not just corpus size; a
#   defi dry-run OOM-killed at 15.4GB RSS on 2026-07-28 under elevated concurrent-
#   backfill load, so 16GB is not a safe floor when other DeFi VMs are actively
#   writing shards; a follow-up e2-highmem-8/64GB run on the SAME corpus stalled
#   at 96% mem instead of completing — see
#   /plans/active/issues/reconcile_phantom_manifest_rows_all_defi_memory_footprint_2026_07_28.md
#   — so even this bumped default is not a guaranteed-sufficient floor for the
#   full 3-4-script chain, just the largest size this incident actually tested
#   without a hard kernel OOM-kill; the real fix is the column-pruned read path
#   tracked as that doc's P3 follow-on). Estimated runtime (read-only, same-region):
#   cefi/tradfi: ~45-60 min | defi: ~15 min | sports/prediction: ~10 min
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false

_positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
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

ASSET_GROUP="${1:-defi}"

case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
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
SINGLETON_PREFIX="manifest-recon-${ASSET_GROUP}-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: manifest-recon VM already running for ${ASSET_GROUP} in $ZONE: $EXISTING
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

# Build the chained backfill command.
#
# Python-substitution discipline (critical — learned from first run 2026-05-13):
# setup-data-pipeline-vm.sh runs:
#   FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
# This SINGLE-OCCURRENCE substitution matches the first `python ` in the string.
# Using an explicit "/path/to/venv/bin/python " prefix fails because the trailing
# `python ` in the path is itself the match target → double-path corruption
# ("/home/.../venv/bin//home/.../venv/bin/python").
#
# Correct pattern:
#   cmd1: bare `python` prefix → setup-script substitution handles it correctly.
#   cmd2+: use `\$PYTHON_BIN` (literal dollar-sign-PYTHON_BIN in metadata).
#          _launch_with_tee exports PYTHON_BIN="$VENV/bin/python" before calling
#          `bash -c "$FULL_CMD"`, so $PYTHON_BIN expands at runtime to the right
#          venv python. No setup-script substitution fires (string doesn't have `python `).
#
# SCRIPTS path: instruments-service-code.tar.gz is extracted to $WORKSPACE/instruments/
# (alias defined in setup-data-pipeline-vm.sh TARBALL_DIRS), so scripts live at:
SCRIPTS="/home/ikennaigboaka/workspace/instruments/scripts"
RECON_LOGS="gs://${CODE_BUCKET}/recon-logs/${RECON_DATE}"

# cmd1: bare `python` — setup script substitutes it to $VENV/bin/python correctly.
BACKFILL_CMD="python ${SCRIPTS}/reconcile_phantom_manifest_rows_all.py --asset-group ${ASSET_GROUP} --dry-run"
# cmd2+: \$PYTHON_BIN — literal in metadata, expanded by bash -c at runtime.
BACKFILL_CMD="${BACKFILL_CMD} && \$PYTHON_BIN ${SCRIPTS}/reconcile_expected_absence_reasons.py --asset-group ${ASSET_GROUP}"
BACKFILL_CMD="${BACKFILL_CMD} && \$PYTHON_BIN ${SCRIPTS}/reconcile_legacy_blank_to_typed_reason.py --asset-group ${ASSET_GROUP}"
# cmd4 (defi-only): PYTH oracle_prices stale day-level ghost attempted_failed
# rows (pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md) — dry-run
# report only; the flag itself hard-requires --asset-group defi.
if [[ "$ASSET_GROUP" == "defi" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} && \$PYTHON_BIN ${SCRIPTS}/reconcile_phantom_manifest_rows_all.py --asset-group defi --report-pyth-oracle-prices-ghost-failures"
fi
# Upload combined log to recon-logs/ after all 3 scripts complete (no-fail).
# /home/ikennaigboaka/logs/phantom-recon.log = $LOGS/$VM_TASK.log (VM_TASK=phantom-recon).
BACKFILL_CMD="${BACKFILL_CMD} && { gsutil cp /home/ikennaigboaka/logs/phantom-recon.log ${RECON_LOGS}/${VM_NAME}.log || true; }"

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=phantom-recon"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching $VM_NAME: all-3 manifest recon (dry-run/scan-only) for asset_group=${ASSET_GROUP}"
echo "  Script 1: reconcile_phantom_manifest_rows_all.py --dry-run"
echo "  Script 2: reconcile_expected_absence_reasons.py  (scan-only)"
echo "  Script 3: reconcile_legacy_blank_to_typed_reason.py (scan-only)"
echo "  Log dest: ${RECON_LOGS}/${VM_NAME}.log"

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
    --labels=purpose=manifest-recon-all,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Final log:   gsutil cat gs://${CODE_BUCKET}/recon-logs/${RECON_DATE}/${VM_NAME}.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
