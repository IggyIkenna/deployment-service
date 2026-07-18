#!/usr/bin/env bash
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod/catalog.parquet is confirmed durably converged for
#   BYBIT/KRAKEN-FUTURES/DERIBIT (see instrument_id_format_canonicalization_
#   2026_07_08.md) AND this launcher's last real run is verified GREEN.
#
# Launch a short-lived GCE VM that runs
# `instruments-service/scripts/cefi_durability_force_converge_2026_07_10.py`
# (quarantine stray inline backup files that were polluting the catalogue
# walk + re-derive instrument_key/margin_type on current by_date files).
#
# Usage:
#   bash launch-cefi-durability-force-converge-vm.sh --quarantine-backups --apply
#   bash launch-cefi-durability-force-converge-vm.sh --fix-by-date --apply
#   bash launch-cefi-durability-force-converge-vm.sh --quarantine-backups --fix-by-date --apply --workers 32
#   bash launch-cefi-durability-force-converge-vm.sh --fix-by-date --limit 20   # smoke test, no --apply = report-only
#   bash launch-cefi-durability-force-converge-vm.sh --env staging --fix-by-date --apply
#
# All args after the optional --force/--env are forwarded VERBATIM to the
# script (it is report-only by default; --apply is required to write).
#
# Singleton lock: refuses to launch if another cefi-durability-force-converge-*
# VM is RUNNING in the zone. Pass --force to bypass.
#
# Env overrides:
#   ON_DEMAND=true      opt out of the SPOT default (backfill/idempotent VMs → SPOT per HARD RULE)
#   MACHINE_TYPE=...     default e2-standard-8 (fix-by-date re-derives+rewrites ~14,900 parquet
#                         files across 3 venues; sized above the expected-universe-v2 launcher's
#                         default after that one OOM-killed on e2-standard-4 for a comparable job)
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - Quarantined backups → gs://instruments-store-cefi-{env}-{pid}/_migration_backup/
#       cefi_by_date_bak_relocation_2026_07_10/
#
# SSOT: unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DRY_RUN=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse named flags before forwarding the rest verbatim to the script.
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force) FORCE=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) break ;;
    esac
done

SCRIPT_ARGS="$*"
if [[ -z "$SCRIPT_ARGS" ]]; then
    echo "ERROR: at least one of --quarantine-backups / --fix-by-date is required (forwarded verbatim to the script)" >&2
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac
case "$DEPLOYMENT_ENV" in
    prod)    DEPLOYMENT_ENV_SHORT="prd" ;;
    staging) DEPLOYMENT_ENV_SHORT="staging" ;;
    dev)     DEPLOYMENT_ENV_SHORT="dev" ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) — the
# script is idempotent (quarantine skips already-moved files; fix-by-date backs up
# before rewriting and re-derives from already-captured columns), so preemption
# just resumes on restart. ON_DEMAND=true is the only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

# Singleton lock: only one cefi-durability-force-converge-* per zone at a time.
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^cefi-durability-force-converge-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: cefi-durability-force-converge VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate without --force.

Options:
  Inspect:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:    bash $0 --force ${SCRIPT_ARGS}

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
VM_NAME="cefi-durability-force-converge-${RUN_TS}"

BACKFILL_CMD="python scripts/cefi_durability_force_converge_2026_07_10.py ${SCRIPT_ARGS}"

echo "Launching $VM_NAME: cefi durability force-converge"
echo "  args:  $SCRIPT_ARGS"
echo "  env:   $DEPLOYMENT_ENV"

METADATA="VM_TASK=cefi-durability-force-converge"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=cefi-durability-force-converge"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},GCP_PROJECT_ID=${PROJECT}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: $VM_NAME"
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE_TYPE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      "${PROVISIONING_ARGS[@]}" \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      --labels=purpose=cefi-durability-force-converge,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:    gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/cefi-durability-force-converge.log'"
echo "GCS log: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:  gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify event stream (required — no fire-and-forget):"
echo "  STARTED within 60s, >= 1 progress event/hr, STOPPED/FAILED on exit."
