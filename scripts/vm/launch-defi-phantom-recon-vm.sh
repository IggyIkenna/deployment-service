#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a short-lived GCE VM that runs the multi-asset-group phantom audit
# (`instruments-service/scripts/reconcile_phantom_manifest_rows_all.py`) for a
# given asset_group, in same-region (asia-northeast1-c) so the GCS listing
# walk is fast (CLAUDE.md "Manifest phantom audit" — cross-region listing is
# 18× slower).
#
# Why this exists: phantom rows accumulate in the availability manifest when
# adapters record `captured` for a shard but the parquet doesn't exist at the
# canonical GCS path (stale rescan output, schema migration churn, broken
# denorm). The orchestrator's `_should_skip_shard` trusts the manifest, so
# phantoms cause permanent skip — every backfill VM exits doing nothing for
# the affected shards. Periodic recon catches this.
#
# Default: DRY-RUN only (read-only, no manifest writes). Pass --apply to
# actually flip phantom rows to attempted_failed (see script docstring).
#
# Usage:
#   bash launch-defi-phantom-recon-vm.sh                          # defi --dry-run
#   bash launch-defi-phantom-recon-vm.sh defi --apply             # defi, write back
#   bash launch-defi-phantom-recon-vm.sh cefi                     # cefi --dry-run
#   bash launch-defi-phantom-recon-vm.sh sports                   # sports --dry-run
#   bash launch-defi-phantom-recon-vm.sh --force defi             # bypass singleton
#   MACHINE_TYPE=e2-highmem-4 bash launch-defi-phantom-recon-vm.sh cefi --apply
#                                                                  # bigger box (default
#                                                                  # e2-standard-4/16GB OOMs
#                                                                  # loading the full cefi
#                                                                  # manifest -- see
#                                                                  # cefi_residual_followups_after_honest_done_2026_07_17.md)
#   VENUES=HYPERLIQUID bash launch-defi-phantom-recon-vm.sh cefi --apply
#                                                                  # scope the audit to one
#                                                                  # (or comma-separated) venue(s)
#   bash launch-defi-phantom-recon-vm.sh defi --report-pyth-oracle-prices-ghost-failures
#                                                                  # PYTH oracle_prices stale
#                                                                  # day-level ghost-failure
#                                                                  # report (dry-run only)
#   bash launch-defi-phantom-recon-vm.sh defi --report-pyth-oracle-prices-ghost-failures --apply
#                                                                  # ...and DELETE the ghost set
#                                                                  # (see plans/active/issues/
#                                                                  # pyth_oracle_prices_stale_ghost_
#                                                                  # failure_rows_2026_07_28.md)
#
# Cost: e2-standard-4 + 50GB (defi defaults to e2-highmem-8/64GB instead --
#   reconcile_phantom_manifest_rows_all.py's full-wide-schema manifest read
#   OOM-killed at 15.4GB RSS on e2-standard-4 and stalled at 96% mem even on a
#   64GB e2-highmem-8 box under the heavier 3-4-script chain -- see
#   /plans/active/issues/reconcile_phantom_manifest_rows_all_defi_memory_footprint_2026_07_28.md;
#   this bumped default is the largest size that DIDN'T hard-OOM-kill in that
#   incident, not a guaranteed-sufficient floor). ~13 min for full DEFI per
# CLAUDE.md (~222 prefixes/sec on same-region GCE). Larger asset_groups (CEFI
# ~313k rows) take 30-60 min. MACHINE_TYPE/VENUES env vars override the default
# box size / venue scope (the initial manifest load is asset_group-wide
# regardless of VENUES -- it only trims the downstream audit-loop scope, not
# the load).
#
# Singleton lock: refuses to launch if another defi-phantom-recon-* VM is
# RUNNING in the zone. The audit script reads the canonical manifest +
# bulk-lists GCS prefixes; concurrent runs duplicate the GCS list-blob
# budget without speedup. --force bypasses for legitimate parallel runs
# across asset_groups (the singleton check matches the prefix not the
# asset_group, so launching defi + cefi back-to-back needs --force on the
# second; in practice run them sequentially).
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - Auto-shutdown when the script exits (VM_SHUTDOWN_ON_COMPLETION=true)
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
APPLY_FLAG="${2:---dry-run}"
# Optional third positional: --apply, paired ONLY with a --report-* single-purpose
# pass above (those passes are dry-run-by-default and need --apply as a SEPARATE
# flag to mutate — see reconcile_phantom_manifest_rows_all.py's own --apply switch).
EXTRA_FLAG="${3:-}"

# Validate asset_group + flag.
case "$ASSET_GROUP" in
    cefi|defi|tradfi|prediction|sports) ;;
    *) echo "ERROR: asset_group must be one of cefi/defi/tradfi/prediction/sports (got: $ASSET_GROUP)" >&2; exit 2 ;;
esac
case "$APPLY_FLAG" in
    --dry-run|--apply) ;;
    # Single-purpose report passes (mirror-image phantom cases, single-walk reuse
    # of the already-loaded index — see the pass's own --help in the script).
    # PYTH oracle_prices ghost-failure pass added 2026-07-28, see
    # plans/active/issues/pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md.
    --report-pyth-oracle-prices-ghost-failures) ;;
    *)
        echo "ERROR: second arg must be --dry-run (default), --apply, or a --report-* single-purpose pass (got: $APPLY_FLAG)" >&2
        exit 2
        ;;
esac
if [[ -n "$EXTRA_FLAG" && "$EXTRA_FLAG" != "--apply" ]]; then
    echo "ERROR: third arg (if given) must be --apply (got: $EXTRA_FLAG)" >&2
    exit 2
fi
if [[ -n "$EXTRA_FLAG" && "$APPLY_FLAG" != --report-* ]]; then
    echo "ERROR: third arg --apply is only valid paired with a --report-* second arg (got: $APPLY_FLAG $EXTRA_FLAG)" >&2
    exit 2
fi

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
VENUES="${VENUES:-}"

if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    # Idempotent audit-and-relabel: the write-back is a single atomic CAS at
    # the very end of the run (see reconcile_phantom_manifest_rows_all.py),
    # so a mid-run preemption loses no partial state -- safe SPOT default per
    # CLAUDE.md "Backfill VMs default to SPOT".
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^defi-phantom-recon-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: phantom-recon VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — concurrent GCS list-blob budget shared,
no speedup. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force ${ASSET_GROUP} ${APPLY_FLAG}

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
VM_NAME="defi-phantom-recon-${ASSET_GROUP}-${RUN_TS}"

echo "Launching $VM_NAME: phantom recon for asset_group=${ASSET_GROUP} (${APPLY_FLAG})${VENUES:+ venues=${VENUES}} machine=${MACHINE_TYPE}"

# Route via VM_TASK=phantom-recon (added to setup-data-pipeline-vm.sh
# 2026-05-07): the setup script pulls the instruments-service tarball
# (because VM_SERVICE=instruments_service) + sets up the venv, then runs
# VM_BACKFILL_CMD verbatim with `python` rewritten to `$VENV/bin/python`.
# setup-data-pipeline-vm.sh extracts instruments-service-code tarball to
# $WORKSPACE/instruments (not /instruments-service) per the TARBALL_DIRS
# alias in that script. $WORKSPACE = /home/ikennaigboaka/workspace.
RECON_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/reconcile_phantom_manifest_rows_all.py"
BACKFILL_CMD="python ${RECON_SCRIPT} --asset-group ${ASSET_GROUP} ${APPLY_FLAG}"
if [[ -n "$EXTRA_FLAG" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} ${EXTRA_FLAG}"
fi
if [[ -n "$VENUES" ]]; then
    BACKFILL_CMD="${BACKFILL_CMD} --venues ${VENUES}"
fi

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=phantom-recon"
METADATA="${METADATA},VM_ASSET_GROUP=$(echo "$ASSET_GROUP" | tr '[:lower:]' '[:upper:]')"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    "${PROVISIONING_ARGS[@]}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=defi-phantom-recon,asset-group="${ASSET_GROUP}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/phantom-recon.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
