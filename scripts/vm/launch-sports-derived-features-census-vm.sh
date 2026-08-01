#!/usr/bin/env bash
# Epic: sports_master
# Lifecycle: campaign
# Delete-when: after sports_derived_features_postfloor_residue_purge_2026_07_27.md
#   archives and a fresh re-census confirms 0 residue objects remain.
#
# Launch a short-lived SPOT GCE VM that runs the sports derived_features
# post-floor residue CENSUS (features-service/scripts/purge_sports_derived_
# features_post_floor_residue_2026_07_27.py, no --apply) as the ONE sanctioned
# single-walk for this audit. It enumerates
# gs://features-sports-prd-central-element-323112/sports_features/by_date/
# day={D}/.../feature_group=derived_features/ for D in Jun-Dec 2020 + 2021-2026
# (per-day server-side-scoped prefix listing, not a whole-corpus walk), flags
# every object whose GCS last_modified (proxy for time_created -- write-once
# artifact; see the script's own docstring) is BEFORE 2026-07-19, and writes
# the full flagged-object list to a STABLE GCS path
# (gs://features-sports-prd-central-element-323112/_audits/
# derived_features_postfloor_residue_census_2026_07_27.json) -- the artifact
# the plan's Todo 2 (a separate, reversibility-verified delete step) reads.
#
# This launcher NEVER passes --apply -- it is READ-ONLY by construction (lists
# + describes objects, writes only the audit manifest above). Re-running it is
# a no-op refresh (idempotent), never a destructive action.
#
# SSOT: unified-trading-pm/plans/active/sports_derived_features_postfloor_residue_purge_2026_07_27.md
#       unified-trading-pm/codex/02-data/reconciliation-census-and-compute-tiers.md § 3
#
# Run same-region (asia-northeast1-c) so the corpus GCS reads are fast
# (cross-region listing is ~18× slower).
#
# SPOT by default (backfill HARD RULE, spot-vms-for-backfill.md): the census is
# read-only + idempotent (re-running just re-scans and overwrites the same
# fixed report path), so a preemption relaunch is a safe restart-from-scratch.
# ON_DEMAND=true is the only opt-out.
#
# The VM prefix MUST match the registered sports-derived-features-census- entry
# in deployment_service/vm_prefix_registry.py (bucket=None, heartbeat-only --
# this tool writes a fixed report path, not a per-VM shard) + its
# launcher_registry.py twin.
#
# Singleton-locked (one census run at a time).
#
# Output:
#   - Live combined stdout: gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#   - Census manifest: gs://features-sports-prd-{pid}/_audits/derived_features_postfloor_residue_census_2026_07_27.json
#
# Usage:
#   bash launch-sports-derived-features-census-vm.sh
#   bash launch-sports-derived-features-census-vm.sh --force        # bypass singleton lock
#   bash launch-sports-derived-features-census-vm.sh --env staging
#   WORKERS=64 bash launch-sports-derived-features-census-vm.sh     # more concurrent per-day listers
#   ON_DEMAND=true bash launch-sports-derived-features-census-vm.sh # opt out of SPOT
#
# Cost: e2-standard-4 SPOT + 100GB. Read-only single walk, expected minutes not hours
# (per-day prefix listing across ~2400 in-scope days, not a full-corpus object walk).
#
# qg-disk-exempt: this VM_TASK contains "features" (a check_backfill_vm_disk_
# provisioning.py TASK_MARKERS substring match), but it is NOT a download-heavy
# backfill — it lists object METADATA only (no parquet downloads) and writes ONE
# small JSON manifest (a few KB at most, not the sustained-parquet-write workload
# the 250GB/pd-balanced minimum was sized for). 100GB pd-balanced is ample.
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

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
# Matches the script's own hardcoded `_BUCKET` constant (features-service/scripts/
# purge_sports_derived_features_post_floor_residue_2026_07_27.py) — informational
# only, this launcher never touches GCS objects directly.
SPORTS_BUCKET="features-sports-prd-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-100}"
WORKERS="${WORKERS:-32}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: spot-vms-for-backfill) — the
# census is read-only + idempotent (re-scanning just overwrites the same fixed
# report path), so a preemption relaunch is a safe restart-from-scratch.
# ON_DEMAND=true is the only opt-out.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

SINGLETON_PREFIX="sports-derived-features-census-"
if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter="name~\"^${SINGLETON_PREFIX}\" AND status=RUNNING" \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: sports-derived-features-census VM already running: $EXISTING
Refusing to launch a duplicate. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect
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

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
VM_NAME="${SINGLETON_PREFIX}${RUN_TS}"

# features-service-code.tar.gz is extracted to $WORKSPACE/features/ by
# setup-data-pipeline-vm.sh (VM_SERVICE=features_service), so the script lives at:
SCRIPTS="/home/ikennaigboaka/workspace/features/scripts"

# Single `python ` token → setup-data-pipeline-vm.sh substitutes it to $VENV/bin/python.
# NO --apply — this launcher is CENSUS-ONLY, never a delete path. A single scan is
# all Todo 1 needs (no prior delete exists yet, so a chained --recensus would just
# re-run the identical scan for no new information).
BACKFILL_CMD="python ${SCRIPTS}/purge_sports_derived_features_post_floor_residue_2026_07_27.py"
BACKFILL_CMD="${BACKFILL_CMD} --workers ${WORKERS}"

METADATA="VM_TASK=sports-derived-features-census"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=census"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

# Persist the exact launch scope BEFORE create so a SPOT-preemption relaunch
# (RelaunchPreemptedVm) replays the same scope — read-only + idempotent, so a
# restart-from-scratch is safe (best-effort; never fails the launch).
lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-sports-derived-features-census-vm.sh" \
    WORKERS="$WORKERS" DEPLOYMENT_ENV="$DEPLOYMENT_ENV"

echo "Launching $VM_NAME: sports derived_features post-floor residue CENSUS (read-only)"
echo "  Script:    purge_sports_derived_features_post_floor_residue_2026_07_27.py (no --apply)"
echo "  Manifest:  gs://${SPORTS_BUCKET}/_audits/derived_features_postfloor_residue_census_2026_07_27.json"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service unified-api-contracts unified-trading-library \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    "${PROVISIONING_ARGS[@]}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=sports-derived-features-census,asset-group=sports,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Live log:    gsutil cat -r 0- gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Manifest:    gcloud storage cat gs://${SPORTS_BUCKET}/_audits/derived_features_postfloor_residue_census_2026_07_27.json"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "NO FIRE-AND-FORGET (async-wait HARD RULE): the CALLER must, in the SAME turn,"
echo "arm a run_in_background heartbeat watchdog (≤30-min, kill -0 liveness, no self-match) keyed on"
echo "the run.log tail + the census manifest's generated_at/total_delete fields:"
echo "  - STARTED < 60s; require ≥1 progress/hr; flat ⇒ STALL ⇒ diagnose; verify T+10min."
