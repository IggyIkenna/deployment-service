#!/usr/bin/env bash
# Epic: perp_funding_data_semantics_and_cadence_2026_06_16
# Lifecycle: oneoff
# Delete-when: after the full historical derivative_ticker funding_timestamp reprocess has run
#   --apply to completion (verified, manifest-row-count-unchanged) across every venue in
#   market-tick-data-service/scripts/one_offs/reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py's
#   KNOWN_TARDIS_DERIVATIVE_TICKER_VENUES.
#
# One-off CeFi migration VM (Pattern A, same shape as launch-cefi-migration-vm.sh) that scales the
# already-sample-proven ``reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py``
# script (design + a real 86/86-object production sample verified 2026-07-28, see
# plans/active/issues/perp_funding_data_semantics_and_cadence_2026_06_16.md) to a REAL venue's FULL
# affected date range. One VM per venue (independent GCS prefixes -- safe to launch concurrently);
# --mode apply --apply is baked in (this launcher's whole purpose is the real correction run, not a
# dry-run scan -- use the script directly with --mode scan for quantification).
#
# GCS-only workload: the script reads/rewrites ALREADY-CAPTURED historical
# raw_tick_data/by_date/.../data_type=derivative_ticker parquet via UTL's get_storage_client() /
# gcs_copy_object / gcs_describe_object / gcs_conditional_put -- it makes ZERO live calls to Tardis's
# API (no tardis-py / websocket / REST client import anywhere in the script), so the
# tardis-concurrency-guard cap-1 rule does NOT apply to this launcher.
#
# Usage:
#   bash launch-cefi-funding-timestamp-fix-vm.sh <VENUE> <START_DATE> <END_DATE>
#   bash launch-cefi-funding-timestamp-fix-vm.sh --dry-run <VENUE> <START_DATE> <END_DATE>
#   bash launch-cefi-funding-timestamp-fix-vm.sh --env staging <VENUE> <START_DATE> <END_DATE>
#
# Example (BYBIT, its full manifest-scanned range per the issue doc's scope table):
#   bash launch-cefi-funding-timestamp-fix-vm.sh BYBIT 2020-01-01 2026-05-01
#
# Env overrides:
#   MACHINE_TYPE=e2-standard-4   VM size (default: e2-standard-4 -- this workload is GCS-I/O-bound,
#                                single-threaded per the script's own design, not CPU-bound)
#   BOOT_DISK_SIZE=250GB         boot disk (default 250GB — GCP PD throughput scales with size,
#                                ~0.28 MB/s per GB; 250GB is the workspace's backfill-VM minimum,
#                                see plans/active/issues/backfill_vm_disk_starvation_misdiagnosed_as_tardis_quota_2026_07_18.md)
#   ON_DEMAND=true               opt out of the SPOT default (backfill VMs default to SPOT per the
#                                spot-vms-for-backfill.md HARD RULE; this job's own idempotency guard
#                                -- skipped_next_funding_timestamp_already_present -- makes a verbatim
#                                START_DATE replay on preemption safe, i.e. a "skip-enabled backfill")
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-250GB}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
ON_DEMAND="${ON_DEMAND:-false}"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
    --project)   PROJECT_ID="$2"; shift 2 ;;
    --zone)      ZONE="$2"; shift 2 ;;
    --on-demand) ON_DEMAND=true; shift ;;
    *)           POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]:-}"

VENUE="${1:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] <VENUE> <START_DATE> <END_DATE>}"
START_DATE="${2:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] <VENUE> <START_DATE> <END_DATE>}"
END_DATE="${3:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] <VENUE> <START_DATE> <END_DATE>}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# Bare positive-integer-free venue slug for the VM name (lowercased, dashes only) -- avoids leaking
# an arbitrary --venue string with unsafe characters into a GCE resource name.
VENUE_SLUG="$(printf '%s' "$VENUE" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/-\+/-/g; s/^-//; s/-$//')"
RUN_TS="$(lc_run_ts)"
# Namespaced under the ALREADY-REGISTERED "canonical-migration-cefi-" VM_PREFIX_TO_BUCKET prefix
# (deployment_service/vm_prefix_registry.py) -- longest-prefix-match routes this to the CeFi tick
# bucket + EPHEMERAL_BATCH lifecycle with NO new registry entry needed (same convention documented
# throughout launch-canonical-migration-vm.sh for its category-suffixed VM names). "fts" (funding
# timestamp) keeps the full name under GCE's 63-char instance-name limit even for the longest venue
# slug ("bitfinex-futures", 16 chars): 29 (prefix) + 16 (venue) + 1 + 15 (run_ts) = 61.
VM_NAME="canonical-migration-cefi-fts-${VENUE_SLUG}-${RUN_TS}"

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: codex/05-infrastructure/spot-vms-for-backfill.md).
# --instance-termination-action=DELETE (not STOP) so a preempted VM doesn't orphan its boot disk;
# --no-restart-on-failure is passed separately below (own flag), so it is OMITTED from this string
# per the SSOT's own note ("gcloud errors on a duplicate flag").
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if [[ "$ON_DEMAND" == "true" ]]; then
  PROVISIONING_FLAGS="--provisioning-model=STANDARD"
fi

echo "============================================================"
echo "CeFi Bulk-Tardis derivative_ticker funding_timestamp Fix VM"
echo "  Project:    ${PROJECT_ID}"
echo "  Zone:       ${ZONE}"
echo "  Machine:    ${MACHINE_TYPE}"
echo "  VM:         ${VM_NAME}"
echo "  Env:        ${DEPLOYMENT_ENV}"
echo "  Venue:      ${VENUE}"
echo "  Window:     ${START_DATE} .. ${END_DATE}"
echo "  Provision:  ${PROVISIONING_FLAGS:-STANDARD (--on-demand)}"
echo "  Tarball:    gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

VM_MIGRATION_CMD="python scripts/one_offs/reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py --mode apply --venue ${VENUE} --start-date ${START_DATE} --end-date ${END_DATE} --apply"

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=canonical-migration"
  echo "  VM_MIGRATION_CMD=${VM_MIGRATION_CMD}"
  exit 0
fi

echo "Launching VM..."
gcloud compute instances delete "${VM_NAME}" \
  --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=canonical-migration"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${VM_MIGRATION_CMD}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

lc_verify_tarball_freshness "$CODE_BUCKET" \
    market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

# shellcheck disable=SC2086
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${BOOT_DISK_SIZE}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=cefi-funding-timestamp-fix,env=${DEPLOYMENT_ENV},venue=${VENUE_SLUG}" \
  --metadata="${METADATA}"

# Best-effort exact-replay record for a SPOT-preemption relaunch (never fails the launch — see
# lc_write_launch_params's own docstring).
lc_write_launch_params "$VM_NAME" "$PROJECT_ID" "launch-cefi-funding-timestamp-fix-vm.sh" \
  "VENUE=${VENUE}" "START_DATE=${START_DATE}" "END_DATE=${END_DATE}" "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}" || true

echo ""
echo "VM launched: ${VM_NAME}"
echo "  Monitor run.log:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log | tail -100"
echo "  Monitor checkpoint: gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/PROGRESS.json"
echo "  SSH:                gcloud compute ssh ${VM_NAME} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
echo "============================================================"
