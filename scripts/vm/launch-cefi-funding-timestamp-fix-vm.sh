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
#   bash launch-cefi-funding-timestamp-fix-vm.sh --shards N <VENUE> <START_DATE> <END_DATE>
#
# Example (BYBIT, its full manifest-scanned range per the issue doc's scope table):
#   bash launch-cefi-funding-timestamp-fix-vm.sh BYBIT 2020-01-01 2026-05-01
#
# Example (BINANCE-FUTURES split across 4 concurrent VMs -- the ~24h single-VM
# range from plans/active/issues/cefi_migration_vm_launcher_no_sharding_and_spot_preemption_churn_2026_07_28.md
# cut to ~N/4 wall-clock each):
#   bash launch-cefi-funding-timestamp-fix-vm.sh --shards 4 BINANCE-FUTURES 2020-01-01 2026-05-01
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
#   SHARD_COUNT=1                date-range sharding: launch N VMs, one per evenly-split contiguous
#                                sub-range of <START_DATE>..<END_DATE> (default 1 -- unchanged
#                                single-VM behavior). Same as `--shards N`; the flag wins if both are
#                                given. Bounded to MAX_SHARDS below. See
#                                plans/active/issues/cefi_migration_vm_launcher_no_sharding_and_spot_preemption_churn_2026_07_28.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"
source "${SCRIPT_DIR}/_cefi-fts-launcher-lib.sh"

# Hard cap on --shards / SHARD_COUNT: (1) a fat-fingered --shards 50 must not
# launch a runaway 50-VM SPOT storm into a zone that already churns under
# this launcher family's own load (5-of-11 preempted in one ~2h window,
# 2026-07-28 issue doc); (2) the VM_NAME budget below only has room for a
# SINGLE ASCII digit of shard-index suffix within GCE's 63-char instance-name
# limit for the longest known venue slug ("bitfinex-futures", 16 chars) -- see
# the VM_NAME comment. 8 is comfortably above the largest split actually
# needed/proven this session (2-way, done by hand) while staying single-digit.
MAX_SHARDS=8

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-250GB}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
ON_DEMAND="${ON_DEMAND:-false}"
SHARD_COUNT="${SHARD_COUNT:-1}"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)   DRY_RUN=true; shift ;;
    --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
    --project)   PROJECT_ID="$2"; shift 2 ;;
    --zone)      ZONE="$2"; shift 2 ;;
    --on-demand) ON_DEMAND=true; shift ;;
    --shards)    SHARD_COUNT="$2"; shift 2 ;;
    *)           POSITIONAL+=("$1"); shift ;;
  esac
done
set -- "${POSITIONAL[@]:-}"

VENUE="${1:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] [--shards N] <VENUE> <START_DATE> <END_DATE>}"
START_DATE="${2:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] [--shards N] <VENUE> <START_DATE> <END_DATE>}"
END_DATE="${3:?Usage: $0 [--dry-run] [--env prod|staging|dev] [--on-demand] [--shards N] <VENUE> <START_DATE> <END_DATE>}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ ! "$SHARD_COUNT" =~ ^[0-9]+$ ]] || (( SHARD_COUNT < 1 )); then
  echo "ERROR: --shards/SHARD_COUNT must be a positive integer (got: $SHARD_COUNT)" >&2
  exit 1
fi
if (( SHARD_COUNT > MAX_SHARDS )); then
  echo "ERROR: --shards/SHARD_COUNT=${SHARD_COUNT} exceeds MAX_SHARDS=${MAX_SHARDS} -- refusing to launch a" \
    "potential VM storm. Raise MAX_SHARDS deliberately (with justification) if a larger split is genuinely needed." >&2
  exit 1
fi

# Bare positive-integer-free venue slug for the VM name (lowercased, dashes only) -- avoids leaking
# an arbitrary --venue string with unsafe characters into a GCE resource name.
VENUE_SLUG="$(printf '%s' "$VENUE" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/-\+/-/g; s/^-//; s/-$//')"
# Computed ONCE for the whole invocation (not per shard) -- all shards launched
# by one `--shards N` invocation share this RUN_TS and are disambiguated by the
# "s<i>" suffix appended in the sharded branch below, never by relying on
# distinct per-shard timestamps (a bash loop launching N VMs back-to-back can
# easily land in the same wall-clock second, which would collide under
# lc_run_ts's 1-second resolution).
RUN_TS="$(lc_run_ts)"
# Namespaced under the ALREADY-REGISTERED "canonical-migration-cefi-" VM_PREFIX_TO_BUCKET prefix
# (deployment_service/vm_prefix_registry.py) -- longest-prefix-match routes this to the CeFi tick
# bucket + EPHEMERAL_BATCH lifecycle with NO new registry entry needed (same convention documented
# throughout launch-canonical-migration-vm.sh for its category-suffixed VM names). "fts" (funding
# timestamp) keeps the full name under GCE's 63-char instance-name limit even for the longest venue
# slug ("bitfinex-futures", 16 chars): 29 (prefix) + 16 (venue) + 1 + 15 (run_ts) = 61. The N=1
# (default) VM_NAME below is UNCHANGED from before --shards existed -- the sharded branch's VM_NAME
# (built separately, see below) appends a single-ASCII-digit "s<i>" with NO extra separator
# (61 + len("sN") == 63 for the longest venue slug -- exactly GCE's limit, hence MAX_SHARDS=8 keeping
# every shard index single-digit).
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

# _launch_one_vm <vm_name> <start_date> <end_date> [shard_label]
# ---------------------------------------------------------------------------
# Launches exactly one VM for the given (possibly shard-narrowed) date window.
# Reads VENUE/PROJECT_ID/ZONE/MACHINE_TYPE/BOOT_DISK_SIZE/DEPLOYMENT_ENV/
# CODE_BUCKET/PROVISIONING_FLAGS/VENUE_SLUG/DRY_RUN from the enclosing script
# scope (all fixed for the whole invocation, identical across shards).
#
# Called exactly once with shard_label="" for the default N=1 (unsharded)
# path -- the `[[ -n "$shard_label" ]] && echo ...` line below then emits
# nothing, so the N=1 banner/behavior is byte-identical to the pre-sharding
# script. `$DRY_RUN` no longer `exit 0`s from inside this function (so a
# sharded dry-run can print all N shards before exiting) -- the caller exits
# after the whole dispatch instead.
_launch_one_vm() {
  local vm_name="$1" start_date="$2" end_date="$3" shard_label="${4:-}"

  echo "============================================================"
  echo "CeFi Bulk-Tardis derivative_ticker funding_timestamp Fix VM"
  [[ -n "$shard_label" ]] && echo "  Shard:      ${shard_label}"
  echo "  Project:    ${PROJECT_ID}"
  echo "  Zone:       ${ZONE}"
  echo "  Machine:    ${MACHINE_TYPE}"
  echo "  VM:         ${vm_name}"
  echo "  Env:        ${DEPLOYMENT_ENV}"
  echo "  Venue:      ${VENUE}"
  echo "  Window:     ${start_date} .. ${end_date}"
  echo "  Provision:  ${PROVISIONING_FLAGS:-STANDARD (--on-demand)}"
  echo "  Tarball:    gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
  echo "============================================================"

  local vm_migration_cmd="python scripts/one_offs/reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py --mode apply --venue ${VENUE} --start-date ${start_date} --end-date ${end_date} --apply"

  if $DRY_RUN; then
    echo "[DRY RUN] Would launch VM ${vm_name} — skipping gcloud create."
    echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
    echo "  VM_TASK=canonical-migration"
    echo "  VM_MIGRATION_CMD=${vm_migration_cmd}"
    return 0
  fi

  echo "Launching VM..."
  gcloud compute instances delete "${vm_name}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" --quiet 2>/dev/null || true

  local metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  metadata="${metadata},VM_TASK=canonical-migration"
  metadata="${metadata},VM_SERVICE=market_tick_data_service"
  metadata="${metadata},VM_MIGRATION_CMD=${vm_migration_cmd}"
  metadata="${metadata},MANIFEST_PER_VM_SHARDS=true"
  metadata="${metadata},VM_NAME=${vm_name}"
  metadata="${metadata},VM_SHUTDOWN_ON_COMPLETION=true"
  metadata="${metadata},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  # STALL_PROGRESS_REGEX (real incident 2026-07-29: a BINANCE-FUTURES shard's migration script
  # printed its own final "=== SUMMARY (APPLIED) ..." line, then the process never actually
  # exited -- the VM sat RUNNING for 3+ hours with zero real progress before being noticed and
  # manually killed). setup-data-pipeline-vm.sh's VM_TASK=canonical-migration dispatch already
  # routes this launcher through vm-exec-with-gcs-tee.sh's stall-kill unconditionally (confirmed,
  # see migration_vm_hung_detection_monitoring_gap_2026_07_27.md Gap 2 + its CORRECTION note), but
  # with no STALL_PROGRESS_REGEX set it falls back to raw log-BYTE-GROWTH detection -- permanently
  # defeated by the always-on PIPELINE_HEARTBEAT emitter writing a line every 60s into the SAME
  # tee'd log regardless of whether the real workload is alive (the exact mechanism that doc's
  # Gap 2 documents for the cefi-content-apply campaign; this launcher independently hit the
  # identical bug class). Fix: a content-aware regex matching this script's real per-object
  # progress line (reprocess_bulk_tardis_derivative_ticker_funding_timestamp_2026_07_28.py line
  # ~592: "  {venue}: action=<corrected|skipped_...|would_correct> rows=... ..."), which fires
  # many times per second during real work and never again after the final SUMMARY line -- so a
  # post-completion hang (or any other genuine stall) now trips the existing STALL_TIMEOUT_SEC
  # (1800s default) instead of running for hours. No "wedged worker" warning string exists in this
  # script to accidentally match (grep confirms), so no exclusion pattern is needed.
  metadata="${metadata},STALL_PROGRESS_REGEX=action="

  lc_verify_tarball_freshness "$CODE_BUCKET" \
      market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
      || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

  # shellcheck disable=SC2086
  gcloud compute instances create "${vm_name}" \
    --project="${PROJECT_ID}" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "${PROJECT_ID}")" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --scopes=cloud-platform \
    --no-restart-on-failure \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --labels="purpose=cefi-funding-timestamp-fix,env=${DEPLOYMENT_ENV},venue=${VENUE_SLUG}",managed-by=deployment-service \
    --metadata="${metadata}"

  # Best-effort exact-replay record for a SPOT-preemption relaunch (never fails the launch — see
  # lc_write_launch_params's own docstring). Per-shard start/end so a relaunch of THIS shard's VM
  # replays THIS shard's window, not the original unsharded full range.
  lc_write_launch_params "$vm_name" "$PROJECT_ID" "launch-cefi-funding-timestamp-fix-vm.sh" \
    "VENUE=${VENUE}" "START_DATE=${start_date}" "END_DATE=${end_date}" "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}" || true

  echo ""
  echo "VM launched: ${vm_name}"
  echo "  Monitor run.log:    gsutil cat gs://${CODE_BUCKET}/vm-logs/${vm_name}/run.log | tail -100"
  echo "  Monitor checkpoint: gsutil cat gs://${CODE_BUCKET}/vm-logs/${vm_name}/PROGRESS.json"
  echo "  SSH:                gcloud compute ssh ${vm_name} --zone=${ZONE} -- tail -f /var/log/vm-setup.log"
  echo "============================================================"
}

if (( SHARD_COUNT == 1 )); then
  # Strict additive N=1 default path -- same single call, same VM_NAME, same
  # START_DATE/END_DATE the script always used before --shards existed.
  _launch_one_vm "$VM_NAME" "$START_DATE" "$END_DATE" ""
else
  echo "============================================================"
  echo "Sharding ${VENUE} into ${SHARD_COUNT} date-range VMs (${START_DATE} .. ${END_DATE})"
  echo "============================================================"
  shard_i=0
  while IFS=':' read -r shard_start shard_end; do
    shard_i=$(( shard_i + 1 ))
    shard_vm_name="canonical-migration-cefi-fts-${VENUE_SLUG}-${RUN_TS}s${shard_i}"
    _launch_one_vm "$shard_vm_name" "$shard_start" "$shard_end" "${shard_i}/${SHARD_COUNT}"
  done < <(cefi_fts_split_date_shards "$START_DATE" "$END_DATE" "$SHARD_COUNT")
fi

if $DRY_RUN; then
  exit 0
fi
exit 0
