#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launches the pipeline_e2e_check.py DRIVER on its own dedicated VM instead of
# inline on the shared AO orchestrator host.
#
# Why this exists: pipeline_e2e_check.py already delegates the actual heavy
# data work to its own SPOT VMs (launch-features-vm.sh etc, via
# unified_trading_library.pipeline_e2e_check.launcher.launch_vm_and_wait) — that
# part of the pattern was already mature. What was NOT on a VM was the driver
# process itself (the orchestration loop that launches + polls + verifies those
# VMs): live evidence 2026-08-06 showed the driver's own RSS reaching 15.7GB
# (features, --legs benchmark) and 21.9GB (MTDS, --legs force,skip), getting
# OOM-killed by the AO host's resource-watchdog after competing with every
# other slot for the host's fixed memory pool. This launcher gives the driver
# its own VM with real headroom instead.
#
# Reuses the EXISTING, service-agnostic VM_TASK=pipeline-e2e-check dispatch
# branch in setup-data-pipeline-vm.sh (mirrors the canonical-migration branch's
# VM_MIGRATION_CMD + SERVICE_TARBALLS/TARBALL_DIRS pattern) — this launcher only
# resolves flags + calls lc_gcloud_create, same division of labor every other
# launcher here follows.
#
# The driver VM's OWN calls to launch-{features,mtds,instruments,mdps}-vm.sh
# (deployment-service/scripts/vm/) work unmodified — deployment-service is
# ALWAYS installed on every VM this dispatcher launches (NEEDED_TARBALLS), so
# nothing extra is required for the driver-launches-VMs chain to resolve.
#
# Usage:
#   bash launch-pipeline-e2e-check-driver-vm.sh --service features \
#     --day 2026-08-03 --asset-group DEFI --family onchain \
#     --legs benchmark --benchmark-days 7 --project central-element-323112
#
#   bash launch-pipeline-e2e-check-driver-vm.sh --service mtds \
#     --day 2026-08-05 --asset-group DEFI --legs force,skip --mvp-only \
#     --require-captured --auto-day --project central-element-323112
#
# The GCS report upload (unified_trading_library.pipeline_e2e_check.report)
# lands at gs://deployment-scripts-{project}/pipeline-e2e-check-reports/
#   {service}/{run_date}/{stem}.md + .json — pollable the same way run.log/
# EXIT_STATUS already are (this VM's own observability is the SAME
# lc_log_upload_trap_block contract every other VM here emits).
#
# Poll to completion the same way any other launch_vm_and_wait() caller does —
# from Python:
#   from unified_trading_library.pipeline_e2e_check.launcher import launch_vm_and_wait
#   launch_vm_and_wait(
#       launcher_script="deployment-service/scripts/vm/launch-pipeline-e2e-check-driver-vm.sh",
#       argv=[...],  # the flags below
#       vm_name=vm_name,  # must match what THIS script computes -- see _vm_name()
#       project_id=project_id,
#       code_bucket=f"deployment-scripts-{project_id}",
#       timeout_sec=...,
#   )
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ZONE="asia-northeast1-c"
# 32GB headroom -- comfortably above the observed 15.7GB (features) / 21.9GB
# (MTDS) driver peaks, without over-provisioning to a full 8-vCPU box for what
# is an I/O/poll-bound orchestration loop, not a compute-bound workload.
MACHINE_TYPE="e2-highmem-4"
DISK_GB="30"

declare -A _SERVICE_ALIAS_TO_VM_SERVICE=(
  ["features"]="features_service"
  ["mtds"]="market_tick_data_service"
  ["is"]="instruments_service"
  ["mdps"]="market_data_processing_service"
)
# Actual repo DIRECTORY names -- lc_verify_tarball_freshness resolves
# $WORKSPACE_ROOT/$repo/.git, so this must be the real clone dirname, not the
# --service alias or the underscored VM_SERVICE value.
declare -A _SERVICE_ALIAS_TO_REPO_DIR=(
  ["features"]="features-service"
  ["mtds"]="market-tick-data-service"
  ["is"]="instruments-service"
  ["mdps"]="market-data-processing-service"
)

SERVICE_ALIAS=""
PROJECT="central-element-323112"
VM_NAME_OVERRIDE=""
PASSTHROUGH_ARGS=()

while [[ $# -gt 0 ]]; do
  case "${1:-}" in
    --service) SERVICE_ALIAS="$2"; shift 2 ;;
    --project) PROJECT="$2"; PASSTHROUGH_ARGS+=("--project" "$2"); shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --dry-run) export LC_DRY_RUN=true; shift ;;
    --vm-name) VM_NAME_OVERRIDE="$2"; shift 2 ;;
    *) PASSTHROUGH_ARGS+=("$1"); shift ;;
  esac
done

if [[ -z "$SERVICE_ALIAS" ]]; then
  echo "ERROR: --service <features|mtds|is|mdps> is required" >&2
  exit 2
fi
VM_SERVICE="${_SERVICE_ALIAS_TO_VM_SERVICE[$SERVICE_ALIAS]:-}"
if [[ -z "$VM_SERVICE" ]]; then
  echo "ERROR: unknown --service '${SERVICE_ALIAS}' (expected features|mtds|is|mdps)" >&2
  exit 2
fi

lc_validate_env "$DEPLOYMENT_ENV"
CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
RUN_TS="$(lc_run_ts)"
# Deterministic, collision-resistant name -- same lesson as the features driver's
# own _vm_name() (issues/features_pipeline_e2e_check_vm_name_collision_same_second_2026_08_01.md):
# hash the full passthrough argv (which includes --day) so two concurrent
# different-day runs of the same service never collide on a same-second name.
_ARGV_HASH="$(printf '%s' "${PASSTHROUGH_ARGS[*]}" | shasum -a 256 | cut -c1-6)"
VM_NAME="${VM_NAME_OVERRIDE:-pipeline-e2e-check-${SERVICE_ALIAS}-${RUN_TS}-${_ARGV_HASH}}"

# pipeline_e2e_check.py's own CLI convention: `python3 scripts/pipeline_e2e_check.py <flags>`,
# run from the service repo's root -- VM_MIGRATION_CMD carries the full command,
# setup-data-pipeline-vm.sh's pipeline-e2e-check branch cd's into the right dir
# (VM_SERVICE -> SERVICE_TARBALLS -> TARBALL_DIRS, never hand-rolled here).
MIGRATION_CMD="python scripts/pipeline_e2e_check.py"
for a in "${PASSTHROUGH_ARGS[@]}"; do
  MIGRATION_CMD="${MIGRATION_CMD} $(printf '%q' "$a")"
done

if [[ "${LC_DRY_RUN:-false}" != "true" ]]; then
  lc_verify_tarball_freshness "$CODE_BUCKET" \
    "${_SERVICE_ALIAS_TO_REPO_DIR[$SERVICE_ALIAS]}" deployment-service unified-api-contracts unified-trading-library \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

METADATA="VM_TASK=pipeline-e2e-check"
METADATA="${METADATA},VM_SERVICE=${VM_SERVICE}"
METADATA="${METADATA},VM_ASSET_GROUP=E2E-CHECK-DRIVER"
METADATA="${METADATA},VM_MIGRATION_CMD=${MIGRATION_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# Ephemeral: self-delete on completion, same as the smoke VMs this mirrors --
# a driver run has no reason to leave a stopped VM (+ its boot disk) billing.
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching ${VM_NAME} (service=${VM_SERVICE}, cmd=${MIGRATION_CMD})"

lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "$MACHINE_TYPE" "$DISK_GB" \
  "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  "purpose=pipeline-e2e-check-driver,service=${SERVICE_ALIAS},env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
  "$(lc_tier_service_account "$DEPLOYMENT_ENV" "$PROJECT" "true")"

echo "  vm_name=${VM_NAME}"
echo "  run.log:      gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "  EXIT_STATUS:  gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/EXIT_STATUS"
echo "  → SSH: gcloud compute ssh $VM_NAME --zone=$ZONE --project=$PROJECT"
echo "  → Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --project=$PROJECT --quiet"
