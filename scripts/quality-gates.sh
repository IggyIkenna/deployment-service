#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-pm/codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-trading-library)
SERVICE_NAME="deployment-service"
SOURCE_DIR="deployment_service"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
# deployment-api is a TEST-ONLY peer dep: deployment_service/ source never imports it,
# but tests/ do (tests/mocks.py → deployment_api.utils.path_combinatorics). It is NOT a
# [project.dependencies] entry because deployment-api -> deployment-service is the real
# runtime edge; declaring the reverse would re-create a manifest cycle (fails
# system-integration-tests test_dependency_graph_is_acyclic) and a dep-alignment mismatch.
# Peer-installing it here satisfies the test imports without entering the runtime graph.
LOCAL_DEPS=(deployment-api)
# Deployment orchestration has inherently large functions:
# - Cloud backends deploy_shard(): 90-324L (API calls, retries, error handling)
# - Worker managers launch_shards_*(): 232-502L (parallel shard dispatch + monitoring)
# - CLI display functions: 200-293L (rich terminal output construction)
# Standard services use 50L; deployment-service needs higher due to orchestration complexity.
MAX_FUNCTION_LINES=510
MAX_METHOD_LINES=510

# Per-repo QG exclusions (see base-service.sh for variable documentation)
# print(): Rich console.print in progress.py, bash heredoc print in vm_config.py
PRINT_EXCLUDE_GLOBS=(--glob "!**/deployment/progress.py" --glob "!**/backends/services/vm_config.py")
# os.environ: bootstrap_config.py (documented exception), worker_manager.py + shard_builder.py (comments only)
OS_ENV_EXCLUDE_GLOBS=(--glob "!**/config/bootstrap_config.py" --glob "!**/deployment/worker_manager.py" --glob "!**/config/env_substitutor.py" --glob "!**/shard_builder.py" --glob "!**/deployment_config.py")
# Imports inside functions: deferred for CLI entry, route handlers, cloud SDK lazy loading, AWS boundary
IMPORT_INSIDE_EXCLUDE_GLOBS=("!**/__main__.py" "!**/api/routes/state.py" "!**/backends/aws_ec2.py" "!**/backends/aws.py" "!**/backends/aws_batch.py" "!**/backends/_gcp_sdk.py" "!**/backends/base.py" "!**/calculators/shard_dimensions.py" "!**/backends/provider_factory.py" "!**/cli/handlers/cluster_handler.py")
# Broad except Exception — best-effort multi-cloud (GCS+S3) CLI reads where any failure
# (missing bucket, network, auth, corrupt parquet) is treated identically as "absent, skip"
# rather than crashing the CLI display path. Justified in QUALITY_GATE_BYPASS_AUDIT.md §2.18
# (2026-07-13). §§2.19-2.20: same never-raises pattern across the fleet-monitoring package.
# §2.21 (2026-07-15): check_vm_cli.py's bucket-resolution helper, same pattern.
BE_EXCLUDE_GLOBS=(
    "**/cli/utils/manifest_reader.py"
    "**/data_pipeline_monitors/_gcs.py"
    "**/data_pipeline_monitors/exit_code_fleet_monitor.py"
    "**/data_pipeline_monitors/live_stream_watcher.py"
    "**/data_pipeline_monitors/cli.py"
    "**/data_pipeline_monitors/meta_targets.py"
    "**/data_pipeline_monitors/heartbeat_stall_watcher.py"
    "**/data_pipeline_monitors/meta_watchers.py"
    "**/data_pipeline_monitors/deadman_poster.py"
    "**/data_pipeline_monitors/check_vm_cli.py"
)
# Empty string/dict/list: safe defaults for optional config dict lookups
EMPTY_STR_EXCLUDE_GLOBS=(--glob "!**/config_loader.py" --glob "!**/shard_calculator.py" --glob "!**/dependencies.py")
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(--glob "!**/config_loader.py" --glob "!**/shard_calculator.py" --glob "!**/dependencies.py" --glob "!**/orchestrator.py")
# GCP_PROJECT_ID: docstrings, template substitution strings, error messages, bash heredocs
GCP_PROJECT_ID_EXCLUDE_GLOBS=("!**/cli/commands/calculation.py" "!**/smoke_test_framework.py" "!**/deployment_config.py" "!**/config_loader.py" "!**/cloud_client.py" "!**/backends/services/vm_config.py" "!**/dependencies.py" "!**/shard_builder.py" "!**/backends/services/vm_lifecycle.py")
# escalation.py calls setup_events() as a best-effort module-level initializer (not
# an entry-point main) — its callers own the run_lifecycle context; exclude from STEP 5.63.
LIFECYCLE_EXCLUDE_GLOBS=("!**/data_pipeline_monitors/escalation.py")
# vm_prefix_registry.py is a 205-entry VM-prefix → VmPrefixSpec DATA REGISTRY (moved out of
# scripts/vm/vm_zombie_watchdog.py on 2026-07-13 so it ships in the wheel). It exceeds the
# file-line ceiling by data volume, not SRP — one logical registry dict, not decomposable.
# Exempt from the file-size check (same precedent as unified-trading-api mock_data/seed_strategies.py).
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./deployment_service/vm_prefix_registry.py"
)
# Ratcheted 8→1 on 2026-06-11 (codex_violations_ratchet_to_five_2026_06_10 plan): census run
# showed a single firing class (broad-except, documented in QUALITY_GATE_BYPASS_AUDIT.md).
# Budgets only ratchet DOWN — a bump is review-blocking.
CODEX_MAX_VIOLATIONS=1
# 2026-05-26 baseline: 1297 errors with pre-rollout basedpyright strictness (reportUnknown* partially enabled).
# Ratchet this down as type errors are fixed. Enforced by base-service.sh BASEDPYRIGHT_MAX_ERRORS ratchet.
# 2026-07-13: 1297 -> 1293. Fixed 5 real errors — removed 1 redundant cast (meta_watchers.py, pd.read_parquet
# already returns DataFrame) + annotated 4 gunicorn hook params (pre_fork/post_fork server+worker: object).
# CI's actual count is 1292; the value keeps 1 slot of headroom because a workspace checkout lacking the
# ../unified-cloud-interface + ../unified-config-interface siblings (basedpyright extraPaths) resolves one
# cross-repo type to Unknown, yielding 1293 locally. Do NOT drop below 1293 without adding those siblings first.
BASEDPYRIGHT_MAX_ERRORS=1293
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(git rev-parse --show-toplevel)/.." && pwd)}"
BASE_QG_SCRIPT="${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"
if [ ! -f "${BASE_QG_SCRIPT}" ]; then
    # In-image (CI test-in-image) runs have no PM repo / no git → the base script is absent.
    # Mirror the fleet-canonical mtds guard: skip the in-image gate pass gracefully.
    if [ "${CLOUD_BUILD:-false}" = "true" ]; then
        echo "quality-gates base script unavailable in image; skipping in-image gate pass"
        exit 0
    fi
    echo "Missing base quality-gates script: ${BASE_QG_SCRIPT}" >&2
    exit 1
fi
source "${BASE_QG_SCRIPT}"

# Codex enforcement: lifecycle triple (STARTED / STOPPED / FAILED) via UTL — not duplicated in service code.
# See: unified-trading-pm/codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
if rg -q 'fastapi_uei_lifespan\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: fastapi_uei_lifespan (canonical HTTP wiring in UTL)"
elif rg -q 'ServiceBootstrap\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: ServiceBootstrap (canonical CLI wiring in UTL)"
else
    for event in STARTED STOPPED FAILED; do
        # -U: allow multiline call sites (e.g. log_event(\n  "STARTED", ...))
        run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -U -q \
            || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
    done
fi

# Dual-cloud build parity: every repo with a real Docker image build (cloudbuild.yaml)
# must also have buildspec.aws.yaml. SSOT: codex/05-infrastructure/dual-cloud-image-builds.md
log_section "[DUAL-CLOUD] Dual-cloud image build parity check"
WORKSPACE_ROOT="${WORKSPACE_ROOT}" run_timeout 30 \
    python3 "${WORKSPACE_ROOT}/deployment-service/scripts/quality_gates/check_dual_cloud_parity.py" \
    && log_success "Dual-cloud parity: all image-building repos have buildspec.aws.yaml" \
    || log_fail "Dual-cloud parity FAILED — see above for missing repos"

# Tardis-consuming VM launchers must provision a disk that can absorb the download.
# A pd-standard 50GB boot disk throttled the CeFi backfill to ~2.4 MB/s after ~7.5GB
# (measured 2026-07-18: %util 99.94, w_await 1015ms, CPU idle, RAM free) and was
# misdiagnosed for hours as a Tardis quota. SSOT: plans/active/issues/
# tardis_account_volume_quota_7gb_throughput_cliff_2026_07_18.md
log_section "[BACKFILL-DISK] Backfill VM boot-disk provisioning check"
run_timeout 30 \
    python3 "${WORKSPACE_ROOT}/deployment-service/scripts/quality_gates/check_backfill_vm_disk_provisioning.py" \
    && log_success "Backfill VM disks: all download-heavy launchers on adequate disks" \
    || log_fail "Backfill VM disk provisioning FAILED — see above"
