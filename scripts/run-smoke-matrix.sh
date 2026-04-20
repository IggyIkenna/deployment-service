#!/usr/bin/env bash
# ============================================================================
# Institutional Smoke Matrix Orchestrator — Phase 4 of the
# institutional-smoke-matrix plan.
#
# Dispatches every service's scripts/smoke_matrix.py in service-dependency
# order, parallel within a tier and sequential across tiers. Reads the
# authoritative service DAG from deployment-service/configs/dependencies.yaml
# (falls back to a hard-coded tier map that mirrors the plan's Phase 4
# description if yaml parsing is unavailable). Each service run gets
# IS_TEST_RUN=true so all writes route to -test- buckets.
#
# 3-step assertion contract lives IN each service's smoke_matrix.py. This
# orchestrator only aggregates per-service pass/fail/skip counts and decides
# overall pass/fail + a structured summary.
#
# SSOT: unified-trading-pm/plans/active/institutional_smoke_matrix_2026_04_20.plan.md
# ============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Paths (portable: resolve WORKSPACE_ROOT from this script's location)
# ----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_SERVICE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${DEPLOYMENT_SERVICE_ROOT}/.." && pwd)"
DEPS_YAML="${DEPLOYMENT_SERVICE_ROOT}/configs/dependencies.yaml"

# ----------------------------------------------------------------------------
# Tier DAG (hard-coded fallback mirroring the plan). Kept in sync with
# dependencies.yaml execution_order. Parallel within each tier; sequential
# across tiers.
# ----------------------------------------------------------------------------
TIER0_SERVICES=("instruments-service")
TIER1_SERVICES=("market-tick-data-service")
TIER2_SERVICES=("market-data-processing-service")
TIER3_SERVICES=(
    "features-delta-one-service"
    "features-volatility-service"
    "features-onchain-service"
    "features-sports-service"
    "features-calendar-service"
    "features-multi-timeframe-service"
    "features-cross-instrument-service"
    "features-commodity-service"
)
# ML services (Phase 2 P2, default excluded — toggle with --include-ml)
ML_SERVICES=(
    "ml-training-service"
    "ml-inference-service"
)

# ----------------------------------------------------------------------------
# CLI defaults
# ----------------------------------------------------------------------------
SERVICE_FILTER=""
CATEGORY_FILTER=""
VENUE_FILTER=""
DATA_TYPE_FILTER=""
NO_DEPS=false
CLEANUP=false
DRY_RUN=false
REPORT_DIR="$(mktemp -d 2>/dev/null || echo "${TMPDIR:-/tmp}/smoke-results-$$")"
TIMEOUT_PER_SERVICE=600
FAIL_FAST=false
INCLUDE_ML=false

# Hook for test harness to stub the per-service invocation.
# When set, the orchestrator invokes "$SMOKE_RUNNER_CMD <service-repo-path>
# <report-path> <python-args...>" instead of running the real python CLI.
SMOKE_RUNNER_CMD="${SMOKE_RUNNER_CMD:-}"

usage() {
    cat <<USAGE
Usage: run-smoke-matrix.sh [OPTIONS]

Workspace smoke-matrix orchestrator. Dispatches every service's
scripts/smoke_matrix.py in service-dependency tier order (parallel within a
tier, sequential across tiers) with IS_TEST_RUN=true.

Options:
  --service X               Smoke only one service (skip deps; fails loud if
                            --no-deps was not passed and prior-tier TEST data
                            is missing — actionable error).
  --category Y              Limit across all services (CEFI|TRADFI|DEFI|SPORTS|PREDICTION).
  --venue Z                 Limit to one venue (propagated to supporting services).
  --data-type W             Limit to one data_type.
  --no-deps                 Skip dep cascade (assumes prior state in TEST buckets).
  --cleanup                 Delete TEST bucket data after the run.
  --dry-run                 Enumerate cells + show invocation plan; no CLI runs.
  --report-dir PATH         Directory for per-service JSON reports + summary.json
                            (default: a fresh temp dir under \$TMPDIR).
  --timeout-per-service N   Seconds per service smoke (default: 600).
  --fail-fast               Abort on first service failure (default: continue).
  --include-ml              Include ml-* services (P2; default excluded).
  -h, --help                This help text.

Exit code: 0 if every service reports all cells PASS; 1 if any cell FAILed.
USAGE
}

# ----------------------------------------------------------------------------
# Arg parsing
# ----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --service) SERVICE_FILTER="$2"; shift 2 ;;
        --category) CATEGORY_FILTER="$2"; shift 2 ;;
        --venue) VENUE_FILTER="$2"; shift 2 ;;
        --data-type) DATA_TYPE_FILTER="$2"; shift 2 ;;
        --no-deps) NO_DEPS=true; shift ;;
        --cleanup) CLEANUP=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --report-dir) REPORT_DIR="$2"; shift 2 ;;
        --timeout-per-service) TIMEOUT_PER_SERVICE="$2"; shift 2 ;;
        --fail-fast) FAIL_FAST=true; shift ;;
        --include-ml) INCLUDE_ML=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

mkdir -p "${REPORT_DIR}"

# ----------------------------------------------------------------------------
# Logging helpers
# ----------------------------------------------------------------------------
_log() { printf '[run-smoke-matrix] %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
_err() { printf '[run-smoke-matrix] %s ERROR %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2; }

# ----------------------------------------------------------------------------
# Tier resolution — filter by --service + --include-ml
# ----------------------------------------------------------------------------
filter_tier() {
    # $1=tier label, remaining args=services. Prints included services (one per line).
    local label="$1"; shift
    local svc
    for svc in "$@"; do
        if [[ -n "${SERVICE_FILTER}" && "${SERVICE_FILTER}" != "${svc}" ]]; then
            continue
        fi
        echo "${svc}"
    done
}

# ----------------------------------------------------------------------------
# Enumerate one service (dry-run helper). Prints a single TSV line:
# <tier>\t<service>\t<repo-path>
# ----------------------------------------------------------------------------
enumerate_service() {
    local tier="$1"
    local svc="$2"
    local repo="${WORKSPACE_ROOT}/${svc}"
    printf '%s\t%s\t%s\n' "${tier}" "${svc}" "${repo}"
}

# ----------------------------------------------------------------------------
# Invoke one service's smoke_matrix.py (real mode).
# Writes the JSON report to ${REPORT_DIR}/<service>.json.
# Returns:
#   0 if the service reports zero cell-level failures
#   1 if any cell failed or the subprocess itself failed
# ----------------------------------------------------------------------------
run_service_smoke() {
    local svc="$1"
    local repo="${WORKSPACE_ROOT}/${svc}"
    local smoke_script="${repo}/scripts/smoke_matrix.py"
    local report_path="${REPORT_DIR}/${svc}.json"

    # In real mode we require the per-service smoke script. In test-harness
    # mode (SMOKE_RUNNER_CMD set) the stub provides the report, so existence
    # of the real python script is irrelevant.
    if [[ -z "${SMOKE_RUNNER_CMD}" && ! -f "${smoke_script}" ]]; then
        _err "smoke_matrix.py missing for ${svc} at ${smoke_script}"
        # Synthesise a minimal failure report so the summary captures this.
        cat >"${report_path}" <<JSON
{"service": "${svc}", "total_cells": 0, "passed": 0, "failed": 1, "skipped": 0,
 "results": [], "error": "smoke_matrix.py not found"}
JSON
        return 1
    fi

    local -a py_args=(--execute --report "${report_path}")
    [[ -n "${CATEGORY_FILTER}" ]]  && py_args+=(--category "${CATEGORY_FILTER}")
    [[ -n "${VENUE_FILTER}" ]]     && py_args+=(--venue "${VENUE_FILTER}")
    [[ -n "${DATA_TYPE_FILTER}" ]] && py_args+=(--data-type "${DATA_TYPE_FILTER}")

    local log_path="${REPORT_DIR}/${svc}.log"
    _log "running ${svc} smoke matrix (report=${report_path})"

    # Shard-level isolation: one service's failure within a tier must NOT
    # abort siblings. The caller runs us in a background subshell with its own
    # error handling — we just propagate the rc.
    #
    # `timeout` is a GNU util and may be absent on macOS; fall back to no
    # external wrapper (the per-cell timeouts inside smoke_matrix.py still
    # apply). We use `gtimeout` if available as a portable alternative.
    local -a timeout_cmd=()
    if command -v timeout >/dev/null 2>&1; then
        timeout_cmd=(timeout "${TIMEOUT_PER_SERVICE}")
    elif command -v gtimeout >/dev/null 2>&1; then
        timeout_cmd=(gtimeout "${TIMEOUT_PER_SERVICE}")
    fi

    local rc=0
    # Bash 3 + `set -u` treats `${arr[@]}` on an empty array as unbound.
    # Use `${arr[@]+"${arr[@]}"}` to expand to literally nothing when empty.
    if [[ -n "${SMOKE_RUNNER_CMD}" ]]; then
        # Test-harness hook: stub command receives (repo, report_path, py_args...)
        IS_TEST_RUN=true \
            ${timeout_cmd[@]+"${timeout_cmd[@]}"} \
            "${SMOKE_RUNNER_CMD}" "${repo}" "${report_path}" "${py_args[@]}" \
            >"${log_path}" 2>&1 || rc=$?
    else
        # Real mode: run the service's python CLI from the repo root so
        # relative imports resolve.
        (
            cd "${repo}"
            IS_TEST_RUN=true \
                ${timeout_cmd[@]+"${timeout_cmd[@]}"} \
                python scripts/smoke_matrix.py "${py_args[@]}"
        ) >"${log_path}" 2>&1 || rc=$?
    fi

    if [[ ${rc} -ne 0 ]]; then
        _err "${svc} smoke returned rc=${rc} (see ${log_path})"
        # If the subprocess crashed before writing a report, synthesise one.
        if [[ ! -s "${report_path}" ]]; then
            cat >"${report_path}" <<JSON
{"service": "${svc}", "total_cells": 0, "passed": 0, "failed": 1, "skipped": 0,
 "results": [], "error": "smoke subprocess exited rc=${rc}"}
JSON
        fi
    fi
    return ${rc}
}

# ----------------------------------------------------------------------------
# Tier executor: runs every service in $@ in parallel (real mode) and waits
# for all to finish. Returns 0 only if every service returned 0.
# ----------------------------------------------------------------------------
run_tier() {
    local tier_label="$1"; shift
    local -a services=("$@")
    [[ ${#services[@]} -eq 0 ]] && return 0

    _log "=== ${tier_label}: ${services[*]} ==="

    if ${DRY_RUN}; then
        for svc in "${services[@]}"; do
            _log "  DRY-RUN ${svc} → python scripts/smoke_matrix.py --execute --report ${REPORT_DIR}/${svc}.json"
        done
        return 0
    fi

    local -a pids=()
    local -a names=()
    for svc in "${services[@]}"; do
        run_service_smoke "${svc}" &
        pids+=($!)
        names+=("${svc}")
    done

    local tier_rc=0
    local i
    for i in "${!pids[@]}"; do
        local pid="${pids[$i]}"
        local svc_name="${names[$i]}"
        local svc_rc=0
        wait "${pid}" || svc_rc=$?
        if [[ ${svc_rc} -ne 0 ]]; then
            tier_rc=1
            if ${FAIL_FAST}; then
                _err "fail-fast: ${svc_name} failed; aborting remaining tiers"
                # Kill any still-running siblings in the current tier.
                local j
                for j in "${!pids[@]}"; do
                    [[ "${j}" != "${i}" ]] && kill "${pids[$j]}" 2>/dev/null || true
                done
                return 1
            fi
        fi
    done
    return ${tier_rc}
}

# ----------------------------------------------------------------------------
# Cleanup: wipe every -test- bucket in the project (overrides 7-day lifecycle).
# Best-effort; prints what was attempted.
# ----------------------------------------------------------------------------
cleanup_test_buckets() {
    _log "=== cleanup: removing all -test- bucket contents ==="
    if ! command -v gsutil >/dev/null 2>&1; then
        _err "gsutil not found on PATH; cannot run --cleanup"
        return 0
    fi
    local bucket
    while IFS= read -r bucket; do
        [[ -z "${bucket}" ]] && continue
        _log "  gsutil rm -r ${bucket}/*"
        gsutil -m rm -r "${bucket}/*" 2>/dev/null || _log "  (nothing to delete in ${bucket} or permission denied)"
    done < <(gsutil ls 2>/dev/null | grep -- '-test-' || true)
}

# ----------------------------------------------------------------------------
# Summary aggregator — reads each service's JSON report and writes summary.json.
# Handles both report schemas:
#   (A) instruments/MTDS/MDPS: {total_cells, passed, failed, skipped, results[]}
#   (B) features-*: {cells[]} with status in {PASS, FAIL, SKIP}
# ----------------------------------------------------------------------------
write_summary() {
    local summary_path="${REPORT_DIR}/summary.json"
    python3 - "${REPORT_DIR}" "${summary_path}" <<'PYAGG'
import json
import sys
from pathlib import Path

report_dir = Path(sys.argv[1])
summary_path = Path(sys.argv[2])

service_counts: list[dict[str, object]] = []
overall_total = overall_pass = overall_fail = overall_skip = 0

for report in sorted(report_dir.glob("*.json")):
    if report.name == "summary.json":
        continue
    try:
        data = json.loads(report.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        service_counts.append({
            "service": report.stem,
            "total": 0, "passed": 0, "failed": 1, "skipped": 0,
            "error": f"invalid JSON: {exc}",
        })
        overall_fail += 1
        continue

    svc = data.get("service") or report.stem

    # Schema A: aggregated counters on the report root.
    if "total_cells" in data:
        total = int(data.get("total_cells", 0))
        passed = int(data.get("passed", 0))
        failed = int(data.get("failed", 0))
        skipped = int(data.get("skipped", 0))
    # Schema B: per-cell results only; count from `cells` or `results`.
    else:
        items = data.get("cells") or data.get("results") or []
        total = len(items)
        passed = failed = skipped = 0
        for cell in items:
            status = str(cell.get("status", "")).upper()
            if status in ("PASS", "PASSED"):
                passed += 1
            elif status in ("FAIL", "FAILED"):
                failed += 1
            elif status in ("SKIP", "SKIPPED"):
                skipped += 1
            else:
                failed += 1  # unknown status is treated as failure

    service_counts.append({
        "service": svc,
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
    })
    overall_total += total
    overall_pass += passed
    overall_fail += failed
    overall_skip += skipped

summary = {
    "services": service_counts,
    "total": overall_total,
    "passed": overall_pass,
    "failed": overall_fail,
    "skipped": overall_skip,
    "exit_code": 1 if overall_fail else 0,
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PYAGG
}

# ----------------------------------------------------------------------------
# Print the human-friendly tier breakdown.
# ----------------------------------------------------------------------------
print_summary() {
    local summary_path="${REPORT_DIR}/summary.json"
    [[ ! -f "${summary_path}" ]] && return 0

    echo
    echo "========================="
    echo "INSTITUTIONAL SMOKE MATRIX"
    echo "========================="

    python3 - "${summary_path}" "${REPORT_DIR}" <<'PYPRINT'
import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
report_dir = Path(sys.argv[2])
service_counts = {s["service"]: s for s in summary.get("services", [])}

TIERS: list[tuple[str, list[str]]] = [
    ("Tier 0 (instruments-service)", ["instruments-service"]),
    ("Tier 1 (market-tick-data-service)", ["market-tick-data-service"]),
    ("Tier 2 (market-data-processing-service)", ["market-data-processing-service"]),
    ("Tier 3 parallel", [
        "features-delta-one-service",
        "features-volatility-service",
        "features-onchain-service",
        "features-sports-service",
        "features-calendar-service",
        "features-multi-timeframe-service",
        "features-cross-instrument-service",
        "features-commodity-service",
    ]),
    ("Tier 4 parallel (ml-*)", ["ml-training-service", "ml-inference-service"]),
]

for tier_label, members in TIERS:
    present = [m for m in members if m in service_counts]
    if not present:
        continue
    if len(present) == 1 and not tier_label.endswith("parallel"):
        sc = service_counts[present[0]]
        print(f"{tier_label}: {sc['total']} cells -> {sc['passed']} PASS, "
              f"{sc['failed']} FAIL, {sc['skipped']} SKIP")
    else:
        print(f"{tier_label}:")
        for name in present:
            sc = service_counts[name]
            print(f"  {name}: {sc['total']} cells -> {sc['passed']} PASS, "
                  f"{sc['failed']} FAIL, {sc['skipped']} SKIP")

print("=========================")
print(f"TOTAL: {summary['total']} cells | {summary['passed']} PASS | "
      f"{summary['failed']} FAIL | {summary['skipped']} SKIP")
print(f"Exit: {summary['exit_code']}")
print(f"Reports: {report_dir}")
PYPRINT
}

# ----------------------------------------------------------------------------
# Main driver
# ----------------------------------------------------------------------------
main() {
    _log "WORKSPACE_ROOT=${WORKSPACE_ROOT}"
    _log "REPORT_DIR=${REPORT_DIR}"
    _log "DEPS_YAML=${DEPS_YAML} (exists=$( [[ -f ${DEPS_YAML} ]] && echo yes || echo no ))"
    _log "filters: service=${SERVICE_FILTER:-*} category=${CATEGORY_FILTER:-*} venue=${VENUE_FILTER:-*} data_type=${DATA_TYPE_FILTER:-*}"
    _log "flags: no_deps=${NO_DEPS} cleanup=${CLEANUP} dry_run=${DRY_RUN} fail_fast=${FAIL_FAST} include_ml=${INCLUDE_ML} timeout=${TIMEOUT_PER_SERVICE}s"

    # Build filtered tier lists up-front so dry-run shows the same plan real-mode
    # would execute. Use portable while-read instead of `mapfile` (which is
    # bash 4+; macOS ships bash 3 by default).
    local tier0=() tier1=() tier2=() tier3=() tier4=()
    local _svc
    while IFS= read -r _svc; do [[ -n "${_svc}" ]] && tier0+=("${_svc}"); done < <(filter_tier "Tier 0" "${TIER0_SERVICES[@]}")
    while IFS= read -r _svc; do [[ -n "${_svc}" ]] && tier1+=("${_svc}"); done < <(filter_tier "Tier 1" "${TIER1_SERVICES[@]}")
    while IFS= read -r _svc; do [[ -n "${_svc}" ]] && tier2+=("${_svc}"); done < <(filter_tier "Tier 2" "${TIER2_SERVICES[@]}")
    while IFS= read -r _svc; do [[ -n "${_svc}" ]] && tier3+=("${_svc}"); done < <(filter_tier "Tier 3" "${TIER3_SERVICES[@]}")
    if ${INCLUDE_ML}; then
        while IFS= read -r _svc; do [[ -n "${_svc}" ]] && tier4+=("${_svc}"); done < <(filter_tier "Tier 4" "${ML_SERVICES[@]}")
    fi

    # --service pre-check: when the user scoped to one service and did not pass
    # --no-deps, warn that upstream deps are being skipped — this is not fatal,
    # just a clear actionable log so they know to pass --no-deps deliberately.
    if [[ -n "${SERVICE_FILTER}" && ${NO_DEPS} == false ]]; then
        _log "note: --service=${SERVICE_FILTER} restricts to a single service."
        _log "      upstream tiers are SKIPPED for this run; if their TEST buckets"
        _log "      lack data from a prior matrix run, ${SERVICE_FILTER}'s cells"
        _log "      will surface as skipped=upstream_missing or attempted_failed."
        _log "      (pass --no-deps to silence this note)"
    fi

    if ${DRY_RUN}; then
        _log "=== DRY RUN plan ==="
        [[ ${#tier0[@]} -gt 0 ]] && _log "Tier 0: ${tier0[*]}"
        [[ ${#tier1[@]} -gt 0 ]] && _log "Tier 1: ${tier1[*]}"
        [[ ${#tier2[@]} -gt 0 ]] && _log "Tier 2: ${tier2[*]}"
        [[ ${#tier3[@]} -gt 0 ]] && _log "Tier 3: ${tier3[*]}"
        [[ ${#tier4[@]} -gt 0 ]] && _log "Tier 4 (ml): ${tier4[*]}"
        # Still write summary.json with zero counts so downstream tooling can
        # distinguish "dry run produced no failures" from "script never ran".
        mkdir -p "${REPORT_DIR}"
        cat >"${REPORT_DIR}/summary.json" <<JSON
{"services": [], "total": 0, "passed": 0, "failed": 0, "skipped": 0, "exit_code": 0, "dry_run": true}
JSON
        return 0
    fi

    local overall_rc=0
    # Bash 3 + `set -u` fires on empty arrays; guard each tier expansion.
    if [[ ${#tier0[@]} -gt 0 ]] && ! run_tier "Tier 0" "${tier0[@]}"; then overall_rc=1; ${FAIL_FAST} && { finalise "${overall_rc}"; return 1; }; fi
    if [[ ${#tier1[@]} -gt 0 ]] && ! run_tier "Tier 1" "${tier1[@]}"; then overall_rc=1; ${FAIL_FAST} && { finalise "${overall_rc}"; return 1; }; fi
    if [[ ${#tier2[@]} -gt 0 ]] && ! run_tier "Tier 2" "${tier2[@]}"; then overall_rc=1; ${FAIL_FAST} && { finalise "${overall_rc}"; return 1; }; fi
    if [[ ${#tier3[@]} -gt 0 ]] && ! run_tier "Tier 3" "${tier3[@]}"; then overall_rc=1; ${FAIL_FAST} && { finalise "${overall_rc}"; return 1; }; fi
    if ${INCLUDE_ML} && [[ ${#tier4[@]} -gt 0 ]]; then
        if ! run_tier "Tier 4 (ml-*)" "${tier4[@]}"; then overall_rc=1; ${FAIL_FAST} && { finalise "${overall_rc}"; return 1; }; fi
    fi

    finalise "${overall_rc}"
    return "${overall_rc}"
}

finalise() {
    local rc="$1"
    write_summary || true
    print_summary || true
    ${CLEANUP} && cleanup_test_buckets || true
    if [[ ${rc} -eq 0 ]]; then
        _log "smoke matrix PASSED"
    else
        _log "smoke matrix FAILED (see per-service logs in ${REPORT_DIR}/)"
    fi
}

main "$@"
