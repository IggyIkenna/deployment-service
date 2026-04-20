"""Unit tests for deployment-service/scripts/run-smoke-matrix.sh.

Exercises the workspace smoke-matrix orchestrator with a stubbed per-service
runner (via SMOKE_RUNNER_CMD) so we can validate:

- `--dry-run` enumerates every tier without invoking the per-service CLI
- `--service X` restricts dispatch to one service
- `--fail-fast` aborts on first service failure
- `--include-ml` adds the ml-* services to dispatch
- Non-zero exit when any cell reports FAIL (stubbed JSON)
- Summary.json schema aggregates both schema-A and schema-B reports

The stub is a tiny bash script the orchestrator invokes in place of
`python scripts/smoke_matrix.py`. It writes a canned JSON report and exits
with a configurable rc.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess  # nosec B404 — invokes our own shell script under test
from pathlib import Path
from typing import Literal

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ORCHESTRATOR = _REPO_ROOT / "scripts" / "run-smoke-matrix.sh"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_stub(
    stub_path: Path,
    *,
    rc: int = 0,
    schema: Literal["A", "B"] = "A",
    failed_cells: int = 0,
    passed_cells: int = 1,
    skipped_cells: int = 0,
    fail_for_services: tuple[str, ...] = (),
) -> None:
    """Write a stub command that mimics a per-service smoke_matrix.py run.

    The stub receives (repo-path, report-path, py-args...). It synthesises a
    JSON report at report-path and exits rc.

    When ``fail_for_services`` is provided, only those service basenames
    return the non-zero ``rc`` and failing counts; others return rc=0 with
    passing counts (covers the fail-fast test).
    """
    fail_list = " ".join(fail_for_services)
    # Stub uses python3 inline to emit a canned JSON report — keeps the stub
    # simple and avoids bash heredoc counting bugs.
    content = f"""#!/usr/bin/env bash
# Stub per-service smoke runner for test_run_smoke_matrix.py.
set -eu
REPO="$1"
REPORT="$2"
shift 2

SERVICE_NAME=$(basename "${{REPO}}")
FAIL_LIST="{fail_list}"

# Decide whether this service should emit a failure.
SERVICE_RC={rc}
SERVICE_PASSED={passed_cells}
SERVICE_FAILED={failed_cells}
SERVICE_SKIPPED={skipped_cells}
if [[ -n "${{FAIL_LIST}}" ]]; then
    SHOULD_FAIL=false
    for svc in ${{FAIL_LIST}}; do
        if [[ "${{SERVICE_NAME}}" == "${{svc}}" ]]; then
            SHOULD_FAIL=true
            break
        fi
    done
    if ! ${{SHOULD_FAIL}}; then
        SERVICE_RC=0
        SERVICE_PASSED=1
        SERVICE_FAILED=0
        SERVICE_SKIPPED=0
    fi
fi

mkdir -p "$(dirname "${{REPORT}}")"
python3 - "${{SERVICE_NAME}}" "${{REPORT}}" "${{SERVICE_PASSED}}" "${{SERVICE_FAILED}}" "${{SERVICE_SKIPPED}}" "{schema}" <<'PYJSON'
import json
import sys
svc, report_path, passed, failed, skipped, schema = sys.argv[1:7]
passed, failed, skipped = int(passed), int(failed), int(skipped)
if schema == "A":
    payload = {{
        "service": svc,
        "total_cells": passed + failed + skipped,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "results": [],
    }}
else:
    cells = (
        [{{"status": "PASS"}} for _ in range(passed)]
        + [{{"status": "FAIL"}} for _ in range(failed)]
        + [{{"status": "SKIP"}} for _ in range(skipped)]
    )
    payload = {{"service": svc, "cells": cells}}
with open(report_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh)
PYJSON
exit ${{SERVICE_RC}}
"""
    stub_path.write_text(content, encoding="utf-8")
    stub_path.chmod(stub_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _run_orchestrator(
    *args: str,
    env_overrides: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(  # nosec B603 — shell script under test
        ["bash", str(_ORCHESTRATOR), *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(cwd) if cwd else None,
        check=False,
        timeout=60,
    )


@pytest.fixture
def tmp_report_dir(tmp_path: Path) -> Path:
    d = tmp_path / "reports"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# 1. --dry-run enumerates tiers without invoking the per-service CLI
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dry_run_enumerates_all_default_tiers(tmp_report_dir: Path) -> None:
    result = _run_orchestrator("--dry-run", "--report-dir", str(tmp_report_dir))

    assert result.returncode == 0, result.stderr
    assert "Tier 0: instruments-service" in result.stdout
    assert "Tier 1: market-tick-data-service" in result.stdout
    assert "Tier 2: market-data-processing-service" in result.stdout
    assert "Tier 3: features-delta-one-service" in result.stdout
    # Default: ml-* excluded
    assert "Tier 4" not in result.stdout
    # dry-run writes a placeholder summary.json
    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["exit_code"] == 0
    assert summary["dry_run"] is True


@pytest.mark.unit
def test_dry_run_does_not_invoke_per_service_stub(tmp_path: Path, tmp_report_dir: Path) -> None:
    """Dry-run must never call the per-service runner, even when a stub is set."""
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=99)  # would make the test fail if stub ever runs

    result = _run_orchestrator(
        "--dry-run",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0
    # No per-service JSON report should exist (only the dry-run summary).
    reports = list(tmp_report_dir.glob("*.json"))
    assert len(reports) == 1
    assert reports[0].name == "summary.json"


# ---------------------------------------------------------------------------
# 2. --service X runs only X (via mocked runner)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_service_filter_runs_only_one_service(tmp_path: Path, tmp_report_dir: Path) -> None:
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=0, schema="A", passed_cells=3)

    result = _run_orchestrator(
        "--service",
        "market-tick-data-service",
        "--no-deps",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr

    # Exactly one service report (plus summary).
    per_service = [p for p in tmp_report_dir.glob("*.json") if p.name != "summary.json"]
    assert len(per_service) == 1
    assert per_service[0].name == "market-tick-data-service.json"

    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["services"][0]["service"] == "market-tick-data-service"
    assert summary["total"] == 3
    assert summary["passed"] == 3
    assert summary["failed"] == 0


# ---------------------------------------------------------------------------
# 3. --fail-fast aborts on first service failure
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fail_fast_aborts_on_first_failure(tmp_path: Path, tmp_report_dir: Path) -> None:
    # Make Tier 0 (instruments-service) fail — fail-fast should prevent Tier 1+
    # from running.
    stub = tmp_path / "stub.sh"
    _write_stub(
        stub,
        rc=1,
        schema="A",
        passed_cells=0,
        failed_cells=5,
        fail_for_services=("instruments-service",),
    )

    result = _run_orchestrator(
        "--fail-fast",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 1

    # Only instruments-service should have produced a report.
    per_service = [p.name for p in tmp_report_dir.glob("*.json") if p.name != "summary.json"]
    assert "instruments-service.json" in per_service
    assert "market-tick-data-service.json" not in per_service
    assert "market-data-processing-service.json" not in per_service

    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["failed"] >= 1
    assert summary["exit_code"] == 1


@pytest.mark.unit
def test_continue_on_failure_when_no_fail_fast(tmp_path: Path, tmp_report_dir: Path) -> None:
    # Without --fail-fast, a Tier 0 failure must not prevent Tier 1+.
    stub = tmp_path / "stub.sh"
    _write_stub(
        stub,
        rc=1,
        schema="A",
        passed_cells=0,
        failed_cells=1,
        fail_for_services=("instruments-service",),
    )

    result = _run_orchestrator(
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    # Overall rc is 1 (instruments failed) but other services DID run.
    assert result.returncode == 1
    per_service = {p.name for p in tmp_report_dir.glob("*.json") if p.name != "summary.json"}
    assert "instruments-service.json" in per_service
    assert "market-tick-data-service.json" in per_service
    assert "market-data-processing-service.json" in per_service


# ---------------------------------------------------------------------------
# 4. --include-ml adds ml-* to dispatch
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_include_ml_adds_ml_services_to_dispatch(tmp_path: Path, tmp_report_dir: Path) -> None:
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=0, schema="A", passed_cells=1)

    result = _run_orchestrator(
        "--include-ml",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr

    per_service = {p.name for p in tmp_report_dir.glob("*.json") if p.name != "summary.json"}
    assert "ml-training-service.json" in per_service
    assert "ml-inference-service.json" in per_service


@pytest.mark.unit
def test_ml_excluded_by_default(tmp_path: Path, tmp_report_dir: Path) -> None:
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=0, schema="A", passed_cells=1)

    result = _run_orchestrator(
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr
    per_service = {p.name for p in tmp_report_dir.glob("*.json") if p.name != "summary.json"}
    assert "ml-training-service.json" not in per_service
    assert "ml-inference-service.json" not in per_service


# ---------------------------------------------------------------------------
# 5. Non-zero exit when any cell reports FAIL (via stubbed JSON)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_non_zero_exit_when_any_cell_fails(tmp_path: Path, tmp_report_dir: Path) -> None:
    # Single failing cell anywhere in the matrix must flip overall exit to 1.
    stub = tmp_path / "stub.sh"
    _write_stub(
        stub,
        rc=1,
        schema="A",
        passed_cells=0,
        failed_cells=1,
        fail_for_services=("features-delta-one-service",),
    )

    result = _run_orchestrator(
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 1

    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["failed"] == 1
    assert summary["exit_code"] == 1


@pytest.mark.unit
def test_all_green_exits_zero(tmp_path: Path, tmp_report_dir: Path) -> None:
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=0, schema="A", passed_cells=2)

    result = _run_orchestrator(
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr

    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["failed"] == 0
    assert summary["exit_code"] == 0
    # 11 services in default matrix (1 + 1 + 1 + 8) × 2 passing cells each.
    assert summary["passed"] == 22


# ---------------------------------------------------------------------------
# 6. Schema-B (features-*) reports are aggregated correctly
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_schema_b_cells_are_counted(tmp_path: Path, tmp_report_dir: Path) -> None:
    stub = tmp_path / "stub.sh"
    _write_stub(stub, rc=0, schema="B", passed_cells=2, failed_cells=0, skipped_cells=1)

    result = _run_orchestrator(
        "--service",
        "features-delta-one-service",
        "--no-deps",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr

    summary = json.loads((tmp_report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["services"][0]["service"] == "features-delta-one-service"
    assert summary["total"] == 3
    assert summary["passed"] == 2
    assert summary["skipped"] == 1


# ---------------------------------------------------------------------------
# 7. Help is available and exits cleanly
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_help_exits_zero() -> None:
    result = _run_orchestrator("--help")
    assert result.returncode == 0
    assert "--service" in result.stdout
    assert "--include-ml" in result.stdout
    assert "--fail-fast" in result.stdout


# ---------------------------------------------------------------------------
# 8. Unknown option is rejected
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_unknown_option_exits_nonzero() -> None:
    result = _run_orchestrator("--totally-not-a-flag")
    assert result.returncode == 2
    assert "Unknown option" in result.stderr


# ---------------------------------------------------------------------------
# 9. Missing per-service smoke script surfaces as a failure
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_missing_smoke_script_surfaces_as_failure(tmp_path: Path, tmp_report_dir: Path) -> None:
    """If a service's scripts/smoke_matrix.py is missing, the orchestrator must
    synthesise a failure report rather than crashing silently."""
    # Build a throw-away workspace with a fake repo that has no smoke script.
    fake_ws = tmp_path / "fake-workspace"
    fake_ws.mkdir()
    fake_deploy = fake_ws / "deployment-service"
    fake_deploy_scripts = fake_deploy / "scripts"
    fake_deploy_scripts.mkdir(parents=True)
    fake_configs = fake_deploy / "configs"
    fake_configs.mkdir()
    # Copy the orchestrator + a stub deps yaml into the fake workspace.
    shutil.copy(_ORCHESTRATOR, fake_deploy_scripts / "run-smoke-matrix.sh")
    (fake_deploy_scripts / "run-smoke-matrix.sh").chmod(0o755)
    (fake_configs / "dependencies.yaml").write_text("services: {}\n", encoding="utf-8")
    # No instruments-service repo at all — smoke script is missing.

    fake_orch = fake_deploy_scripts / "run-smoke-matrix.sh"
    reports = fake_ws / "reports"
    reports.mkdir()

    result = subprocess.run(  # nosec B603 — self-owned shell script
        [
            "bash",
            str(fake_orch),
            "--service",
            "instruments-service",
            "--no-deps",
            "--report-dir",
            str(reports),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 1
    report = reports / "instruments-service.json"
    assert report.exists()
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["failed"] >= 1
    assert "not found" in data.get("error", "").lower()


# ---------------------------------------------------------------------------
# 10. Category/venue/data-type filters are forwarded
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filters_are_forwarded_to_stub(tmp_path: Path, tmp_report_dir: Path) -> None:
    """The stub echoes the arguments it received; we verify the filters arrive."""
    stub = tmp_path / "stub.sh"
    stub.write_text(
        """#!/usr/bin/env bash
set -eu
REPO="$1"
REPORT="$2"
shift 2
# Persist the args for the test to inspect.
echo "$@" >"${REPORT}.args"
# Minimal valid report so summary aggregation succeeds.
cat >"${REPORT}" <<JSON
{"service": "$(basename "${REPO}")", "total_cells": 1, "passed": 1, "failed": 0, "skipped": 0, "results": []}
JSON
""",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    result = _run_orchestrator(
        "--service",
        "instruments-service",
        "--no-deps",
        "--category",
        "CEFI",
        "--venue",
        "BINANCE-SPOT",
        "--data-type",
        "trades",
        "--report-dir",
        str(tmp_report_dir),
        env_overrides={"SMOKE_RUNNER_CMD": str(stub)},
    )
    assert result.returncode == 0, result.stderr
    args_file = tmp_report_dir / "instruments-service.json.args"
    assert args_file.exists()
    args_text = args_file.read_text(encoding="utf-8")
    assert "--category CEFI" in args_text
    assert "--venue BINANCE-SPOT" in args_text
    assert "--data-type trades" in args_text
    assert "--execute" in args_text
