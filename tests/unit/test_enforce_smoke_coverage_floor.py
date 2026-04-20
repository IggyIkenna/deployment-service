"""Unit tests for deployment-service/scripts/enforce-smoke-coverage-floor.py.

Exercises the per-(service, category) floor gate with synthetic summary.json +
per-service report files. Tests cover:

- Zero-baseline floor passes when no reports exist (green=0 >= floor=0).
- Non-zero floor passes when observed green >= floor.
- Regression surfaces as rc=1 with actionable message to stderr.
- `null` floor entries are treated as architecturally unsupported (skipped).
- Malformed floor YAML exits rc=2.
- Dry-run summary.json is accepted (floor enforcement skipped).
- `--strict-missing` flips warn → fail for services missing from the floor.
- Shard isolation: multiple regressions are reported, not short-circuited.

Follows the existing test pattern in test_run_smoke_matrix.py (shell-out via
subprocess to the real script under test, synthetic inputs in tmp_path).
"""

from __future__ import annotations

import json
import os
import subprocess  # nosec B404 — invokes our own python script under test
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENFORCER = _REPO_ROOT / "scripts" / "enforce-smoke-coverage-floor.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_floor(
    path: Path,
    *,
    cells: dict[str, dict[str, int | None]],
    baseline_date: str = "2026-04-20",
) -> None:
    """Write a minimal smoke-coverage-floor.yaml for a given cells dict."""
    import yaml

    path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "baseline_date": baseline_date,
                "cells": cells,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _write_summary_and_reports(
    report_dir: Path,
    per_service_cells: dict[str, list[dict[str, str]]],
    *,
    dry_run: bool = False,
) -> Path:
    """Write a summary.json + one per-service JSON report per service.

    `per_service_cells` maps service name → list of cell dicts (each dict has
    `category` + `status`). The summary.json aggregates counts across them,
    matching the schema the orchestrator emits.
    """
    services = []
    total = passed = failed = skipped = 0
    for svc, cells in per_service_cells.items():
        svc_pass = sum(1 for c in cells if c["status"].upper() in ("PASS", "PASSED"))
        svc_fail = sum(1 for c in cells if c["status"].upper() in ("FAIL", "FAILED"))
        svc_skip = sum(1 for c in cells if c["status"].upper() in ("SKIP", "SKIPPED"))
        svc_total = len(cells)
        services.append(
            {
                "service": svc,
                "total": svc_total,
                "passed": svc_pass,
                "failed": svc_fail,
                "skipped": svc_skip,
            }
        )
        # Write per-service report in schema A.
        (report_dir / f"{svc}.json").write_text(
            json.dumps(
                {
                    "service": svc,
                    "total_cells": svc_total,
                    "passed": svc_pass,
                    "failed": svc_fail,
                    "skipped": svc_skip,
                    "results": cells,
                }
            ),
            encoding="utf-8",
        )
        total += svc_total
        passed += svc_pass
        failed += svc_fail
        skipped += svc_skip

    summary = {
        "services": services,
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "exit_code": 1 if failed else 0,
    }
    if dry_run:
        summary["dry_run"] = True
    summary_path = report_dir / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    return summary_path


def _run_enforcer(
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # nosec B603 — invokes our own python script
        [sys.executable, str(_ENFORCER), *args],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        check=False,
        timeout=30,
    )


# ---------------------------------------------------------------------------
# 1. Zero-baseline floor: any green count >= 0 passes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_zero_baseline_floor_passes_when_no_green_cells(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 0, "TRADFI": 0}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {"instruments-service": [{"category": "CEFI", "status": "SKIP"}]},
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


# ---------------------------------------------------------------------------
# 2. Non-zero floor met by green cells: passes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_floor_met_passes(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 2}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {
            "instruments-service": [
                {"category": "CEFI", "status": "PASS"},
                {"category": "CEFI", "status": "PASS"},
                {"category": "CEFI", "status": "PASS"},
            ]
        },
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# 3. Regression: observed green < floor
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_regression_exits_one_and_prints_actionable_error(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 5}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {
            "instruments-service": [
                {"category": "CEFI", "status": "PASS"},
                {"category": "CEFI", "status": "PASS"},
            ]
        },
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 1
    assert "REGRESSION" in result.stderr
    assert "instruments-service" in result.stderr
    assert "CEFI" in result.stderr
    assert "green=2" in result.stderr
    assert "floor=5" in result.stderr


# ---------------------------------------------------------------------------
# 4. `null` entries are architecturally unsupported -> skipped
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_null_floor_is_skipped_not_enforced(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"features-sports-service": {"CEFI": None, "SPORTS": 0}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {"features-sports-service": [{"category": "SPORTS", "status": "PASS"}]},
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# 5. Malformed floor YAML exits rc=2
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_malformed_floor_exits_two(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    floor.write_text("not: a: valid: yaml: at: all", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(reports, {})

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 2
    assert "ERROR" in result.stderr


@pytest.mark.unit
def test_floor_missing_cells_key_exits_two(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    floor.write_text("version: 1\nbaseline_date: '2026-04-20'\n", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(reports, {})

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 2
    assert "cells" in result.stderr


@pytest.mark.unit
def test_floor_negative_int_rejected(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": -1}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(reports, {})

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 2
    assert ">=0" in result.stderr


# ---------------------------------------------------------------------------
# 6. Dry-run summary.json is accepted
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dry_run_summary_skips_enforcement(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 100}})  # huge floor
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(reports, {}, dry_run=True)

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 0
    assert "DRY RUN" in result.stdout.upper()


# ---------------------------------------------------------------------------
# 7. --strict-missing flips warn to fail
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_missing_service_is_warning_by_default(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 0}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {"some-new-service": [{"category": "CEFI", "status": "PASS"}]},
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 0
    assert "WARN" in result.stderr
    assert "some-new-service" in result.stderr


@pytest.mark.unit
def test_strict_missing_fails_for_unknown_service(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 0}})
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {"some-new-service": [{"category": "CEFI", "status": "PASS"}]},
    )

    result = _run_enforcer(
        "--summary",
        str(summary),
        "--floor-config",
        str(floor),
        "--strict-missing",
    )
    assert result.returncode == 1
    assert "MISSING" in result.stderr


# ---------------------------------------------------------------------------
# 8. Shard isolation — multiple regressions are ALL reported
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_multiple_regressions_all_reported(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(
        floor,
        cells={
            "instruments-service": {"CEFI": 3, "TRADFI": 2},
            "market-tick-data-service": {"DEFI": 1},
        },
    )
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = _write_summary_and_reports(
        reports,
        {
            "instruments-service": [
                # 1 CEFI green (floor 3) + 0 TRADFI green (floor 2) => both regress
                {"category": "CEFI", "status": "PASS"},
                {"category": "TRADFI", "status": "FAIL"},
            ],
            "market-tick-data-service": [
                # 0 DEFI green (floor 1) => regresses
                {"category": "DEFI", "status": "SKIP"},
            ],
        },
    )

    result = _run_enforcer("--summary", str(summary), "--floor-config", str(floor))
    assert result.returncode == 1
    # All three regressions must appear in stderr.
    assert "instruments-service / CEFI" in result.stderr
    assert "instruments-service / TRADFI" in result.stderr
    assert "market-tick-data-service / DEFI" in result.stderr


# ---------------------------------------------------------------------------
# 9. Accepts a directory as --summary (resolves to dir/summary.json)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_summary_accepts_directory(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 0}})
    reports = tmp_path / "reports"
    reports.mkdir()
    _write_summary_and_reports(
        reports,
        {"instruments-service": [{"category": "CEFI", "status": "PASS"}]},
    )

    # Pass the directory, not the file.
    result = _run_enforcer("--summary", str(reports), "--floor-config", str(floor))
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# 10. Missing summary -> rc=2
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_missing_summary_exits_two(tmp_path: Path) -> None:
    floor = tmp_path / "floor.yaml"
    _write_floor(floor, cells={"instruments-service": {"CEFI": 0}})

    result = _run_enforcer("--summary", str(tmp_path / "nope.json"), "--floor-config", str(floor))
    assert result.returncode == 2
    assert "not found" in result.stderr
