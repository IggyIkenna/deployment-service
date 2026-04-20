"""Enforce the per-(service, category) smoke-coverage floor.

Reads the latest `summary.json` produced by
`deployment-service/scripts/run-smoke-matrix.sh` and fails (exits non-zero) if
any `(service, category)` pair has fewer green cells than the floor declared
in `deployment-service/configs/smoke-coverage-floor.yaml`.

Invocation:

    python deployment-service/scripts/enforce-smoke-coverage-floor.py \
        --summary <path-to-summary.json> \
        [--floor-config <path-to-smoke-coverage-floor.yaml>]

Exit codes:
  0 — every (service, category) meets or exceeds the floor.
  1 — one or more (service, category) regressions. Details to stderr.
  2 — CLI misuse (missing summary, malformed config, etc).

Design notes:
- Floor values may be `null` to mark architecturally-unsupported (service,
  category) pairs — those are skipped rather than treated as zero. This
  matches the Phase 2 smoke matrix convention where unsupported cells emit
  `status=SKIP`, not FAIL.
- The enforcer only looks at `passed` (green) counts. `skipped` is explicitly
  NOT counted as green — `skipped=upstream_missing` / `architecturally_unsupported`
  must not silently satisfy the floor.
- The summary.json schema is the aggregator output from run-smoke-matrix.sh:
  `{services: [{service, total, passed, failed, skipped}], total, passed, ...}`.
  The per-cell breakdown (with category) lives in the per-service JSON reports
  under the same report directory — we walk those if the summary doesn't
  already carry category granularity.

SSOT:
  unified-trading-pm/plans/active/institutional_smoke_matrix_2026_04_20.plan.md
  unified-trading-pm/codex/14-playbooks/smoke-testing-playbook.md
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import yaml

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEPLOYMENT_ROOT = _SCRIPT_DIR.parent
_DEFAULT_FLOOR_CONFIG = _DEPLOYMENT_ROOT / "configs" / "smoke-coverage-floor.yaml"


class FloorConfigError(Exception):
    """Raised when the floor YAML fails validation."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enforce per-(service, category) smoke-coverage floor.",
    )
    parser.add_argument(
        "--summary",
        required=True,
        type=Path,
        help="Path to the run-smoke-matrix.sh summary.json (or a directory "
        "containing it + per-service reports).",
    )
    parser.add_argument(
        "--floor-config",
        default=_DEFAULT_FLOOR_CONFIG,
        type=Path,
        help=f"Path to smoke-coverage-floor.yaml (default: {_DEFAULT_FLOOR_CONFIG})",
    )
    parser.add_argument(
        "--strict-missing",
        action="store_true",
        help="Fail if a service appears in summary.json but not in the floor config. "
        "Default: warn + skip.",
    )
    return parser.parse_args()


def _load_floor(floor_path: Path) -> dict[str, dict[str, int | None]]:
    """Load + validate the floor YAML. Returns {service: {category: int|None}}."""
    if not floor_path.exists():
        raise FloorConfigError(f"floor config not found: {floor_path}")
    try:
        raw = yaml.safe_load(floor_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise FloorConfigError(f"malformed YAML in {floor_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise FloorConfigError(f"floor config root must be a mapping, got {type(raw).__name__}")
    cells = raw.get("cells")
    if not isinstance(cells, dict):
        raise FloorConfigError("floor config missing top-level `cells` mapping")

    normalised: dict[str, dict[str, int | None]] = {}
    for service, per_cat in cells.items():
        if not isinstance(per_cat, dict):
            raise FloorConfigError(
                f"cells.{service} must be a mapping of category to int|null, got {type(per_cat).__name__}"
            )
        svc_map: dict[str, int | None] = {}
        for category, value in per_cat.items():
            cat_key = str(category).upper()
            if value is None:
                svc_map[cat_key] = None
                continue
            if not isinstance(value, int) or isinstance(value, bool):
                raise FloorConfigError(
                    f"cells.{service}.{category} must be int or null, got {type(value).__name__}"
                )
            if value < 0:
                raise FloorConfigError(
                    f"cells.{service}.{category} must be >=0 (floor is a lower bound), got {value}"
                )
            svc_map[cat_key] = value
        normalised[service] = svc_map
    return normalised


def _resolve_summary_path(summary_arg: Path) -> tuple[Path, Path]:
    """Return (summary_json_path, report_dir). Accepts a file or a directory."""
    if summary_arg.is_dir():
        candidate = summary_arg / "summary.json"
        if not candidate.exists():
            raise FileNotFoundError(f"no summary.json in directory: {summary_arg}")
        return candidate, summary_arg
    if not summary_arg.exists():
        raise FileNotFoundError(f"summary file not found: {summary_arg}")
    return summary_arg, summary_arg.parent


def _count_green_by_category(summary_path: Path, report_dir: Path) -> dict[str, dict[str, int]]:
    """Return {service: {category: green_count}} by walking per-service reports.

    Each per-service smoke_matrix.py writes either:
      Schema A: {total_cells, passed, failed, skipped, results: [{category, ...}]}
      Schema B: {cells: [{category, status, ...}]}
    Both include a `category` field on the per-cell entry (lower- or
    upper-case, we uppercase on read). Cells without an explicit category are
    bucketed under "_UNCATEGORISED" so they still contribute to the service
    total but cannot satisfy a category floor.
    """
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("dry_run"):
        # Dry run — nothing to enforce. Caller's responsibility to skip.
        return {}

    service_names = [s["service"] for s in summary.get("services", []) if "service" in s]
    green_by_svc_cat: dict[str, dict[str, int]] = {s: defaultdict(int) for s in service_names}

    for report in sorted(report_dir.glob("*.json")):
        if report.name == "summary.json":
            continue
        try:
            data = json.loads(report.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # Malformed report => every cell implicitly failed; summary.json
            # already accounts for that, so just skip category accounting here.
            continue
        svc = data.get("service") or report.stem
        green_by_svc_cat.setdefault(svc, defaultdict(int))
        items = data.get("results") or data.get("cells") or []
        for cell in items:
            if not isinstance(cell, dict):
                continue
            status = str(cell.get("status", "")).upper()
            if status not in ("PASS", "PASSED"):
                continue
            category = str(cell.get("category", "")).upper() or "_UNCATEGORISED"
            green_by_svc_cat[svc][category] += 1

    # Convert inner defaultdicts to regular dicts for tidy error messages.
    return {svc: dict(cats) for svc, cats in green_by_svc_cat.items()}


def _enforce(
    floor: dict[str, dict[str, int | None]],
    green: dict[str, dict[str, int]],
    *,
    strict_missing: bool,
) -> list[str]:
    """Return list of regression messages (empty => pass).

    Shard-level failure isolation: every (service, category) pair is checked
    independently. A single regression does NOT short-circuit the rest of the
    matrix — the full report of regressions is returned to the caller.
    """
    regressions: list[str] = []

    for service, per_cat in floor.items():
        observed = green.get(service, {})
        for category, floor_value in per_cat.items():
            if floor_value is None:
                # Architecturally unsupported — skip even if summary reports
                # green cells (would indicate an over-count but not a regression).
                continue
            observed_green = observed.get(category, 0)
            if observed_green < floor_value:
                regressions.append(
                    f"REGRESSION: {service} / {category}: "
                    f"green={observed_green} < floor={floor_value}"
                )

    # Services in summary.json but not in floor config
    for service in green.keys() - floor.keys():
        msg = f"{service} in summary but not declared in smoke-coverage-floor.yaml"
        if strict_missing:
            regressions.append(f"MISSING: {msg}")
        else:
            print(f"WARN: {msg} (add an entry or pass --strict-missing)", file=sys.stderr)

    return regressions


def main() -> int:
    args = _parse_args()
    try:
        summary_path, report_dir = _resolve_summary_path(args.summary)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        floor = _load_floor(args.floor_config)
    except FloorConfigError as exc:
        print(f"ERROR: invalid floor config: {exc}", file=sys.stderr)
        return 2

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("dry_run"):
        print("Smoke matrix was a DRY RUN; floor enforcement skipped.")
        return 0

    green = _count_green_by_category(summary_path, report_dir)
    regressions = _enforce(floor, green, strict_missing=args.strict_missing)

    if not regressions:
        print("Smoke-coverage floor: OK (no regressions).")
        return 0

    print("Smoke-coverage floor: FAIL", file=sys.stderr)
    for line in regressions:
        print(f"  {line}", file=sys.stderr)
    print(
        "\nTo ratchet the floor UP after a confirmed green run, edit "
        f"{args.floor_config} and commit with a conventional "
        "`chore(smoke): ratchet ...` prefix. Never lower the floor without an "
        "admin-override audit entry.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
