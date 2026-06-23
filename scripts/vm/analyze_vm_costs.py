#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Analyze GCE VM costs from vm-logs directory in GCS.

Uses two fast GCS operations:
  1. ``gsutil ls gs://{code_bucket}/vm-logs/`` — enumerate VM run directories.
  2. ``gsutil ls -l gs://{code_bucket}/vm-logs/*/EXIT_STATUS`` — get EXIT_STATUS
     modification times (used as end-time proxy).

Start time is parsed from the VM name's run-ts suffix (YYYYMMDD-HHMMSS).
Duration = EXIT_STATUS mtime - run-ts. Machine type is inferred from the VM
name prefix using a static mapping derived from the launcher defaults.

Usage:
    python3 analyze_vm_costs.py [--project PROJECT] [--days N] [--output PATH]

Examples:
    python3 analyze_vm_costs.py --days 30                      # last 30 days, stdout
    python3 analyze_vm_costs.py --days 7 --output /tmp/out.csv # last 7 days, to file

Output columns:
    date, week, vm_name, vm_prefix, asset_group, machine_type,
    duration_min, cost_usd, exit_code
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast


class VmRow(TypedDict):
    date: str
    week: str
    vm_name: str
    asset_group: str
    machine_type: str
    hourly_rate_usd: str
    duration_min: str
    cost_usd: str
    exit_code: str


# ---------------------------------------------------------------------------
# GCE on-demand pricing — asia-northeast1 (Tokyo) region, 2026-05 rates
# Source: https://cloud.google.com/compute/vm-instance-pricing
# ---------------------------------------------------------------------------
_MACHINE_HOURLY_USD: dict[str, float] = {
    "e2-micro": 0.0084,
    "e2-small": 0.0168,
    "e2-medium": 0.0335,
    "e2-standard-2": 0.0670,
    "e2-standard-4": 0.1340,
    "e2-standard-8": 0.2680,
    "e2-standard-16": 0.5361,
    "e2-highmem-2": 0.0905,
    "e2-highmem-4": 0.1811,
    "e2-highmem-8": 0.3621,
    "n2-standard-2": 0.0949,
    "n2-standard-4": 0.1898,
    "n2-standard-8": 0.3795,
    "n2-standard-16": 0.7590,
    "n2-highmem-4": 0.2469,
    "n2-highmem-8": 0.4937,
    "c3-highcpu-4": 0.1780,
    "c3-highcpu-8": 0.3560,
    "c3-highcpu-176": 7.8320,
}

# ---------------------------------------------------------------------------
# VM prefix → (machine_type, asset_group)
# Derived from deployment-service/scripts/vm/launch-*.sh defaults.
# ---------------------------------------------------------------------------
_PREFIX_TO_META: dict[str, tuple[str, str]] = {
    "af-": ("e2-standard-4", "sports"),
    "af-audit-": ("e2-standard-4", "sports"),
    "af-backfill-": ("e2-standard-4", "sports"),
    "amm-golden-": ("n2-standard-2", "defi"),
    "aave-lending-": ("n2-standard-2", "defi"),
    "batch-live-recon-": ("n2-standard-2", "cefi"),
    "canonical-smoke-": ("e2-standard-2", "cefi"),
    "cefi-instruments-": ("e2-standard-4", "cefi"),
    "cefi-migration-": ("e2-standard-4", "cefi"),
    "cefi-massive-": ("e2-standard-4", "cefi"),
    "cefi-fwd-": ("e2-standard-4", "cefi"),
    "cefi-backfill-": ("e2-standard-4", "cefi"),
    "defi-fwd-": ("e2-standard-2", "defi"),
    "defi-phantom-recon-": ("e2-standard-2", "defi"),
    "features-backfill-": ("e2-standard-4", "cefi"),
    "features-fwd-": ("e2-standard-4", "cefi"),
    "features-onchain-": ("e2-standard-4", "defi"),
    "features-sports-": ("e2-standard-4", "sports"),
    "features-tradfi-": ("e2-standard-4", "tradfi"),
    "footystats-fwd-": ("e2-standard-2", "prediction"),
    "honest-coverage-": ("e2-standard-2", "cefi"),
    "measure-honest-coverage-": ("e2-standard-2", "cefi"),
    "instruments-smoke-": ("e2-standard-2", "cefi"),
    "instruments-backfill-": ("e2-standard-4", "cefi"),
    "instruments-fwd-": ("e2-standard-4", "cefi"),
    "lending-rate-validation-": ("n2-standard-2", "defi"),
    "manifest-recon-": ("e2-standard-2", "cefi"),
    "ml-train-": ("c3-highcpu-176", "cefi"),
    "mtds-backfill-": ("n2-standard-4", "cefi"),
    "mtds-fwd-": ("n2-standard-4", "cefi"),
    "mtds-pyth-": ("n2-standard-2", "defi"),
    "prediction-fwd-": ("e2-standard-2", "prediction"),
    "qg-snapshot-": ("e2-standard-2", "cefi"),
    "sfi-fwd-": ("e2-standard-2", "sports"),
    "sports-manifest-rescan-": ("e2-standard-2", "sports"),
    "sports-scheduler-": ("e2-standard-2", "sports"),
    "strategy-test-": ("n2-standard-4", "cefi"),
    "tradfi-backfill-": ("n2-standard-4", "tradfi"),
    "tradfi-fwd-": ("n2-standard-4", "tradfi"),
    "vm-zombie-watchdog-": ("e2-standard-2", "cefi"),
    "weather-backfill-": ("e2-standard-2", "cefi"),
    # Additional prefixes observed in vm-logs
    "mtds-lending-indices-": ("n2-standard-4", "defi"),
    "mtds-lst-rates-": ("n2-standard-2", "defi"),
    "cross-asset-rescan-": ("e2-standard-4", "cefi"),
    "manifest-consolidator-": ("e2-standard-2", "cefi"),
    "phantom-apply-": ("e2-standard-2", "cefi"),
    "migrate-": ("e2-standard-4", "cefi"),
    "mdps-backfill-": ("e2-standard-4", "cefi"),
    "gcs-migration-": ("e2-standard-4", "cefi"),
    "strategy-backtest-": ("e2-standard-4", "cefi"),
    "strategy-paper-": ("e2-standard-4", "cefi"),
    "strategy-live-": ("e2-standard-4", "cefi"),
    "synbench-": ("e2-standard-4", "cefi"),
}
_DEFAULT_MACHINE = "e2-standard-4"
_DEFAULT_ASSET_GROUP = "unknown"

# Pattern: YYYYMMDD-HHMMSS anywhere in VM name (usually suffix)
_RUN_TS_RE = re.compile(r"(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})")
# gsutil ls -l output: "<size>  <datetime>  gs://..." e.g. "   1234  2026-05-15T10:20:30Z  gs://..."
_LS_L_RE = re.compile(r"^\s*\d+\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(gs://.+)$")


def _resolve_prefix_meta(vm_name: str) -> tuple[str, str, str]:
    """Return (machine_type, asset_group, matched_prefix)."""
    for prefix, (mtype, ag) in sorted(_PREFIX_TO_META.items(), key=lambda x: -len(x[0])):
        if vm_name.startswith(prefix):
            return mtype, ag, prefix
    return _DEFAULT_MACHINE, _DEFAULT_ASSET_GROUP, ""


def _parse_run_ts(vm_name: str) -> datetime | None:
    """Extract start datetime from VM name's YYYYMMDD-HHMMSS suffix."""
    m = _RUN_TS_RE.search(vm_name)
    if not m:
        return None
    try:
        return datetime(
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
            int(m.group(4)),
            int(m.group(5)),
            int(m.group(6)),
            tzinfo=UTC,
        )
    except ValueError:
        return None


def _iso_week(dt: datetime) -> str:
    return dt.strftime("%Y-W%V")


def _gsutil_ls(pattern: str) -> list[str]:
    """Run gsutil ls and return non-empty lines."""
    result = subprocess.run(["gsutil", "ls", pattern], capture_output=True, text=True, check=False)
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _gsutil_ls_l(pattern: str) -> list[tuple[datetime, str]]:
    """Run gsutil ls -l and parse (mtime, gcs_path) pairs."""
    result = subprocess.run(["gsutil", "ls", "-l", pattern], capture_output=True, text=True, check=False)
    out: list[tuple[datetime, str]] = []
    for line in result.stdout.splitlines():
        m = _LS_L_RE.match(line)
        if m:
            try:
                mtime = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
                out.append((mtime, m.group(2)))
            except ValueError:
                pass
    return out


def analyze(project: str, days: int, output_path: str | None) -> None:
    code_bucket = f"deployment-scripts-{project}"
    print(f"[analyze_vm_costs] project={project} bucket={code_bucket} days={days}", file=sys.stderr)

    # 1. List all vm-log directories
    all_dirs = _gsutil_ls(f"gs://{code_bucket}/vm-logs/")
    prefix = f"gs://{code_bucket}/vm-logs/"
    all_vm_names = [
        d.removeprefix(prefix).rstrip("/")
        for d in all_dirs
        if d.startswith(prefix) and "/" not in d.removeprefix(prefix).rstrip("/")
    ]

    # 2. Filter by date
    cutoff: datetime | None = None
    if days > 0:
        cutoff = datetime.now(UTC) - timedelta(days=days)

    vm_names: list[str] = []
    for name in all_vm_names:
        if not name:
            continue
        ts = _parse_run_ts(name)
        if ts is None:
            continue  # skip entries with no parseable run-ts
        if cutoff and ts < cutoff:
            continue
        vm_names.append(name)

    print(
        f"[analyze_vm_costs] {len(vm_names)} VM runs in window (total dirs: {len(all_vm_names)})",
        file=sys.stderr,
    )

    if not vm_names:
        print("[analyze_vm_costs] No VM runs found.", file=sys.stderr)
        return

    # 3. Get end-time proxies: prefer EXIT_STATUS mtime, fall back to run.log mtime.
    # Both use single batch gsutil ls -l calls (no per-VM round trips).
    print("[analyze_vm_costs] Fetching EXIT_STATUS + run.log timestamps...", file=sys.stderr)
    end_mtimes: dict[str, datetime] = {}
    for filename in ("EXIT_STATUS", "run.log"):
        for mtime, gcs_path in _gsutil_ls_l(f"gs://{code_bucket}/vm-logs/**/{filename}"):
            inner = gcs_path.removeprefix(f"gs://{code_bucket}/vm-logs/")
            parts = inner.rstrip("/").split("/")
            if len(parts) == 2 and parts[1] == filename:
                vm = parts[0]
                # EXIT_STATUS takes priority (already set = skip run.log)
                if filename == "EXIT_STATUS" or vm not in end_mtimes:
                    end_mtimes[vm] = mtime

    print(f"[analyze_vm_costs] end-time timestamps found for {len(end_mtimes)} VMs", file=sys.stderr)

    # 4. Compute rows
    rows: list[VmRow] = []
    for vm_name in sorted(vm_names):
        machine_type, asset_group, _ = _resolve_prefix_meta(vm_name)
        hourly = _MACHINE_HOURLY_USD.get(machine_type, _MACHINE_HOURLY_USD[_DEFAULT_MACHINE])

        start_ts = _parse_run_ts(vm_name)
        end_ts = end_mtimes.get(vm_name)
        exit_code = "unknown"

        if start_ts and end_ts and end_ts > start_ts:
            duration_min = (end_ts - start_ts).total_seconds() / 60.0
            cost_usd = (duration_min / 60.0) * hourly
            date_str = start_ts.strftime("%Y-%m-%d")
            week_str = _iso_week(start_ts)
        elif start_ts:
            date_str = start_ts.strftime("%Y-%m-%d")
            week_str = _iso_week(start_ts)
            duration_min = 0.0
            cost_usd = 0.0
        else:
            date_str = "unknown"
            week_str = "unknown"
            duration_min = 0.0
            cost_usd = 0.0

        rows.append(
            {
                "date": date_str,
                "week": week_str,
                "vm_name": vm_name,
                "asset_group": asset_group,
                "machine_type": machine_type,
                "hourly_rate_usd": f"{hourly:.4f}",
                "duration_min": f"{duration_min:.1f}",
                "cost_usd": f"{cost_usd:.4f}",
                "exit_code": exit_code,
            }
        )

    fieldnames = [
        "date",
        "week",
        "vm_name",
        "asset_group",
        "machine_type",
        "hourly_rate_usd",
        "duration_min",
        "cost_usd",
        "exit_code",
    ]

    out: io.TextIOWrapper | None = None
    try:
        if output_path:
            out = open(output_path, "w", newline="")  # noqa: SIM115
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            print(f"[analyze_vm_costs] CSV written to {output_path}", file=sys.stderr)
        else:
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    finally:
        if out:
            out.close()

    # Summary
    total_cost = sum(float(r["cost_usd"]) for r in rows)
    total_min = sum(float(r["duration_min"]) for r in rows)
    print(f"\n{'=' * 60}", file=sys.stderr)
    print(
        f"SUMMARY: {len(rows)} VM runs | {total_min / 60:.1f} VM-hours | ${total_cost:.2f} total",
        file=sys.stderr,
    )
    print(f"{'=' * 60}", file=sys.stderr)

    by_type: dict[str, tuple[int, float, float]] = {}
    for r in rows:
        mt = r["machine_type"]
        cnt, mins, cost = by_type.get(mt, (0, 0.0, 0.0))
        by_type[mt] = (cnt + 1, mins + float(r["duration_min"]), cost + float(r["cost_usd"]))
    print("\nBy machine_type:", file=sys.stderr)
    for mt, (cnt, mins, cost) in sorted(by_type.items(), key=lambda x: -x[1][2]):
        print(f"  {mt:<22} {cnt:>4} runs  {mins / 60:>7.1f} hrs  ${cost:>8.2f}", file=sys.stderr)

    by_ag: dict[str, tuple[int, float, float]] = {}
    for r in rows:
        ag = r["asset_group"]
        cnt, mins, cost = by_ag.get(ag, (0, 0.0, 0.0))
        by_ag[ag] = (cnt + 1, mins + float(r["duration_min"]), cost + float(r["cost_usd"]))
    print("\nBy asset_group:", file=sys.stderr)
    for ag, (cnt, mins, cost) in sorted(by_ag.items(), key=lambda x: -x[1][2]):
        print(f"  {ag:<12} {cnt:>4} runs  {mins / 60:>7.1f} hrs  ${cost:>8.2f}", file=sys.stderr)

    by_week: dict[str, tuple[int, float]] = {}
    for r in rows:
        wk = r["week"]
        cnt, cost = by_week.get(wk, (0, 0.0))
        by_week[wk] = (cnt + 1, cost + float(r["cost_usd"]))
    print("\nBy week:", file=sys.stderr)
    for wk, (cnt, cost) in sorted(by_week.items()):
        print(f"  {wk}  {cnt:>4} runs  ${cost:>8.2f}", file=sys.stderr)
    print(file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", default="central-element-323112")
    parser.add_argument("--days", type=int, default=30, help="Past days to analyze (0=all)")
    parser.add_argument("--output", help="CSV output path (default: stdout)")
    ns = parser.parse_args()
    project: str = cast(str, ns.project)
    days: int = cast(int, ns.days)
    output_path: str | None = cast("str | None", ns.output)
    analyze(project=project, days=days, output_path=output_path)


if __name__ == "__main__":
    main()
