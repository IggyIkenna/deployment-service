"""Load + validate dependency_health_policies.yaml at startup.

Fails loud on any missing required field or invalid DependencyClass value —
ensures operator misconfiguration is caught before the alerting-service wires
the evaluate_dependency_health() rule.

Usage (startup hook or smoke test):
    python scripts/load_dependency_policies.py
    python scripts/load_dependency_policies.py --config-path configs/dependency_health_policies.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from pydantic import ValidationError
from unified_api_contracts.canonical.crosscutting.dependency.health_policy import (
    DependencyHealthPolicy,
)


def load_policies(config_path: Path) -> list[DependencyHealthPolicy]:
    """Parse YAML, validate every entry against DependencyHealthPolicy schema."""
    raw = yaml.safe_load(config_path.read_text())
    entries: list[dict[str, object]] = raw.get("dependencies", [])
    if not entries:
        raise ValueError(f"No 'dependencies' list found in {config_path}")

    policies: list[DependencyHealthPolicy] = []
    errors: list[str] = []
    for i, entry in enumerate(entries):
        try:
            policies.append(DependencyHealthPolicy(**entry))
        except (ValidationError, TypeError) as exc:
            dep_id = entry.get("dependency_id", f"<entry #{i}>")
            errors.append(f"  [{dep_id}]: {exc}")

    if errors:
        raise ValueError(f"dependency_health_policies.yaml has {len(errors)} invalid entr(ies):\n" + "\n".join(errors))
    return policies


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config-path",
        default=Path(__file__).parent.parent / "configs" / "dependency_health_policies.yaml",
        type=Path,
    )
    args = parser.parse_args()

    try:
        policies = load_policies(args.config_path)
    except (ValueError, FileNotFoundError) as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"OK: loaded {len(policies)} dependency health policies from {args.config_path}")
    for p in sorted(policies, key=lambda x: (x.dependency_class, x.dependency_id)):
        fa = "fallback=YES" if p.fallback_available else "fallback=NO (SEV0 on any outage)"
        print(
            f"  [{p.dependency_class}] {p.dependency_id}: expected={p.expected_recovery_time_seconds}s, hard={p.hard_escalation_seconds}s, {fa}"
        )


if __name__ == "__main__":
    main()
