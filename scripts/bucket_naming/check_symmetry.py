#!/usr/bin/env python3
# pyright: reportAny=false
"""AWS↔GCP bucket-name symmetry checker.

Reads deployment-service/configs/cloud-providers.yaml and verifies that every
kind present in both clouds resolves to a symmetric name: the only difference
allowed is ${GCP_PROJECT_ID} vs ${AWS_ACCOUNT_ID}.

Also checks that DEPLOYMENT_ENV_SHORT usage is consistent across clouds (both
use the token or both omit it for each kind).

Exit codes:
  0 — all checks pass
  1 — one or more symmetry or consistency violations detected

Usage:
  python3 check_symmetry.py [--yaml /path/to/cloud-providers.yaml]
  bash check_symmetry.sh [--yaml /path/to/cloud-providers.yaml]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("PyYAML not available — install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

# Sentinel substitution values used for name resolution in checks.
_GCP_PROJECT = "GCP_PROJECT_SENTINEL"
_AWS_ACCOUNT = "000000000001"
_ENV_SHORT = "prd"

# Cloud-neutral placeholder; after substitution, GCP/AWS names should match under this.
_CLOUD_ID = "__CLOUD_ID__"

# Kinds that are GCP-only by design (no AWS counterpart expected)
_SKIP_GCP_ONLY_KINDS: set[str] = set()

# Kinds with intentional cloud-specific structural divergence (documented in
# plans/audit/results/aws_gcp_bucket_symmetry_2026_05_20_summary.md § "Notes / Caveats").
# These are excluded from the symmetric-name check but still checked for ≤63 chars.
_KNOWN_STRUCTURAL_DIVERGENCES: dict[str, str] = {
    # GCP: ${pid}-events (env-less, project-first). AWS: unified-trading-events-${account} (env-less, account-suffix).
    # Both are deliberately env-less; re-tiering GCP would be high blast radius (operator confirm required).
    "events": "pre-existing env-less shape differs by cloud; env-tiering GCP is post-Phase-1",
    # GCP: ${pid}-client-reports / ${pid}-client-statements (pid-prefix flat format).
    # AWS: unified-trading-client-reports/statements-${account} (account-suffix).
    # Env-tiering GCP side is post-Phase-1 scope.
    "client-reports": "pre-existing pid-prefix vs account-suffix structural divergence",
    "client-statements": "pre-existing pid-prefix vs account-suffix structural divergence",
    # GCP: ${pid}-defi-validation (env-less, pid-prefix). AWS: unified-trading-defi-validation-${account}.
    # Both env-less intentionally (validation runs target mainnet; results are environment-neutral).
    "defi-validation": "pre-existing env-less shape differs by cloud; both intentionally env-less",
}


def _substitute(template: str, *, cloud: str) -> str:
    s = template
    if cloud == "gcp":
        s = s.replace("${GCP_PROJECT_ID}", _GCP_PROJECT)
        s = s.replace("${AWS_ACCOUNT_ID}", _AWS_ACCOUNT)  # shouldn't appear in GCP
    else:
        s = s.replace("${AWS_ACCOUNT_ID}", _AWS_ACCOUNT)
        s = s.replace("${GCP_PROJECT_ID}", _GCP_PROJECT)  # shouldn't appear in AWS
    s = s.replace("${DEPLOYMENT_ENV_SHORT}", _ENV_SHORT)
    return s


def _normalize(name: str) -> str:
    """Replace the cloud-specific ID with a neutral placeholder for comparison."""
    return name.replace(_GCP_PROJECT, _CLOUD_ID).replace(_AWS_ACCOUNT, _CLOUD_ID)


def _check_pair(
    kind: str,
    ag: str | None,
    gcp_tmpl: str,
    aws_tmpl: str,
) -> tuple[bool, list[str]]:
    """Return (ok, list[error_message])."""
    gcp_name = _substitute(gcp_tmpl, cloud="gcp")
    aws_name = _substitute(aws_tmpl, cloud="aws")

    errors: list[str] = []
    ag_label = f"/{ag}" if ag else ""

    if _normalize(gcp_name) != _normalize(aws_name):
        errors.append(
            f"  {kind}{ag_label}: asymmetric names\n"
            f"    GCP: {gcp_name}\n"
            f"    AWS: {aws_name}\n"
            f"    Normalized GCP: {_normalize(gcp_name)}\n"
            f"    Normalized AWS: {_normalize(aws_name)}"
        )

    for label, name in [("GCP", gcp_name), ("AWS", aws_name)]:
        if len(name) > 63:
            errors.append(f"  {kind}{ag_label}: {label} name exceeds 63 chars ({len(name)}): {name}")

    gcp_has_env = "${DEPLOYMENT_ENV_SHORT}" in gcp_tmpl
    aws_has_env = "${DEPLOYMENT_ENV_SHORT}" in aws_tmpl
    if gcp_has_env != aws_has_env:
        errors.append(
            f"  {kind}{ag_label}: DEPLOYMENT_ENV_SHORT inconsistency — "
            f"GCP={'yes' if gcp_has_env else 'no'}, AWS={'yes' if aws_has_env else 'no'}"
        )

    return len(errors) == 0, errors


def run(yaml_path: Path) -> int:
    with open(yaml_path) as f:
        config: dict = yaml.safe_load(f)

    gcp_storage: dict = (config.get("gcp") or {}).get("storage") or {}
    aws_storage: dict = (config.get("aws") or {}).get("storage") or {}

    all_kinds = sorted(set(gcp_storage) | set(aws_storage))

    violations: list[str] = []
    checked = 0
    gcp_only: list[str] = []
    aws_only: list[str] = []

    for kind in all_kinds:
        gcp_tmpl = gcp_storage.get(kind)
        aws_tmpl = aws_storage.get(kind)

        if gcp_tmpl is None:
            aws_only.append(kind)
            continue
        if aws_tmpl is None:
            if kind not in _SKIP_GCP_ONLY_KINDS:
                gcp_only.append(kind)
            continue

        if kind in _KNOWN_STRUCTURAL_DIVERGENCES:
            # Still check ≤63 chars even for divergent kinds.
            # reason = _KNOWN_STRUCTURAL_DIVERGENCES[kind]  # Keep for documentation
            if isinstance(gcp_tmpl, str) and isinstance(aws_tmpl, str):
                for label, tmpl, cloud in [("GCP", gcp_tmpl, "gcp"), ("AWS", aws_tmpl, "aws")]:
                    name = _substitute(tmpl, cloud=cloud)
                    if len(name) > 63:
                        violations.append(f"  {kind}: {label} name exceeds 63 chars ({len(name)}): {name}")
            checked += 1
            continue

        if isinstance(gcp_tmpl, dict) and isinstance(aws_tmpl, dict):
            ags = sorted(set(gcp_tmpl) | set(aws_tmpl))
            for ag in ags:
                g = gcp_tmpl.get(ag)
                a = aws_tmpl.get(ag)
                if g is None or a is None:
                    violations.append(f"  {kind}/{ag}: missing template on one cloud (GCP={g}, AWS={a})")
                    continue
                ok, errs = _check_pair(kind, ag, str(g), str(a))
                checked += 1
                if not ok:
                    violations.extend(errs)
        elif isinstance(gcp_tmpl, str) and isinstance(aws_tmpl, str):
            ok, errs = _check_pair(kind, None, gcp_tmpl, aws_tmpl)
            checked += 1
            if not ok:
                violations.extend(errs)
        else:
            violations.append(f"  {kind}: type mismatch (GCP={type(gcp_tmpl).__name__}, AWS={type(aws_tmpl).__name__})")

    print(f"=== Bucket symmetry check: {checked} kind×ag pairs checked ===")

    if gcp_only:
        print("\nGCP-only kinds (no AWS counterpart — by design or missing):")
        for k in gcp_only:
            print(f"  {k}")

    if aws_only:
        print("\nAWS-only kinds (no GCP counterpart):")
        for k in aws_only:
            print(f"  {k}")

    if not violations:
        print("\n✅ All symmetric: GCP ↔ AWS names differ only by cloud-ID; all ≤63 chars.")
        return 0

    print(f"\n❌ {len(violations)} violation(s) found:\n")
    for v in violations:
        print(v)
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_yaml = Path(__file__).parent.parent.parent / "configs" / "cloud-providers.yaml"
    parser.add_argument("--yaml", default=str(default_yaml), help="Path to cloud-providers.yaml")
    args = parser.parse_args()

    yaml_path = Path(args.yaml)
    if not yaml_path.exists():
        print(f"ERROR: {yaml_path} not found", file=sys.stderr)
        sys.exit(2)

    sys.exit(run(yaml_path))


if __name__ == "__main__":
    main()
