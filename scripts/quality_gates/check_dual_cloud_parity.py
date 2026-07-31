#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Dual-cloud build parity check.

For every repo whose cloudbuild.yaml performs a real Docker image build (detected
by the presence of 'docker push' or '_AR_HOST' or '_IMAGE_URI'), asserts that a
corresponding buildspec.aws.yaml also exists.

Repos with cloudbuild.yaml that only run lint/smoke tests (no-op push) are
excluded from the parity requirement by design.

SSOT: codex/05-infrastructure/dual-cloud-image-builds.md
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_REAL_BUILD_MARKERS = ("docker push", "_AR_HOST", "_IMAGE_URI", "docker build")


def _is_real_image_build(cloudbuild_path: Path) -> bool:
    text = cloudbuild_path.read_text(encoding="utf-8", errors="replace")
    return any(marker in text for marker in _REAL_BUILD_MARKERS)


def main() -> int:
    workspace_root = Path(
        os.environ.get(
            "WORKSPACE_ROOT",
            str(Path(__file__).parent.parent.parent.parent),
        )
    )

    missing: list[str] = []
    checked: list[str] = []

    for repo_dir in sorted(workspace_root.iterdir()):
        if not repo_dir.is_dir():
            continue
        cloudbuild = repo_dir / "cloudbuild.yaml"
        if not cloudbuild.exists():
            continue
        if not _is_real_image_build(cloudbuild):
            continue
        checked.append(repo_dir.name)
        buildspec_aws = repo_dir / "buildspec.aws.yaml"
        if not buildspec_aws.exists():
            missing.append(repo_dir.name)

    print(f"Dual-cloud parity: checked {len(checked)} image-building repo(s).")
    if missing:
        print("FAIL — missing buildspec.aws.yaml in the following repos:")
        for repo in missing:
            print(f"  {repo}")
        print("Add a buildspec.aws.yaml to each repo. Template: deployment-service/templates/buildspec.aws.yaml")
        return 1

    print(f"All {len(checked)} repos have both cloudbuild.yaml and buildspec.aws.yaml. ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())
