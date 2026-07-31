#!/usr/bin/env python3
# Epic: cefi backfill throughput
# Lifecycle: permanent
# Delete-when: NA
"""Fail if a shell comment sits INSIDE a backslash-continued command.

Epic: cefi backfill throughput
Lifecycle: PERMANENT — closes the gate gap that let a P0 reach live infra on 2026-07-18.

WHY THIS EXISTS. A comment line between two `\\`-continued lines SILENTLY TRUNCATES the
command::

    gcloud compute instances create "$vm" \\
        --image-project=ubuntu-os-cloud \\
        # this comment ends the command HERE
        --boot-disk-size=250GB \\
        --metadata="startup-script-url=..."

gcloud then runs with no --metadata, no --boot-disk and no --labels — booting a
metadata-less VM with no startup script, i.e. an idle machine that silently does no
work — and afterwards errors on the stray `--boot-disk-size` flag.

**Neither `bash -n` nor shellcheck flags this.** Both consider it valid shell, because it
is: it is simply a different command than the author intended. On 2026-07-18 the
pd-balanced disk-policy sweep inserted rationale comments in exactly this position across
11 launchers; `bash -n` passed on every one of them, and three metadata-less VMs booted
idle before the breakage was caught by a human running an unrelated measurement.

The rule this enforces: put the comment ABOVE the command, never inside it.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCAN_DIRS = ("scripts/vm", "scripts/cicd", "scripts/recovery", "scripts/dev")


def _breakages(path: Path) -> list[tuple[int, str]]:
    """Return (line_no, text) for each comment sitting inside a continued command."""
    found: list[tuple[int, str]] = []
    continued = False
    for idx, line in enumerate(path.read_text(encoding="utf-8", errors="replace").split("\n"), 1):
        stripped = line.strip()
        if continued and stripped.startswith("#"):
            found.append((idx, stripped[:80]))
        # A COMMENT line ending in a backslash does not continue a command — a block of
        # commented-out example usage is harmless and must not be flagged.
        continued = line.rstrip().endswith("\\") and not stripped.startswith("#")
    return found


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    failures: list[str] = []
    scanned = 0

    for rel in SCAN_DIRS:
        directory = root / rel
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*.sh")):
            scanned += 1
            for line_no, text in _breakages(path):
                failures.append(f"{path.relative_to(root)}:{line_no}: {text}")

    if failures:
        print("❌ comment inside a backslash-continued command (SILENTLY truncates it)\n")
        for f in failures:
            print(f"  • {f}")
        print(
            "\n  A comment between two `\\`-continued lines ends the command there. gcloud then\n"
            "  runs without the remaining flags (no --metadata => a VM with no startup script).\n"
            "  bash -n and shellcheck do NOT catch this. Move the comment ABOVE the command.\n"
            "  Root cause doc: plans/active/issues/"
            "launcher_gcloud_continuation_broken_by_disk_sweep_2026_07_18.md"
        )
        return 1

    print(f"✅ line-continuation check: {scanned} shell script(s), no comments inside continued commands")
    return 0


if __name__ == "__main__":
    sys.exit(main())
