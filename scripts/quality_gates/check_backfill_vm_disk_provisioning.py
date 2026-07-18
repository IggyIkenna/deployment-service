#!/usr/bin/env python3
"""Fail if a download-heavy backfill VM launcher provisions an inadequate boot disk.

Epic: cefi backfill throughput
Lifecycle: PERMANENT — guards the 2026-07-18 root cause from silently regressing.

WHY (measured 2026-07-18 on cefi-queue-heavy-binancefutu-x17-20260718-165536):
a ``pd-standard`` 50GB boot disk sustains only ~6 MB/s of writes, and its burst
credits deplete as a function of CUMULATIVE BYTES WRITTEN. The CeFi backfill
therefore ran at ~12 MB/s for its first ~7.5GB and then collapsed to ~2.4 MB/s
permanently. ``iostat`` on the degraded VM: ``%util=99.94``, ``w_await=1015ms``,
``aqu-sz=51``, CPU 93.5% idle / 6.2% iowait, RAM 115GB free of 128, disk 11% full
— unambiguous disk starvation, which was misdiagnosed for hours as a Tardis
account/session/IP quota (there were ZERO HTTP 429s the whole time).

Tardis serves ``.csv.gz``, so download RX is ~5x amplified by the time it is
decompressed and written as parquet: ~2.4 MB/s of RX is ~12 MB/s of writes. GCP
persistent-disk throughput scales linearly with size, so the fix is a bigger,
faster disk: 250GB ``pd-balanced`` = ~70 MB/s of writes, enough to absorb ~14 MB/s
of RX with no cliff, for ~$25/month prorated.

VALIDATED 2026-07-18 on cefi-queue-heavy-binancefutu-x17-20260718-184320: the same
workload on ``pd-balanced`` 250GB sustained **11.1 MB/s to 18.7GB+** with peaks of
18.15 MB/s and no cliff, versus 2.36 MB/s post-cliff on ``pd-standard`` — a 4.7x gain.

SCOPE — a launcher is gated if it is download-heavy by any of:
  * Tardis consumer (``VM_TARDIS_CONSUMER`` / ``tardis_concurrency_guard``)
  * Databento consumer (the TradFi vendor path)
  * a ``*backfill*`` launcher (bulk historical pulls: mtds/defi/sports/features/...)
  * a ``*forward-poll*`` launcher (continuous vendor polling)
Such a launcher must provision BOTH an explicit non-``pd-standard`` disk type AND at
least MIN_DISK_GB. Omitting ``--boot-disk-type`` entirely is a failure: GCP silently
defaults to ``pd-standard``, which is exactly how this regressed across ~60 launchers.

Genuinely light VMs may opt out with a ``qg-disk-exempt:`` comment carrying a reason.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Required sustained WRITE throughput for a download-heavy VM, in MB/s. Derived from the
# measured cefi workload: 11.1 MB/s of compressed RX becomes ~67 MB/s of parquet writes
# (iostat on the validated pd-balanced VM: wkB/s=67199 at %util 14.7).
TARGET_WRITE_MBPS = 70
# GCP PD sustained write throughput per provisioned GB. Size is what buys throughput, so the
# minimum size is TYPE-AWARE: pd-ssd is ~1.7x faster per GB, so it needs fewer GB to clear the
# same bar. Forcing 250GB of pd-ssd on a 24/7 strategy VM would be pure cost for no gain.
MBPS_PER_GB = {"pd-balanced": 0.28, "pd-ssd": 0.48, "pd-extreme": 0.48}
MIN_DISK_GB = 250  # pd-balanced default (70 / 0.28 = 250)


def _min_gb_for(disk_type: str) -> int:
    rate = MBPS_PER_GB.get(disk_type)
    if rate is None:
        return MIN_DISK_GB
    return int(-(-TARGET_WRITE_MBPS // rate) // 10 * 10)  # round up to 10GB


SLOW_TYPES = {"pd-standard"}
VENDOR_MARKERS = ("VM_TARDIS_CONSUMER", "tardis_concurrency_guard", "databento", "DATABENTO")
NAME_MARKERS = ("backfill", "forward-poll")

# VM_TASK is the DURABLE signal — the filename is not. The 2026-07-18 sweep initially gated
# on filename alone and missed launch-cefi-massive-rollout.sh (VM_TASK=cefi-backfill, the same
# Tardis workload that started this investigation), launch-features-vm.sh (features-backfill),
# launch-mtds-gas-fees-fleet-vm.sh (defi-backfill) and four sports launchers
# (instruments-backfill). Measured sustained write means over 14 days back this up:
# expected-universe-v2 12.24 MB/s, features-sports 11.61, mdps-sports-bucket 5.77,
# sports-v9-migration 5.1-8.6 — all on pd-standard, all doing bulk data work.
TASK_MARKERS = (
    "backfill",
    "migration",
    "-live",
    "recon",
    "rsync",
    "rescan",
    "sweep",
    "seed",
    "cutover",
    "training",
    "replay",
    "coverage",
    "converge",
    "gap-fill",
    "features",
    "universe",
    "stamp",
    "canon",
)


def _vm_task(text: str) -> str:
    m = re.search(r"VM_TASK=([a-z0-9-]+)", text)
    return m.group(1) if m else ""


EXEMPT_MARKER = "qg-disk-exempt:"

# AWS launchers use EBS flags, not GCP persistent disks; they are out of scope here
# and are guarded by their own provisioning path.
SKIP_SUFFIXES = ("-aws.sh",)

_SIZE_RE = re.compile(r"--boot-disk-size=[\"']?(?:\$\{BOOT_DISK_SIZE:-)?(\d+)GB")
# Indirect form: --boot-disk-size="${BOOT_DISK_GB}GB" — resolve the variable's own
# assignment (either VAR="${VAR:-250}" or a plain VAR="250") from the same file.
_SIZE_VAR_RE = re.compile(r"--boot-disk-size=[\"']?\$\{([A-Z_]+)(?::-(\d+)(?:GB)?)?\}")
# Assignments appear quoted, defaulted, or bare: VAR="${VAR:-250}" / VAR="250" / VAR=250
_VAR_ASSIGN_RE = r'^{var}=(?:"(?:\$\{{{var}:-)?(\d+)(?:GB)?\}}?"|(\d+))'


def _resolve_size_gb(text: str) -> int | None:
    """Smallest boot-disk size the launcher can produce, in GB, or None if unparsable."""
    direct = _SIZE_RE.search(text)
    if direct is not None:
        return int(direct.group(1))
    indirect = _SIZE_VAR_RE.search(text)
    if indirect is None:
        return None
    var, inline_default = indirect.group(1), indirect.group(2)
    if inline_default is not None:
        return int(inline_default)
    assign = re.search(_VAR_ASSIGN_RE.format(var=var), text, re.M)
    if assign is None:
        return None
    return int(assign.group(1) or assign.group(2))


_TYPE_RE = re.compile(r"--boot-disk-type=[\"']?(?:\$\{BOOT_DISK_TYPE:-)?([a-z0-9-]+)")


def _iter_launchers(vm_dir: Path):
    for path in sorted(vm_dir.glob("launch-*.sh")):
        if path.name.endswith(SKIP_SUFFIXES):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if EXEMPT_MARKER in text:
            continue
        task = _vm_task(text)
        gated = (
            any(m in text for m in VENDOR_MARKERS)
            or any(m in path.name for m in NAME_MARKERS)
            or any(m in task for m in TASK_MARKERS)
        )
        if gated:
            yield path, text


def main() -> int:
    vm_dir = Path(__file__).resolve().parents[2] / "scripts" / "vm"
    if not vm_dir.is_dir():
        print(f"❌ vm launcher dir not found: {vm_dir}")
        return 1

    failures: list[str] = []
    checked = 0

    for path, text in _iter_launchers(vm_dir):
        # A launcher that never creates an instance (a guard/library) has nothing
        # to provision.
        if "--boot-disk-size" not in text:
            continue
        checked += 1
        rel = path.relative_to(vm_dir.parents[1])

        type_match = _TYPE_RE.search(text)
        if type_match is None:
            failures.append(
                f"{rel}: download-heavy VM with NO --boot-disk-type — GCP defaults to "
                f"pd-standard (~6 MB/s writes at 50GB), which caused the 2026-07-18 "
                f"throughput collapse. Set --boot-disk-type=pd-balanced."
            )
        elif type_match.group(1) in SLOW_TYPES:
            failures.append(
                f"{rel}: download-heavy VM on {type_match.group(1)} — too slow for a "
                f"download-heavy VM. Use pd-balanced or pd-ssd."
            )

        disk_type = type_match.group(1) if type_match else "pd-balanced"
        min_gb = _min_gb_for(disk_type)
        size_gb = _resolve_size_gb(text)
        if size_gb is None:
            failures.append(f"{rel}: could not parse --boot-disk-size")
        elif size_gb < min_gb:
            failures.append(
                f"{rel}: boot disk {size_gb}GB {disk_type} < {min_gb}GB minimum — GCP PD "
                f"throughput scales with SIZE ({MBPS_PER_GB.get(disk_type, 0.28)} MB/s per GB), "
                f"so {size_gb}GB sustains only "
                f"~{size_gb * MBPS_PER_GB.get(disk_type, 0.28):.0f} MB/s of writes vs the "
                f"{TARGET_WRITE_MBPS} MB/s this workload class needs."
            )

    if failures:
        print("❌ Backfill VM disk provisioning check FAILED\n")
        for f in failures:
            print(f"  • {f}")
        print(
            "\n  Root cause doc: plans/active/issues/backfill_vm_disk_starvation_misdiagnosed_as_tardis_quota_2026_07_18.md"
        )
        return 1

    print(
        f"✅ Backfill VM disk provisioning: {checked} download-heavy launcher(s) "
        f"on non-pd-standard disks sized for >={TARGET_WRITE_MBPS} MB/s of writes"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
