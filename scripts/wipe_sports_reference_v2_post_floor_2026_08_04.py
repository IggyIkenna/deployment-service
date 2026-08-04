"""Snapshot-then-cull the dead sports_reference_v2/by_date/ dual-layout (post-floor only).

# Epic: sports_master_closeout_2026_07_21 (Track S — dead v2 layout cull)
# Lifecycle: one-off remediation executor — DELETE after verified complete (0 objects remain).
# Delete-when: confirmed 0 objects under sports_reference_v2/by_date/ in instruments-store-sports-prd.

CONTEXT: sports_reference_v2/by_date/ is a DEAD abandoned layout (frozen 2026-04-20, per
codex/02-data/sports-gcs-path-ssot.md). The pre-floor (<2020-06-06) portion was already wiped
(1,528 objects, 2026-07-21). The 16 remaining post-floor day dirs (2024-12-24..2026-04-20)
hold 64 redundant parquet files that already exist in the canonical sports_reference/by_date/
layout. This script snapshots then deletes them.

SAFETY:
  * Only deletes objects under sports_reference_v2/by_date/ with day >= 2020-06-06 (post-floor).
    Pre-floor objects are SKIPPED (already wiped, but double-check at delete time).
  * --census writes the full object-name snapshot first (recovery record — the ONLY record since
    this bucket has soft-delete=0).
  * Deletes go through gcloud storage rm (prefix-aware, object-scoped).
  * Scope is bounded to exactly 16 known day dirs — no wildcard recursive delete of the root.

BUCKET: instruments-store-sports-prd-central-element-323112
PREFIX: sports_reference_v2/by_date/
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date

FLOOR = date(2020, 6, 6)
BUCKET = "instruments-store-sports-prd-central-element-323112"
ROOT_PREFIX = "sports_reference_v2/by_date"

# The 16 post-floor day dirs confirmed by reader-check 2026-08-04
POST_FLOOR_DAYS = [
    "2024-12-24",
    "2024-12-25",
    "2025-12-25",
    "2025-12-31",
    "2026-02-19",
    "2026-02-26",
    "2026-03-18",
    "2026-03-26",
    "2026-03-30",
    "2026-03-31",
    "2026-04-02",
    "2026-04-07",
    "2026-04-09",
    "2026-04-15",
    "2026-04-16",
    "2026-04-20",
]


def _run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"!!! Command failed (rc={result.returncode}): {' '.join(cmd)}", flush=True)
        print(f"    stderr: {result.stderr[:500]}", flush=True)
    return result.stdout


def collect_objects() -> list[str]:
    """List all .parquet objects under the known post-floor day dirs."""
    names: list[str] = []
    for day in POST_FLOOR_DAYS:
        prefix = f"gs://{BUCKET}/{ROOT_PREFIX}/day={day}/"
        out = _run(["gcloud", "storage", "ls", "-r", prefix])
        for line in out.strip().split("\n"):
            line = line.strip()
            if line.endswith(".parquet") and line.startswith(f"gs://{BUCKET}/"):
                name = line[len(f"gs://{BUCKET}/") :]
                names.append(name)
    return names


def do_delete(names: list[str]) -> dict[str, int]:
    """Delete each object individually via gcloud storage rm."""
    verdicts: dict[str, int] = {"DELETED": 0, "ERROR": 0, "SKIP_PRE_FLOOR": 0}
    for i, name in enumerate(names, 1):
        # Triple-check: only delete post-floor objects
        if "/day=" in name:
            day_str = name.split("/day=")[1].split("/")[0]
            try:
                d = date.fromisoformat(day_str)
            except ValueError:
                verdicts["ERROR"] += 1
                print(f"  [{i}/{len(names)}] SKIP (unparseable day): {name}", flush=True)
                continue
            if d < FLOOR:
                verdicts["SKIP_PRE_FLOOR"] += 1
                print(f"  [{i}/{len(names)}] SKIP (pre-floor {day_str}): {name}", flush=True)
                continue

        uri = f"gs://{BUCKET}/{name}"
        out = _run(["gcloud", "storage", "rm", uri])
        if "Removing" in out or out.strip() == "":
            verdicts["DELETED"] += 1
        else:
            verdicts["ERROR"] += 1
            print(f"  [{i}/{len(names)}] ERROR deleting: {name}", flush=True)

        if i % 20 == 0:
            print(f"  {i}/{len(names)} {verdicts}", flush=True)
    return verdicts


def verify_empty() -> int:
    """Confirm 0 objects remain under the root prefix."""
    prefix = f"gs://{BUCKET}/{ROOT_PREFIX}/"
    out = _run(["gcloud", "storage", "ls", "-r", prefix])
    remaining = [line.strip() for line in out.strip().split("\n") if line.strip().endswith(".parquet")]
    return len(remaining)


def main() -> int:
    ap = argparse.ArgumentParser(description="Snapshot-then-cull the dead sports_reference_v2/by_date/ dual-layout")
    ap.add_argument("--snapshot", required=True, help="json path for the object-name snapshot")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: census+snapshot only)")
    args = ap.parse_args()

    print(f"=== sports_reference_v2 post-floor cull {'APPLY' if args.apply else 'CENSUS'} ===", flush=True)
    print(f"Bucket: {BUCKET}", flush=True)
    print(f"Prefix: {ROOT_PREFIX}/", flush=True)
    print(f"Floor:  {FLOOR}", flush=True)
    print(f"Scope:  {len(POST_FLOOR_DAYS)} known post-floor day dirs", flush=True)
    print()

    # Step 1: collect
    print("--- Step 1: Collecting objects ---", flush=True)
    names = collect_objects()
    print(f"  Found: {len(names)} parquet objects", flush=True)
    if not names:
        print("  No objects found — nothing to do.", flush=True)
        # Still verify
        remaining = verify_empty()
        print(f"  Verification: {remaining} objects under prefix", flush=True)
        return 0

    # Step 2: snapshot (always)
    print(f"\n--- Step 2: Writing snapshot to {args.snapshot} ---", flush=True)
    snapshot_data = {
        "bucket": BUCKET,
        "root_prefix": ROOT_PREFIX,
        "floor": FLOOR.isoformat(),
        "post_floor_object_count": len(names),
        "post_floor_days": POST_FLOOR_DAYS,
        "objects": names,
    }
    with open(args.snapshot, "w", encoding="utf-8") as f:
        json.dump(snapshot_data, f, indent=2)
    print(f"  Snapshot written: {len(names)} paths", flush=True)

    if not args.apply:
        print("\n(CENSUS only — nothing deleted. Re-run with --apply to delete.)", flush=True)
        return 0

    # Step 3: delete
    print(f"\n--- Step 3: Deleting {len(names)} objects ---", flush=True)
    verdicts = do_delete(names)
    print(f"\n  DELETE RESULT: {verdicts}", flush=True)

    # Step 4: verify
    print("\n--- Step 4: Post-delete verification ---", flush=True)
    remaining = verify_empty()
    print(f"  Objects remaining under prefix: {remaining}", flush=True)
    if remaining == 0:
        print("  ✅ VERIFIED: 0 objects remain — purge complete.", flush=True)
    else:
        print(f"  ❌ {remaining} objects still remain — review needed.", flush=True)
        return 1

    return 0 if verdicts.get("ERROR", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
