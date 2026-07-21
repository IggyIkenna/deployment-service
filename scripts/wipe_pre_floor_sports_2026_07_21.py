"""Wipe pre-2020-06-06 (pre-floor) sports data from a GCS bucket subtree.

# Epic: sports_master_closeout_2026_07_21 (2020-06 data floor + pre-floor wipe)
# Lifecycle: one-off remediation executor — DELETE after the pre-floor wipe is verified.
# Delete-when: zero day=<D> partitions with D < 2020-06-06 remain in the sports subtrees.

OPERATOR RULING 2026-07-21 (authoritative): sports odds tick data starts 2020-06-06; there
is ZERO legitimate sports data before it, so everything pre-floor is fabrication-by-
construction and is WIPED from GCS + manifest ("data before this can just be wiped out from
gcs and manifest to keep things honest"). This deletes the pre-floor OBJECTS; the manifest
prune is a separate rebuild pass.

WHY path-based day parsing (a measured trap): blob.time_created is None via the UTL storage
client's list_blobs — so creation-time is unusable here. The Hive partition segment
`day=YYYY-MM-DD` in the OBJECT PATH is the authoritative, correct day. We parse THAT and only
that; a path without a parseable day< floor is NEVER deleted.

SAFETY:
  * day-dir enumeration is a sanctioned DELIMITER listing (gcloud storage ls, read-only) —
    NOT a whole-corpus walk.
  * strict cutoff: an object is delete-eligible ONLY if its path's day=<D> parses AND
    D < FLOOR. Anything that does not parse to a pre-floor day is LEFT UNTOUCHED.
  * --census writes the full pre-floor object-name snapshot first (recovery record); on
    features-sports-prd GCS soft-delete (7d) is the primary net, on instruments-store-sports
    -prd (soft-delete=0) the snapshot is the only record so census is MANDATORY before apply.
  * deletes go through the UTL gcs_delete_object helper (no subprocess), 32-worker pool.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

from unified_trading_library import gcs_delete_object, get_storage_client

FLOOR = dt.date(2020, 6, 6)
_DAY_RE = re.compile(r"/day=(\d{4}-\d{2}-\d{2})/")
_DAY_DIR_RE = re.compile(r"day=(\d{4}-\d{2}-\d{2})/?$")


def parse_day(path: str) -> dt.date | None:
    m = _DAY_RE.search(path if path.endswith("/") else path + "/")
    if not m:
        return None
    try:
        return dt.date.fromisoformat(m.group(1))
    except ValueError:
        return None


def list_day_dirs(bucket: str, root_prefix: str) -> list[tuple[dt.date, str]]:
    """Delimiter listing of day= dirs directly under root_prefix (read-only, sanctioned)."""
    uri = f"gs://{bucket}/{root_prefix.rstrip('/')}/"
    proc = subprocess.run(
        ["gcloud", "storage", "ls", uri],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(f"!!! day-dir listing failed for {uri}: {proc.stderr.strip()[:300]}", flush=True)
        return []
    out: list[tuple[dt.date, str]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        m = _DAY_DIR_RE.search(line)
        if not m:
            continue
        try:
            d = dt.date.fromisoformat(m.group(1))
        except ValueError:
            continue
        # strip the gs://bucket/ prefix to get the object prefix
        obj_prefix = line.split(f"gs://{bucket}/", 1)[-1]
        out.append((d, obj_prefix))
    return out


def collect_pre_floor_objects(bucket: str, pre_floor_prefixes: list[str]) -> list[str]:
    client = get_storage_client()
    names: list[str] = []
    for i, prefix in enumerate(pre_floor_prefixes, 1):
        for b in client.list_blobs(bucket, prefix=prefix):
            name = str(b.name)
            d = parse_day(name)
            if d is not None and d < FLOOR:  # re-assert the cutoff per object
                names.append(name)
        if i % 200 == 0:
            print(
                f"  listed {i}/{len(pre_floor_prefixes)} pre-floor day dirs, {len(names):,} objects so far", flush=True
            )
    return names


def do_delete(bucket: str, names: list[str], workers: int) -> dict[str, int]:
    verdicts: dict[str, int] = {"DELETED": 0, "ERROR": 0}

    def _one(name: str) -> str:
        # triple-check the cutoff at the point of deletion — never delete a non-pre-floor path
        d = parse_day(name)
        if d is None or d >= FLOOR:
            return "SKIP_NOT_PRE_FLOOR"
        try:
            gcs_delete_object(f"gs://{bucket}/{name}")
            return "DELETED"
        except Exception as exc:
            return f"ERROR:{str(exc)[:80]}"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, v in enumerate(ex.map(_one, names), 1):
            key = v.split(":")[0]
            verdicts[key] = verdicts.get(key, 0) + 1
            if i % 5000 == 0:
                print(f"  {i:,}/{len(names):,} {verdicts}", flush=True)
    return verdicts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--root-prefix", required=True, help="e.g. sports_features/by_date")
    ap.add_argument("--snapshot", required=True, help="json path for the pre-floor object-name snapshot")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: census only)")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    print(
        f"=== pre-floor wipe {'APPLY' if args.apply else 'CENSUS'} :: gs://{args.bucket}/{args.root_prefix} floor={FLOOR} ===",
        flush=True,
    )
    day_dirs = list_day_dirs(args.bucket, args.root_prefix)
    if not day_dirs:
        print(">>> no day= dirs found under root prefix — nothing to do", flush=True)
        return 0
    days_sorted = sorted(d for d, _ in day_dirs)
    pre = [(d, p) for d, p in day_dirs if d < FLOOR]
    post = [(d, p) for d, p in day_dirs if d >= FLOOR]
    print(f"  day range: {days_sorted[0]} .. {days_sorted[-1]}  ({len(day_dirs):,} day dirs)", flush=True)
    print(f"  PRE-floor day dirs (<{FLOOR}): {len(pre):,}   POST-floor: {len(post):,}", flush=True)
    if pre:
        print(f"  pre-floor span: {min(d for d, _ in pre)} .. {max(d for d, _ in pre)}", flush=True)

    names = collect_pre_floor_objects(args.bucket, [p for _, p in pre])
    print(f"\n  >>> pre-floor OBJECTS to wipe: {len(names):,}", flush=True)

    # snapshot always (recovery record — the ONLY record where soft-delete=0)
    with open(args.snapshot, "w", encoding="utf-8") as f:
        json.dump(
            {
                "bucket": args.bucket,
                "root_prefix": args.root_prefix,
                "floor": FLOOR.isoformat(),
                "pre_floor_object_count": len(names),
                "pre_floor_objects": names,
            },
            f,
        )
    print(f"  snapshot written: {args.snapshot} ({len(names):,} paths)", flush=True)

    if not args.apply:
        print("\n(CENSUS only — nothing deleted. Re-run with --apply to delete.)", flush=True)
        return 0

    print("\n  DELETING…", flush=True)
    verdicts = do_delete(args.bucket, names, args.workers)
    print(f"\n=== WIPE RESULT: {verdicts} ===", flush=True)
    return 0 if verdicts.get("ERROR", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
