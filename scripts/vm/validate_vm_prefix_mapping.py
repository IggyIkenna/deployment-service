#!/usr/bin/env python3
"""Validate VM_PREFIX_TO_BUCKET dict against GCS: check bucket existence for non-None entries.

Walks VM_PREFIX_TO_BUCKET from vm_zombie_watchdog.py and for every entry
where bucket is not None, verifies the GCS bucket exists in the project.
Reports orphan prefixes (bucket in dict but bucket missing in GCS).

Usage:
    python3 validate_vm_prefix_mapping.py [--project PROJECT_ID] [--dry-run]

Output:
    - ✅ prefix → bucket: exists
    - ❌ prefix → bucket: MISSING (orphan)
    - ℹ️  prefix → None: heartbeat-only (skip)

Exit code:
    0 = all non-None buckets exist
    1 = at least one orphan bucket found
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Import the dict from the watchdog module (same directory).
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from vm_zombie_watchdog import PROJECT_ID, VM_PREFIX_TO_BUCKET  # noqa: E402


def _bucket_exists(project_id: str, bucket_name: str) -> bool:
    from google.cloud import storage  # pyright: ignore[reportMissingModuleSource]  # library has no py.typed marker

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket.exists()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    parser.add_argument("--dry-run", action="store_true", help="Print prefixes without GCS calls")
    args = parser.parse_args()

    project = args.project
    dry_run = args.dry_run

    orphans: list[tuple[str, str]] = []
    heartbeat_only: list[str] = []
    ok: list[tuple[str, str]] = []

    # Deduplicate bucket names to avoid redundant GCS calls.
    buckets_checked: dict[str, bool] = {}

    for prefix, spec in VM_PREFIX_TO_BUCKET.items():
        # spec can be: None (heartbeat-only), str (legacy raw bucket name),
        # or VmPrefixSpec (typed, with .bucket attr that may itself be None).
        if spec is None:
            heartbeat_only.append(prefix)
            continue
        if isinstance(spec, str):
            # Legacy entries: raw bucket name string directly in the dict.
            bucket_name = spec
        elif spec.bucket is None:  # pyright: ignore[reportOptionalMemberAccess]  # storage_client is narrowed to non-None by caller guard; union is Optional[StorageClient]
            heartbeat_only.append(prefix)
            continue
        else:
            bucket_name = spec.bucket  # pyright: ignore[reportOptionalMemberAccess]  # storage_client is narrowed to non-None by caller guard; union is Optional[StorageClient]

        if dry_run:
            print(f"  [DRY-RUN] would check: {prefix!r} → {bucket_name}")
            continue

        if bucket_name not in buckets_checked:
            buckets_checked[bucket_name] = _bucket_exists(project, bucket_name)

        if buckets_checked[bucket_name]:
            ok.append((prefix, bucket_name))
        else:
            orphans.append((prefix, bucket_name))

    if dry_run:
        print(f"\n{len(heartbeat_only)} heartbeat-only prefixes (no GCS check needed)")
        prefixes_with_bucket = sum(1 for s in VM_PREFIX_TO_BUCKET.values() if s is not None and s.bucket is not None)  # pyright: ignore[reportOptionalMemberAccess]  # storage_client is narrowed to non-None by caller guard; union is Optional[StorageClient]
        print(f"{prefixes_with_bucket} prefixes with bucket (would check)")
        return 0

    print(f"\n{'='*60}")
    print(f"VM_PREFIX_TO_BUCKET validation  (project={project})")
    print(f"{'='*60}")

    for prefix, bucket in sorted(ok):
        print(f"  ✅  {prefix!r:45s} → {bucket}")

    for prefix in sorted(heartbeat_only):
        print(f"  ℹ️   {prefix!r:45s} → None (heartbeat-only, skip)")

    for prefix, bucket in sorted(orphans):
        print(f"  ❌  {prefix!r:45s} → {bucket}  MISSING IN GCS")

    print(f"\nSummary: {len(ok)} OK, {len(heartbeat_only)} heartbeat-only, {len(orphans)} orphans")

    if orphans:
        print("\nOrphan buckets found — VM data may be lost for these prefixes.")
        print("Create missing buckets or update VM_PREFIX_TO_BUCKET.")
        return 1

    print("\nAll non-None buckets verified ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
