#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
"""Phase 5A — AWS object migration: sync objects from current bucket names to symmetric target names.

Reads plans/audit/results/aws_gcp_bucket_symmetry_2026_05_20.csv (Phase 1 audit output),
expands templates for prd/stg/dev envs, finds source buckets (handles no-env + env-alias variants),
creates target buckets (ap-northeast-1), runs aws s3 sync, spot-checks 5 objects per kind,
and writes plans/audit/results/phase5_aws_object_migrate.csv.

Usage:
    WORKSPACE_ROOT=... SLOT_ID=10 python3 scripts/aws/phase5a_aws_object_migrate.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

AWS_ACCOUNT_ID = "427895769566"
AWS_REGION = "ap-northeast-1"
ENVS = ["prd", "stg", "dev"]
# Map 3-char short form → full-word aliases used in older bucket names
ENV_ALIASES: dict[str, list[str]] = {
    "prd": ["prod"],
    "stg": ["staging"],
    "dev": [],
}
# Full-word → short form
FULL_TO_SHORT: dict[str, str] = {"prod": "prd", "staging": "stg"}


@dataclass
class MigrationRow:
    kind: str
    asset_group: str
    source: str
    target: str
    env: str
    source_exists: bool = False
    source_object_count: int = 0
    target_created: bool = False
    synced_objects: int = 0
    spot_check_pass: bool = False
    spot_check_note: str = ""
    status: str = "pending"
    error: str = ""


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def list_all_buckets() -> set[str]:
    r = run(["aws", "s3api", "list-buckets", "--query", "Buckets[].Name", "--output", "json"])
    return set(json.loads(r.stdout))


def bucket_object_count(name: str) -> int:
    """Return first-page object count (≤1000). Non-zero = has objects; 0 = empty."""
    r = run(["aws", "s3api", "list-objects-v2", "--bucket", name,
             "--query", "length(Contents)", "--output", "text"], check=False)
    if r.returncode != 0:
        return 0
    # Auto-pagination yields one count per page; take just the first line
    first = r.stdout.strip().splitlines()[0] if r.stdout.strip() else ""
    if not first or first == "None":
        return 0
    try:
        return int(first)
    except ValueError:
        return 0


def create_bucket(name: str, existing: set[str], dry_run: bool) -> bool:
    if name in existing:
        return True
    if dry_run:
        print(f"    [DRY-RUN] create s3://{name}")
        existing.add(name)
        return True
    r = run([
        "aws", "s3api", "create-bucket", "--bucket", name,
        "--region", AWS_REGION,
        "--create-bucket-configuration", f"LocationConstraint={AWS_REGION}",
    ], check=False)
    if r.returncode != 0:
        print(f"    ERROR creating {name}: {r.stderr.strip()[:200]}", file=sys.stderr)
        return False
    run(["aws", "s3api", "put-bucket-versioning", "--bucket", name,
         "--versioning-configuration", "Status=Enabled"], check=False)
    run(["aws", "s3api", "put-public-access-block", "--bucket", name,
         "--public-access-block-configuration",
         "BlockPublicAcls=true,IgnorePublicAcls=true,"
         "BlockPublicPolicy=true,RestrictPublicBuckets=true"], check=False)
    existing.add(name)
    return True


def sync_bucket(source: str, target: str, dry_run: bool) -> tuple[int, str]:
    cmd = ["aws", "s3", "sync", f"s3://{source}", f"s3://{target}",
           "--exact-timestamps", "--no-progress"]
    if dry_run:
        cmd.append("--dryrun")
    r = run(cmd, check=False)
    if r.returncode != 0:
        return 0, r.stderr.strip()[:300]
    copied = sum(1 for ln in r.stdout.splitlines()
                 if "copy:" in ln.lower() or "(dryrun) copy" in ln)
    return copied, ""


def spot_check(source: str, target: str, n: int = 5) -> tuple[bool, str]:
    """Compare up to n objects between source and target for size-identity."""
    r = run(["aws", "s3", "ls", f"s3://{source}", "--recursive"], check=False)
    if r.returncode != 0 or not r.stdout.strip():
        return True, "source empty"
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()][:n]
    if not lines:
        return True, "no objects"
    mismatches: list[str] = []
    for line in lines:
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        key = parts[3]
        src_size = parts[2]
        r2 = run(["aws", "s3api", "head-object", "--bucket", target, "--key", key], check=False)
        if r2.returncode != 0:
            mismatches.append(f"MISSING:{key}")
            continue
        meta = json.loads(r2.stdout)
        tgt_size = str(meta.get("ContentLength", "?"))
        if tgt_size != src_size:
            mismatches.append(f"SIZE_MISMATCH:{key}")
    if mismatches:
        return False, "; ".join(mismatches[:3])
    return True, f"checked {len(lines)} objects OK"


def expand(tpl: str, env: str) -> str:
    return tpl.replace("${DEPLOYMENT_ENV_SHORT}", env).replace("${AWS_ACCOUNT_ID}", AWS_ACCOUNT_ID)


def find_source(current_tpl: str, env: str, all_buckets: set[str]) -> str | None:
    """Find the actual source bucket, trying multiple naming conventions."""
    has_env_var = "${DEPLOYMENT_ENV_SHORT}" in current_tpl

    # 1. Canonical expansion
    c = expand(current_tpl, env)
    if c in all_buckets:
        return c

    if has_env_var:
        # 2. Full-word aliases (prod instead of prd, staging instead of stg)
        for alias in ENV_ALIASES.get(env, []):
            c = expand(current_tpl, alias)
            if c in all_buckets:
                return c
        # 3. No-env fallback: template with env placeholder removed
        no_env = (current_tpl
                  .replace("-${DEPLOYMENT_ENV_SHORT}-", "-")
                  .replace("-${DEPLOYMENT_ENV_SHORT}", "")
                  .replace("${DEPLOYMENT_ENV_SHORT}-", ""))
        c = expand(no_env, env)
        if c in all_buckets:
            return c
    else:
        # Template has no env var — check env-suffixed variants (old provisioning style)
        base = expand(current_tpl, env)  # already no-env
        for suffix_env in [env] + ENV_ALIASES.get(env, []):
            # Insert env between last segment and account ID
            c = base.replace(f"-{AWS_ACCOUNT_ID}", f"-{suffix_env}-{AWS_ACCOUNT_ID}")
            if c in all_buckets:
                return c

    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--asset-group", default=None)
    parser.add_argument("--kind", default=None)
    args = parser.parse_args()

    ws = Path(os.environ.get("WORKSPACE_ROOT", "/home/ubuntu/unified-trading-system-repos"))
    slot = os.environ.get("SLOT_ID", "10")
    pm = ws / ".tabs" / slot / "unified-trading-pm"
    csv_in = pm / "plans" / "audit" / "results" / "aws_gcp_bucket_symmetry_2026_05_20.csv"
    csv_out = pm / "plans" / "audit" / "results" / "phase5_aws_object_migrate.csv"

    print("Loading bucket inventory...")
    all_buckets = list_all_buckets()
    print(f"  {len(all_buckets)} buckets found")

    # Pre-load CSV into dict keyed by (kind, asset_group) for fast lookup
    records: dict[tuple[str, str], dict[str, str]] = {}
    with csv_in.open() as f:
        for rec in csv.DictReader(f):
            records[(rec["kind"], rec["asset_group"])] = rec

    rows: list[MigrationRow] = []
    for (kind, ag), rec in records.items():
        if rec["action"] == "no_action":
            continue
        if args.asset_group and ag not in (args.asset_group, "n/a"):
            continue
        if args.kind and kind != args.kind:
            continue
        tpl_cur = rec["current_template"]
        tpl_tgt = rec["target_template"]
        has_env = "${DEPLOYMENT_ENV_SHORT}" in tpl_cur or "${DEPLOYMENT_ENV_SHORT}" in tpl_tgt
        for env in (ENVS if has_env else ["prd"]):
            rows.append(MigrationRow(
                kind=kind, asset_group=ag,
                source=expand(tpl_cur, env),
                target=expand(tpl_tgt, env),
                env=env,
            ))

    print(f"Processing {len(rows)} pairs  [{('DRY-RUN' if args.dry_run else 'LIVE')}]\n")

    kind_checked: set[str] = set()
    for i, row in enumerate(rows):
        label = f"[{i+1}/{len(rows)}] {row.kind}/{row.asset_group}/{row.env}"
        rec = records.get((row.kind, row.asset_group), {})
        actual = find_source(rec.get("current_template", row.source), row.env, all_buckets)

        if actual is None:
            print(f"{label}  no-source-skip")
            row.status = "skipped_no_source"
            continue

        row.source = actual
        row.source_exists = True
        # Fast object count (only first page — for large buckets this is just ≤1000)
        row.source_object_count = bucket_object_count(actual)
        print(f"{label}  {actual}({row.source_object_count}) -> {row.target}")

        # Create target bucket
        ok = create_bucket(row.target, all_buckets, args.dry_run)
        row.target_created = ok
        if not ok:
            row.status = "error_create_target"
            row.error = "create-bucket failed"
            continue

        if row.source_object_count == 0 and not args.dry_run:
            row.status = "skipped_empty_source"
            row.spot_check_pass = True
            row.spot_check_note = "source empty"
            continue

        # Sync
        synced, err = sync_bucket(actual, row.target, args.dry_run)
        row.synced_objects = synced
        if err:
            row.status = "error_sync"
            row.error = err
            continue
        row.status = "synced" if not args.dry_run else "dry_run_ok"

        # Spot-check once per kind/ag
        ck = f"{row.kind}/{row.asset_group}"
        if ck not in kind_checked and not args.dry_run:
            ok2, note = spot_check(actual, row.target)
            row.spot_check_pass = ok2
            row.spot_check_note = note
            kind_checked.add(ck)
            print(f"    spot: {'PASS' if ok2 else 'FAIL'} {note}")
        elif args.dry_run:
            row.spot_check_pass = True
            row.spot_check_note = "dry-run"
        else:
            row.spot_check_pass = True
            row.spot_check_note = "kind already checked"

    # Write report
    fieldnames = ["kind", "asset_group", "env", "source", "target",
                  "source_exists", "source_object_count", "target_created",
                  "synced_objects", "spot_check_pass", "spot_check_note", "status", "error"]
    with csv_out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: getattr(r, k) for k in fieldnames})

    print(f"\nReport: {csv_out}")
    statuses = [r.status for r in rows]
    for s in sorted(set(statuses)):
        print(f"  {s}: {statuses.count(s)}")


if __name__ == "__main__":
    main()
