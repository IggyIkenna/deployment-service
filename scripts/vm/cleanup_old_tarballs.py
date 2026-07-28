#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Prune old code tarballs from GCS deployment-scripts bucket.

Two modes:
  1. **Name-versioned** (primary): tarballs named `<service>@<sha>.tar.gz` or
     `<service>-code-<sha>.tar.gz` accumulate per service.  Keep the N most
     recent per service (ordered by GCS object mtime); delete the rest.

  2. **GCS-noncurrent-versions** (secondary, --noncurrent): if the bucket has
     object versioning enabled/suspended, noncurrent versions of
     `<service>-code.tar.gz` accumulate silently.  Delete all noncurrent
     versions older than --max-age-days (default 7).

Current state (2026-06-01): SHA-versioned naming IS adopted —
`<service>-code@<sha>.tar.gz` tarballs accumulate unbounded (~366 live @sha
objects; 72 for unified-api-contracts alone), so the name-versioned cleanup
(mode 1, `--keep N`) IS needed and should run on a schedule.  This script is the
canonical cleanup tool.  (The earlier docstring claimed "no cleanup needed today"
under single-file naming — that is stale.)  Scheduling follow-up (daily Cloud
Scheduler + Cloud Run Job, `--keep 5`):
`plans/active/issues/deployment_scripts_bucket_softdelete_log_churn_2026_06_01.md`.

**Pin-aware (2026-07-20)**: mode 1 NEVER deletes a `@sha` tarball that a running
— or still relaunch-eligible — VM is pinned to, regardless of how far it has aged
down the mtime ranking, and it deletes each tarball together with its sibling
`.manifest.json` so an orphan manifest (a pin that resolves to deleted code) can
no longer be minted. If the in-use pin set cannot be determined the run
FAILS CLOSED and deletes nothing. See `deployment_service.vm.tarball_pins`.

SSOT: codex/05-infrastructure/vm-tarball-deployment.md

Usage:
    python cleanup_old_tarballs.py --project central-element-323112 --keep 5 --dry-run
    python cleanup_old_tarballs.py --project central-element-323112 --keep 5
    python cleanup_old_tarballs.py --project central-element-323112 --noncurrent --max-age-days 7 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

from unified_trading_library import get_storage_client
from unified_trading_library.cloud_interface import gcs_delete_object, gcs_describe_object  # noqa: qg-deep-import

from deployment_service.vm.gcp_instance_lister import list_running_instances_strict
from deployment_service.vm.tarball_pins import (
    CODE_PREFIX,
    DEFAULT_PIN_GRACE_DAYS,
    InUsePinsUnavailableError,
    TarballPin,
    collect_in_use_pins,
    find_orphan_manifests,
    is_pin_protected,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_GCS_LS_DATE = re.compile(r"^\s*\d+\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(gs://.+)$")

# Tarball filename patterns:
#   sha-versioned:  <service>@<sha>.tar.gz  OR  <service>-code-<sha>.tar.gz
#   simple:         <service>-code.tar.gz  (no sha — single-version per service)
_SHA_PATTERN = re.compile(r"^(.+?)(?:@([a-f0-9]+)|-code-([a-f0-9]+))\.tar\.gz$")
_SIMPLE_PATTERN = re.compile(r"^(.+?)-code\.tar\.gz$")

_TARBALL_SUFFIX = ".tar.gz"
_MANIFEST_SUFFIX = ".manifest.json"


class TarballEntry(TypedDict):
    gcs_path: str
    service: str
    sha: str
    mtime: datetime
    has_sha: bool


def _list_current_versions(bucket: str, prefix: str) -> list[tuple[datetime, str]]:
    """List live (current-version) objects under prefix via the UTL SDK wrapper."""
    rows: list[tuple[datetime, str]] = []
    for meta in get_storage_client(provider="gcp").list_blobs(bucket, prefix=prefix):
        if not meta.last_modified:
            continue
        ts = datetime.fromisoformat(meta.last_modified)
        rows.append((ts, f"gs://{bucket}/{meta.name}"))
    return rows


def _gsutil_ls_l_all_versions(prefix: str) -> list[tuple[datetime, str]]:
    """Run `gcloud storage ls -l -a` for NONCURRENT-VERSION listing only.

    GCS object-version listing (the `-a` flag / `#<generation>` suffixes marking
    noncurrent versions) has no UTL StorageClient equivalent -- list_blobs() only
    exposes prefix/delimiter/max_results/start_offset, no `versions=` parameter.
    This is the one remaining CLI call site in this file; all CURRENT-version
    listing is migrated to the SDK (`_list_current_versions`, above). `gcloud
    storage`, not `gsutil` — gsutil resolves creds from the CLI's active account
    (a short-lived WIF token in an interactive AO slot can't refresh unattended),
    while `gcloud storage` resolves via ADC, which stays valid. See
    plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    """
    cmd = ["gcloud", "storage", "ls", "-l", "-a", prefix]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: gcs-cli — GCS object-version listing has no UTL SDK equivalent (see docstring)
    if result.returncode != 0:
        logger.warning("gcloud storage ls failed: %s", result.stderr.strip())
        return []
    rows: list[tuple[datetime, str]] = []
    for line in result.stdout.splitlines():
        m = _GCS_LS_DATE.match(line)
        if not m:
            continue
        ts = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
        rows.append((ts, m.group(2).strip()))
    return rows


def _parse_tarballs(bucket: str, prefix: str) -> list[TarballEntry]:
    """List all .tar.gz objects under prefix; parse service name + mtime."""
    rows = _list_current_versions(bucket, prefix)
    entries: list[TarballEntry] = []
    for mtime, gcs_path in rows:
        filename = gcs_path.rsplit("/", 1)[-1]
        if not filename.endswith(".tar.gz"):
            continue
        sha_m = _SHA_PATTERN.match(filename)
        if sha_m:
            service = sha_m.group(1)
            sha = sha_m.group(2) or sha_m.group(3) or ""
            entries.append(TarballEntry(gcs_path=gcs_path, service=service, sha=sha, mtime=mtime, has_sha=True))
            continue
        simple_m = _SIMPLE_PATTERN.match(filename)
        if simple_m:
            service = simple_m.group(1)
            entries.append(TarballEntry(gcs_path=gcs_path, service=service, sha="", mtime=mtime, has_sha=False))
    return entries


def _delete_object(gcs_path: str, dry_run: bool, *, missing_ok: bool = False) -> bool:
    """Delete one object. Returns True on success (or an accepted absence).

    ``missing_ok`` exists for the manifest half of a pair delete: a tarball that
    never had a manifest is not a failure, and must not block its tarball's
    deletion forever. It is checked EXPLICITLY (describe-then-delete) rather than
    by swallowing the delete's exception, so a real permission/transport error
    still reports False and still aborts the pair.
    """
    if dry_run:
        logger.info("[DRY-RUN] would delete %s", gcs_path)
        return True
    if missing_ok:
        try:
            if gcs_describe_object(gcs_path) is None:
                logger.info("no object to delete at %s (nothing to do)", gcs_path)
                return True
        except Exception as exc:
            logger.warning("could not stat %s: %s", gcs_path, exc)
            return False
    try:
        gcs_delete_object(gcs_path)
        logger.info("deleted %s", gcs_path)
        return True
    except Exception as exc:
        logger.warning("failed to delete %s: %s", gcs_path, exc)
        return False


def _manifest_path_for(tarball_gcs_path: str) -> str:
    """The sibling ``...@<sha>.manifest.json`` path for a ``...@<sha>.tar.gz``."""
    return tarball_gcs_path[: -len(_TARBALL_SUFFIX)] + _MANIFEST_SUFFIX


def _delete_tarball_pair(gcs_path: str, dry_run: bool) -> bool:
    """Delete a tarball together with its sibling manifest. Returns tarball success.

    **Deletion order is load-bearing, and so is ABORTING on a partial failure.**
    The two objects must share a fate. Of the two possible half-states:

    - tarball deleted, manifest survives -> an **orphan manifest**: a pin that
      still RESOLVES but whose code is gone. This is the exact 2026-07-20 failure
      shape, and it is silent until a relaunch detonates on it.
    - manifest deleted, tarball survives -> a pin that no longer resolves at all.
      ``setup-data-pipeline-vm.sh`` refuses it loudly ("cannot verify
      provenance"). Loud, but still a broken pin.

    So the manifest goes FIRST **and its result is checked**: if it does not
    delete, we do NOT proceed to the tarball, and the pair is left COMPLETE and
    untouched — the only genuinely safe outcome. Retention deferred to tomorrow
    costs storage; either half-state costs a fleet.

    The old code could only ever produce the orphan state, because
    ``_parse_tarballs`` filters out everything not ending ``.tar.gz`` — manifests
    were structurally invisible to the sweep, so every run minted orphans — and
    because it DISCARDED the manifest delete's return value entirely.
    """
    manifest_path = _manifest_path_for(gcs_path)
    if not _delete_object(manifest_path, dry_run, missing_ok=True):
        logger.error(
            "ABORTING pair delete for %s — its manifest (%s) could not be deleted. "
            "Leaving the COMPLETE pair intact rather than minting an orphan manifest.",
            gcs_path,
            manifest_path,
        )
        return False
    return _delete_object(gcs_path, dry_run)


def reap_orphan_manifests(bucket: str, dry_run: bool, *, pins: frozenset[TarballPin]) -> int:
    """Delete ``@sha`` manifests whose tarball is provably gone. Returns the count.

    Cleans up the residue the incident ALREADY minted — every pre-fix sweep left
    one behind per deleted tarball. An orphan manifest is not inert: it is a pin
    that still resolves, so it makes a dead code identity look alive.

    Deliberately conservative. A manifest is reaped only when (a) a single
    whole-prefix listing shows no sibling tarball — never a per-object probe that
    could race a concurrent upload mid-rebuild, where the manifest lands before
    the tarball — and (b) nothing in the protected pin set claims it. (b) matters
    because a pinned-but-missing tarball is a live incident under repair, and the
    manifest is the only surviving evidence of which sha was wanted.
    """
    storage_client = get_storage_client()
    orphans = find_orphan_manifests(storage_client, bucket)
    if not orphans:
        logger.info("orphan-manifest reap: none found under gs://%s/%s", bucket, CODE_PREFIX)
        return 0

    reaped = 0
    for object_name in orphans:
        stem = object_name[: -len(_MANIFEST_SUFFIX)][len(CODE_PREFIX) :]
        service, _, sha = stem.partition("@")
        if is_pin_protected(service, sha, pins):
            logger.warning(
                "orphan manifest %s is PINNED by a live/relaunchable VM — NOT reaping. "
                "Its tarball is missing: that pin needs re-pinning, not evidence removal.",
                object_name,
            )
            continue
        logger.info("orphan manifest (tarball absent, unpinned): %s", object_name)
        if _delete_object(f"gs://{bucket}/{object_name}", dry_run):
            reaped += 1
    return reaped


def cleanup_name_versioned(
    bucket: str,
    keep: int,
    dry_run: bool,
    *,
    pins: frozenset[TarballPin] = frozenset(),
) -> dict[str, int]:
    """Delete old SHA-versioned tarballs; keep N most recent per service.

    ``pins`` is the in-use protected set (see
    :func:`deployment_service.vm.tarball_pins.collect_in_use_pins`). Protection
    is **orthogonal to ``keep``**: a pinned tarball is retained no matter how far
    it has fallen down the mtime ranking. That orthogonality is the property the
    incident needed — ``unified-api-contracts`` is the highest-velocity repo in
    the fleet, so at ``--keep 5`` any fleet outliving 5 UAC pushes was GUARANTEED
    to lose its pin, deterministically rather than unluckily.
    """
    entries = _parse_tarballs(bucket, "code/")

    # Only process SHA-versioned entries — single-version files are untouched
    sha_entries = [e for e in entries if e["has_sha"]]
    if not sha_entries:
        logger.info("No SHA-versioned tarballs found under gs://%s/code/ — nothing to clean up", bucket)
        return {}

    by_service: dict[str, list[TarballEntry]] = defaultdict(list)
    for entry in sha_entries:
        by_service[entry["service"]].append(entry)

    deleted: dict[str, int] = {}
    for service, service_entries in sorted(by_service.items()):
        sorted_entries = sorted(service_entries, key=lambda e: e["mtime"], reverse=True)
        to_keep = sorted_entries[:keep]
        candidates = sorted_entries[keep:]
        to_delete = [e for e in candidates if not is_pin_protected(service, e["sha"], pins)]
        protected = len(candidates) - len(to_delete)
        if protected:
            logger.info(
                "service=%s: %d aged-out tarball(s) PRESERVED — pinned by a running/relaunchable VM",
                service,
                protected,
            )
        if not to_delete:
            logger.info("service=%s: %d tarballs, %d to keep, 0 to delete", service, len(sorted_entries), keep)
            continue
        logger.info(
            "service=%s: %d tarballs, keeping %d most recent (+%d pinned), deleting %d",
            service,
            len(sorted_entries),
            len(to_keep),
            protected,
            len(to_delete),
        )
        count = 0
        for entry in to_delete:
            if _delete_tarball_pair(entry["gcs_path"], dry_run):
                count += 1
        deleted[service] = count
    return deleted


def collect_pins_for_project(project: str, bucket: str, *, grace_days: int) -> frozenset[TarballPin]:
    """Build the in-use protected pin set, or raise ``InUsePinsUnavailableError``.

    Fail-closed by construction: a compute-API failure propagates (via
    ``list_running_instances_strict``) instead of degrading to an empty
    "nothing is running" set that would authorise deleting every live pin.

    Instances are passed WHOLE, not as bare names — their ``metadata`` is where
    the pins live, and reducing to names here is what made the first fix inert.
    """
    try:
        running = list_running_instances_strict(project)
    except Exception as exc:
        raise InUsePinsUnavailableError(f"listing RUNNING instances in {project} failed: {exc!r}") from exc
    storage_client = get_storage_client()
    return frozenset(collect_in_use_pins(storage_client, bucket, running_instances=running, grace_days=grace_days))


def cleanup_noncurrent_versions(bucket: str, max_age_days: int, dry_run: bool) -> int:
    """Delete noncurrent GCS object versions older than max_age_days.

    Only effective when bucket versioning is Enabled or Suspended.
    Safe to run when versioning is off — gsutil ls -a will list only live objects.
    """
    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    rows = _gsutil_ls_l_all_versions(f"gs://{bucket}/code/")

    # gsutil ls -la adds #<generation> suffix for noncurrent versions
    # e.g.: gs://bucket/code/svc-code.tar.gz#1234567890
    noncurrent_count = 0
    for mtime, gcs_path in rows:
        if "#" not in gcs_path:
            continue  # Live version — skip
        if mtime >= cutoff:
            continue  # Recent enough — keep
        logger.info("noncurrent version (age=%s): %s", datetime.now(UTC) - mtime, gcs_path)
        if _delete_object(gcs_path, dry_run):
            noncurrent_count += 1
    return noncurrent_count


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--bucket",
        default="",
        help="GCS bucket name (default: deployment-scripts-{project})",
    )
    parser.add_argument(
        "--keep", type=int, default=5, help="Number of most-recent tarballs to keep per service (name-versioned mode)"
    )
    parser.add_argument(
        "--noncurrent",
        action="store_true",
        help="Clean up GCS noncurrent object versions instead of name-versioned tarballs",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=7,
        help="Max age (days) for noncurrent versions before deletion (--noncurrent mode)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report but do not delete")
    parser.add_argument(
        "--pin-grace-days",
        type=int,
        default=DEFAULT_PIN_GRACE_DAYS,
        help=(
            "Protect tarballs pinned by any VM launched within this many days, even if that VM is no "
            "longer running (covers SPOT-preemption relaunch of an already-deleted instance)"
        ),
    )
    parser.add_argument(
        "--reap-orphan-manifests",
        action="store_true",
        help=(
            "Also delete @sha manifests whose tarball is provably absent (the residue every "
            "pre-2026-07-20 sweep minted). Never touches a manifest claimed by the in-use pin set"
        ),
    )
    args = parser.parse_args(argv)

    project: str = cast(str, args.project)
    bucket: str = cast(str, args.bucket) or f"deployment-scripts-{project}"
    keep: int = cast(int, args.keep)
    max_age_days: int = cast(int, args.max_age_days)
    dry_run: bool = cast(bool, args.dry_run)
    noncurrent: bool = cast(bool, args.noncurrent)
    pin_grace_days: int = cast(int, args.pin_grace_days)
    reap_orphans: bool = cast(bool, args.reap_orphan_manifests)

    logger.info(
        "bucket=gs://%s  dry_run=%s  mode=%s", bucket, dry_run, "noncurrent" if noncurrent else "name-versioned"
    )

    if noncurrent:
        deleted = cleanup_noncurrent_versions(bucket, max_age_days, dry_run)
        logger.info("noncurrent cleanup complete: %d version(s) deleted", deleted)
    else:
        # FAIL-CLOSED: never delete SHA-pinned tarballs against an unknown or
        # partial view of what is running. Retention blocked for a day costs
        # storage; reaping a live pin silently bricks a whole fleet's relaunch
        # path (2026-07-20).
        try:
            pins = collect_pins_for_project(project, bucket, grace_days=pin_grace_days)
        except InUsePinsUnavailableError as exc:
            logger.error("FAIL-CLOSED: could not determine in-use tarball pins (%s) — deleting NOTHING this run", exc)
            return 1
        logger.info("in-use pin protection: %d pinned tarball(s) exempt from retention", len(pins))
        deleted_by_service = cleanup_name_versioned(bucket, keep, dry_run, pins=pins)
        total = sum(deleted_by_service.values())
        logger.info(
            "name-versioned cleanup complete: %d service(s) processed, %d tarball(s) deleted",
            len(deleted_by_service),
            total,
        )
        if reap_orphans:
            reaped = reap_orphan_manifests(bucket, dry_run, pins=pins)
            logger.info("orphan-manifest reap complete: %d manifest(s) deleted", reaped)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
