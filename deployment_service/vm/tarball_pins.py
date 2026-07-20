"""SSOT for "which SHA-pinned code tarballs are still IN USE" + pin re-resolution.

Closes the 2026-07-20 cefi-migration-fleet outage. A long-running migration fleet
pinned its VM code tarballs by SHA (``UAC_TARBALL_SHA=acd8714c...``); the daily
``scripts/vm/cleanup_old_tarballs.py`` sweep (``--keep 5``, ranked purely on GCS
object mtime, with ZERO awareness of what was running) evicted that pinned
``.tar.gz``. Because the cleanup only ever enumerated ``*.tar.gz``, the sibling
``...@<sha>.manifest.json`` SURVIVED — minting an **orphan manifest**: a pin that
still RESOLVES but whose code is gone. Every relaunch/preemption-recovery of that
fleet then died at ``setup-data-pipeline-vm.sh`` ("refusing floating fallback",
exit 1) and self-deleted, silently. That defeated the shipped ``PROGRESS.json``
checkpoint contract outright — resuming from the correct date is worthless when
the code tarball no longer exists.

Two halves live here, both read-only over the unified cloud interface:

1. :func:`collect_in_use_pins` — the protected set the cleanup sweep subtracts
   before deleting. Built from LIVE state: the pins of currently-RUNNING VMs,
   UNION the pins of any VM whose ``LAUNCH_PARAMS.json`` was written inside the
   relaunch grace window. The second term is load-bearing and is why this is not
   just an "enumerate running instances" check: a SPOT-preempted or self-deleted
   VM no longer exists to be enumerated, yet ``RelaunchPreemptedVm`` still needs
   its pinned tarball to recover. Protecting only running VMs would leave the
   exact incident unfixed.

2. :func:`resolve_available_pin` — the loud re-pin used on the relaunch path. If
   a recorded pin's tarball is already gone, it re-points at the newest pin that
   still has a COMPLETE (tarball + manifest) pair, so the caller can relaunch on
   a known, auditable code identity instead of dying. It NEVER falls back to the
   floating ``<svc>-code.tar.gz`` — the caller must page instead. Preserving
   ``setup-data-pipeline-vm.sh``'s refusal of a floating fallback is deliberate:
   the floating tarball is by definition newer, un-asserted code, and running it
   against a half-migrated corpus is a data-correctness hazard, not merely a
   reproducibility one.

SSOT: ``codex/05-infrastructure/vm-tarball-deployment.md``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors._gcs import read_launch_params

logger = logging.getLogger(__name__)

# Object-name prefix every code tarball lives under, inside the code bucket
# (``deployment-scripts-<project>``). Callers pass the bucket separately — no
# inline ``gs://`` URI is ever built here.
CODE_PREFIX: Final[str] = "code/"

_TARBALL_SUFFIX: Final[str] = ".tar.gz"
_MANIFEST_SUFFIX: Final[str] = ".manifest.json"

# Launch-env var -> tarball base name, mirroring the resolution `case` block in
# ``scripts/vm/setup-data-pipeline-vm.sh`` (the CONSUMER of these pins). Keep the
# two in lockstep: a key present here but not there protects a tarball nothing
# reads; a key there but not here leaves a live pin unprotected — the incident.
# NOTE ``MTDS_TARBALL_SHA`` maps to ``mtds-code``, NOT
# ``market-tick-data-service-code``.
TARBALL_SHA_ENV_TO_NAME: Final[dict[str, str]] = {
    "UTL_TARBALL_SHA": "unified-trading-library-code",
    "UAC_TARBALL_SHA": "unified-api-contracts-code",
    "MDPS_TARBALL_SHA": "market-data-processing-service-code",
    "MTDS_TARBALL_SHA": "mtds-code",
}

# How long after its LAUNCH_PARAMS.json was written a (possibly already deleted)
# VM's pin stays protected. Covers the preemption-recovery window: the VM is gone
# from the GCE instance list, but RelaunchPreemptedVm can still be asked to bring
# it back and needs the exact tarball it was pinned to.
DEFAULT_PIN_GRACE_DAYS: Final[int] = 14


@dataclass(frozen=True)
class TarballPin:
    """One ``<tarball_name>@<sha>`` code-tarball pin held by a VM."""

    tarball_name: str
    sha: str

    @property
    def tarball_object(self) -> str:
        return f"{CODE_PREFIX}{self.tarball_name}@{self.sha}{_TARBALL_SUFFIX}"

    @property
    def manifest_object(self) -> str:
        return f"{CODE_PREFIX}{self.tarball_name}@{self.sha}{_MANIFEST_SUFFIX}"


class InUsePinsUnavailableError(RuntimeError):
    """Raised when the in-use pin set could NOT be determined.

    The cleanup sweep treats this as FAIL-CLOSED and deletes nothing. "I could
    not read live state" must never be silently collapsed into "nothing is
    running" — that collapse is precisely how a live pin gets reaped.
    """


def pins_from_launch_env(env: dict[str, str]) -> set[TarballPin]:
    """Extract the SHA-pins recorded in one VM's captured launch env."""
    pins: set[TarballPin] = set()
    for env_key, tarball_name in TARBALL_SHA_ENV_TO_NAME.items():
        sha = env.get(env_key, "").strip()
        if sha:
            pins.add(TarballPin(tarball_name=tarball_name, sha=sha))
    return pins


def _parse_blob_mtime(last_modified: str | None) -> datetime | None:
    if not last_modified:
        return None
    try:
        parsed = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def recently_launched_vm_names(
    storage_client: StorageClient,
    bucket: str,
    *,
    grace_days: int = DEFAULT_PIN_GRACE_DAYS,
    now: datetime | None = None,
) -> set[str]:
    """VM names whose ``LAUNCH_PARAMS.json`` was written inside the grace window.

    These are relaunch-eligible even when the instance itself is long gone
    (SPOT-preempted, or self-deleted on setup failure), so their pins must stay
    protected. A blob with an unparseable/absent mtime is INCLUDED — failing
    toward protection is the safe direction for a retention sweep.
    """
    cutoff = (now or datetime.now(UTC)) - timedelta(days=grace_days)
    vm_names: set[str] = set()
    for blob in storage_client.list_blobs(bucket, prefix="vm-logs/"):
        name = blob.name
        if not name.endswith("/LAUNCH_PARAMS.json"):
            continue
        parts = name.split("/")
        if len(parts) != 3:
            continue
        mtime = _parse_blob_mtime(blob.last_modified)
        if mtime is not None and mtime < cutoff:
            continue
        vm_names.add(parts[1])
    return vm_names


def collect_in_use_pins(
    storage_client: StorageClient,
    bucket: str,
    *,
    running_vm_names: Iterable[str],
    grace_days: int = DEFAULT_PIN_GRACE_DAYS,
    now: datetime | None = None,
) -> set[TarballPin]:
    """The set of tarball pins a retention sweep must NOT delete.

    Union of (a) pins held by currently-RUNNING VMs and (b) pins of any VM
    launched inside the relaunch grace window. Reads each VM's pins from the
    ``LAUNCH_PARAMS.json`` blob the launcher already writes
    (``lc_write_launch_params``) via the existing
    :func:`~deployment_service.data_pipeline_monitors._gcs.read_launch_params`
    helper — no new mechanism, and it survives VM deletion, which is the whole
    point.

    Raises :class:`InUsePinsUnavailableError` if the launch-params listing fails,
    so the caller can fail closed rather than delete against a partial view.
    """
    candidates: set[str] = set(running_vm_names)
    try:
        candidates |= recently_launched_vm_names(storage_client, bucket, grace_days=grace_days, now=now)
    except Exception as exc:
        raise InUsePinsUnavailableError(f"listing vm-logs/ LAUNCH_PARAMS.json failed: {exc!r}") from exc

    pins: set[TarballPin] = set()
    for vm_name in sorted(candidates):
        env = read_launch_params(storage_client, bucket, vm_name)
        if env:
            pins |= pins_from_launch_env(env)
    logger.info(
        "collect_in_use_pins: %d candidate VM(s) -> %d protected pin(s)",
        len(candidates),
        len(pins),
    )
    return pins


def is_pin_protected(service: str, sha: str, pins: Iterable[TarballPin]) -> bool:
    """True when ``<service>@<sha>`` is held by an in-use pin.

    SHA comparison is prefix-tolerant in BOTH directions: a launcher may record a
    short sha while the object carries the full 40-char one (or vice versa), and
    a pin that fails to match its own tarball would silently re-open the incident.
    """
    if not sha:
        return False
    return any(pin.tarball_name == service and (sha.startswith(pin.sha) or pin.sha.startswith(sha)) for pin in pins)


def _sha_from_object(object_name: str, tarball_name: str, suffix: str) -> str:
    head = f"{CODE_PREFIX}{tarball_name}@"
    if not object_name.startswith(head) or not object_name.endswith(suffix):
        return ""
    return object_name[len(head) : -len(suffix)]


def resolve_available_pin(
    storage_client: StorageClient,
    bucket: str,
    tarball_name: str,
    requested_sha: str,
) -> str | None:
    """Return a pin SHA for ``tarball_name`` that provably RESOLVES, or ``None``.

    - ``requested_sha`` intact (tarball AND manifest both present) -> returned
      unchanged; the caller logs nothing.
    - tarball missing/incomplete -> the newest ``@sha`` with a COMPLETE pair,
      so the caller can re-pin LOUDLY (never silently).
    - no complete pinned pair at all -> ``None``; the caller MUST page. Returning
      the floating tarball here would be the silent-degrade trap this whole
      module exists to prevent — an empty/unset pin is indistinguishable from
      "no pin" to every ``[[ -n ... ]]`` guard in the launchers, which means
      floating.

    A pair is "complete" only when BOTH the ``.tar.gz`` and its sibling
    ``.manifest.json`` exist — never re-pin onto a half-deleted pair, since
    ``setup-data-pipeline-vm.sh`` refuses a tarball whose manifest is absent
    ("cannot verify provenance") just as hard as a missing tarball.
    """
    tarball_shas: dict[str, datetime | None] = {}
    manifest_shas: set[str] = set()
    for blob in storage_client.list_blobs(bucket, prefix=f"{CODE_PREFIX}{tarball_name}@"):
        tar_sha = _sha_from_object(blob.name, tarball_name, _TARBALL_SUFFIX)
        if tar_sha:
            tarball_shas[tar_sha] = _parse_blob_mtime(blob.last_modified)
            continue
        man_sha = _sha_from_object(blob.name, tarball_name, _MANIFEST_SUFFIX)
        if man_sha:
            manifest_shas.add(man_sha)

    complete = {sha: mtime for sha, mtime in tarball_shas.items() if sha in manifest_shas}
    if requested_sha and requested_sha in complete:
        return requested_sha
    if not complete:
        return None
    epoch = datetime.fromtimestamp(0, tz=UTC)
    return max(complete, key=lambda sha: (complete[sha] or epoch, sha))


__all__ = [
    "CODE_PREFIX",
    "DEFAULT_PIN_GRACE_DAYS",
    "TARBALL_SHA_ENV_TO_NAME",
    "InUsePinsUnavailableError",
    "TarballPin",
    "collect_in_use_pins",
    "is_pin_protected",
    "pins_from_launch_env",
    "recently_launched_vm_names",
    "resolve_available_pin",
]
