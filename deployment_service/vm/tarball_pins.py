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

**The protected set is a UNION of two sources, and it has to be.** The first fix
attempt read pins from ``LAUNCH_PARAMS.json`` alone and was therefore INERT: the
launchers that WRITE that blob and the launchers that PIN are disjoint sets. Only
``launch-cefi-sharded-backfill.sh`` calls ``lc_write_launch_params``, and it
passes no ``*_TARBALL_SHA`` keys at all; the five launchers that DO pin
(``launch-canonical-migration-vm.sh``, ``launch-legacy-bucket-migration-sharded.sh``,
``launch-mdps-backfill-vm.sh``, ``launch-mdps-sharded-backfill.sh``,
``launch-mtds-dex-swaps-backfill-vm.sh``) record the sha as GCE **instance
metadata**. ``collect_in_use_pins`` returned an empty set for every VM, always.

So the two legs, each covering the window the other cannot:

* **Leg A — live instance metadata.** Where the pins have always actually been.
  Protects every currently-RUNNING VM with no launcher change at all, which is
  what makes this fix effective on already-running fleets rather than only on
  things launched after it ships. Requires the UTL provider to carry ``metadata``
  through ``aggregated_list_instances`` (added 2026-07-20; before that the
  provider built its result dicts as exactly ``{"name", "status", "zone"}`` and
  metadata was dropped at the provider boundary).
* **Leg B — the durable pin registry**, ``vm-logs/{vm}/TARBALL_PINS.json``,
  written by ``lc_write_tarball_pin_record`` at launch. Instance metadata dies
  WITH the instance; the incident's whole point is the window where the VM is
  gone — SPOT-preempted or self-deleted — and ``RelaunchPreemptedVm`` still needs
  the tarball. Bounded by ``DEFAULT_PIN_GRACE_DAYS`` so it cannot grow unbounded.

``LAUNCH_PARAMS.json`` is retained as a third, best-effort source: harmless, and
it starts protecting automatically if a pinning launcher ever adopts that helper.

Two halves live here, both read-only over the unified cloud interface:

1. :func:`collect_in_use_pins` — the protected set the cleanup sweep subtracts
   before deleting, plus the FAIL-CLOSED gate (:class:`InUsePinsUnavailableError`).

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

import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final, cast

from unified_trading_library import StorageClient

from deployment_service.data_pipeline_monitors._gcs import read_launch_params, read_text

logger = logging.getLogger(__name__)

# Object-name prefix every code tarball lives under, inside the code bucket
# (``deployment-scripts-<project>``). Callers pass the bucket separately — no
# inline ``gs://`` URI is ever built here.
CODE_PREFIX: Final[str] = "code/"

_TARBALL_SUFFIX: Final[str] = ".tar.gz"
_MANIFEST_SUFFIX: Final[str] = ".manifest.json"

TARBALL_PINS_BLOB: Final[str] = "vm-logs/{vm}/TARBALL_PINS.json"

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

# How long after its pin record was written a (possibly already deleted) VM's
# pin stays protected. Covers the preemption-recovery window: the VM is gone from
# the GCE instance list, but RelaunchPreemptedVm can still be asked to bring it
# back and needs the exact tarball it was pinned to. Doubles as the registry's
# TTL — records older than this stop being honoured, so the protected set (and
# the retention exemption it grants) is bounded.
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


def pins_from_launch_env(env: Mapping[str, str]) -> set[TarballPin]:
    """Extract the SHA-pins recorded in one VM's captured launch env.

    Shared by all three sources — instance metadata, ``TARBALL_PINS.json`` and
    ``LAUNCH_PARAMS.json`` all key their pins by the same ``*_TARBALL_SHA`` env
    names, because they are all ultimately the env the launcher was invoked with.
    """
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


# ---------------------------------------------------------------------------
# Leg A — pins held by RUNNING instances, read from live instance metadata
# ---------------------------------------------------------------------------


def pins_from_instances(
    instances: Iterable[Mapping[str, object]],
) -> tuple[set[TarballPin], set[str]]:
    """Pins carried by RUNNING instances, plus the VMs whose metadata is UNKNOWN.

    Returns ``(pins, unknown_vm_names)``. A VM lands in ``unknown_vm_names`` when
    its row carries NO ``metadata`` key at all — i.e. the transport did not
    report metadata, so we cannot tell a pinned VM from an unpinned one. That is
    emphatically NOT the same as ``metadata == {}``, which is a definite "this VM
    pinned nothing" and is safe. Collapsing the two is the exact shape of the
    original bug, one layer down: the provider silently dropped metadata and the
    consumer read the silence as "nothing pinned".

    Only RUNNING instances are considered — a TERMINATED/STOPPING instance's pin
    is covered by the durable registry (Leg B) instead, on its mtime grace.
    """
    pins: set[TarballPin] = set()
    unknown: set[str] = set()
    for inst in instances:
        # Empty-fallback rationale: an instance row with no name/status is not a running VM we
        # can act on; "" is correctly falsy here and the row is skipped. Note this
        # is NOT the fail-open direction — a row we cannot identify is EXCLUDED
        # from the pins, and the caller's fail-closed gate then blocks on it.
        name = str(inst.get("name", ""))  # noqa: qg-empty-fallback
        if not name or str(inst.get("status", "")) != "RUNNING":  # noqa: qg-empty-fallback
            continue
        if "metadata" not in inst:
            unknown.add(name)
            continue
        raw_md = inst.get("metadata")
        if not isinstance(raw_md, dict):
            unknown.add(name)
            continue
        md = cast("dict[str, object]", raw_md)
        pins |= pins_from_launch_env({str(k): str(v) for k, v in md.items()})
    return pins, unknown


# ---------------------------------------------------------------------------
# Leg B — pins held by the durable, VM-deletion-surviving registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PinRecord:
    """One VM's ``TARBALL_PINS.json``, as honoured by the retention sweep."""

    vm_name: str
    pins: set[TarballPin]
    # Repos the launcher explicitly recorded as UNPINNED-ON-PURPOSE. Its presence
    # is what makes a record with zero pins a definite answer rather than a gap.
    floating: tuple[str, ...]


def read_pin_record(storage_client: StorageClient, bucket: str, vm_name: str) -> PinRecord | None:
    """Read one VM's durable pin record, or ``None`` if absent/unparseable."""
    raw = read_text(storage_client, bucket, TARBALL_PINS_BLOB.format(vm=vm_name))
    if not raw:
        return None
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(loaded, dict):
        return None
    data = cast("dict[str, object]", loaded)
    pins_obj = data.get("pins")
    env: dict[str, str] = {}
    if isinstance(pins_obj, dict):
        env = {str(k): str(v) for k, v in cast("dict[str, object]", pins_obj).items()}
    floating_obj = data.get("floating")
    floating: tuple[str, ...] = ()
    if isinstance(floating_obj, list):
        floating = tuple(str(k) for k in cast("list[object]", floating_obj))
    return PinRecord(vm_name=vm_name, pins=pins_from_launch_env(env), floating=floating)


def recently_recorded_vm_names(
    storage_client: StorageClient,
    bucket: str,
    *,
    grace_days: int = DEFAULT_PIN_GRACE_DAYS,
    now: datetime | None = None,
) -> set[str]:
    """VM names with a pin record or launch-params blob inside the grace window.

    These are relaunch-eligible even when the instance itself is long gone
    (SPOT-preempted, or self-deleted on setup failure), so their pins must stay
    protected. A blob with an unparseable/absent mtime is INCLUDED — failing
    toward protection is the safe direction for a retention sweep. Records older
    than the window are dropped, which is what bounds the registry's growth.
    """
    cutoff = (now or datetime.now(UTC)) - timedelta(days=grace_days)
    vm_names: set[str] = set()
    for blob in storage_client.list_blobs(bucket, prefix="vm-logs/"):
        name = blob.name
        if not (name.endswith("/TARBALL_PINS.json") or name.endswith("/LAUNCH_PARAMS.json")):
            continue
        parts = name.split("/")
        if len(parts) != 3:
            continue
        mtime = _parse_blob_mtime(blob.last_modified)
        if mtime is not None and mtime < cutoff:
            continue
        vm_names.add(parts[1])
    return vm_names


# ---------------------------------------------------------------------------
# The union + the fail-closed gate
# ---------------------------------------------------------------------------


def collect_in_use_pins(
    storage_client: StorageClient,
    bucket: str,
    *,
    running_instances: Iterable[Mapping[str, object]],
    grace_days: int = DEFAULT_PIN_GRACE_DAYS,
    now: datetime | None = None,
) -> set[TarballPin]:
    """The set of tarball pins a retention sweep must NOT delete.

    ``union(Leg A live instance-metadata pins, Leg B durable-registry pins within
    grace)``, plus best-effort ``LAUNCH_PARAMS.json`` pins.

    ``running_instances`` are the raw rows from
    ``ComputeEngineClient.aggregated_list_instances`` — passed whole rather than
    as bare names precisely so their ``metadata`` survives to here.

    Raises :class:`InUsePinsUnavailableError` — so the caller fails CLOSED and
    deletes nothing — when either:

    * the registry listing fails (a partial view of the registry is
      indistinguishable from a small one); or
    * ANY RUNNING VM's pins are UNOBSERVABLE — no instance metadata reported for
      it AND no covering record from the durable registry / LAUNCH_PARAMS. We
      cannot prove that VM's tarball is safe to delete, and "unprovable" must
      never resolve to "delete". The gate is name-agnostic on purpose: every
      pinning launcher accepts a ``--vm-name``/``VM_NAME`` override, so no
      name-prefix heuristic can enumerate the VMs that might hold a pin.

    Two escape hatches keep that gate affordable rather than permanent:
    ``metadata == {}`` is a definite "pinned nothing" (observed, not a blind
    spot), and a launcher that deliberately leaves a repo unpinned records
    ``floating`` in its pin record — also a definite answer. So neither a
    non-pinning fleet nor a legitimately-floating one blocks retention forever.
    """
    metadata_pins, unknown_metadata_vms = pins_from_instances(running_instances)
    # Empty-fallback rationale: as above — an unnameable row cannot be a VM we gate on, and the
    # explicit discard("") below removes it rather than letting "" masquerade as a
    # VM name.
    running_names = {
        str(inst.get("name", ""))  # noqa: qg-empty-fallback
        for inst in running_instances
        if str(inst.get("status", "")) == "RUNNING"  # noqa: qg-empty-fallback
    }
    running_names.discard("")

    try:
        recorded = recently_recorded_vm_names(storage_client, bucket, grace_days=grace_days, now=now)
    except Exception as exc:
        raise InUsePinsUnavailableError(f"listing vm-logs/ pin records failed: {exc!r}") from exc

    pins: set[TarballPin] = set(metadata_pins)
    covered_by_record: set[str] = set()
    for vm_name in sorted(recorded | running_names):
        record = read_pin_record(storage_client, bucket, vm_name)
        if record is not None:
            pins |= record.pins
            # A record with zero pins still COVERS the VM as long as it declared
            # what it left floating — that is an answer, not a blind spot.
            if record.pins or record.floating:
                covered_by_record.add(vm_name)
        env = read_launch_params(storage_client, bucket, vm_name)
        if env:
            launch_pins = pins_from_launch_env(env)
            pins |= launch_pins
            if launch_pins:
                covered_by_record.add(vm_name)

    # FAIL-CLOSED gate: ANY running VM whose pins we cannot observe at all.
    #
    # Deliberately NOT keyed on a VM-name prefix. The previous version gated on
    # ``is_pinning_vm()``, a heuristic over the five pinning launchers' DEFAULT
    # name shapes — but every one of them accepts an operator-supplied name
    # (``launch-mdps-backfill-vm.sh --vm-name my-thing``,
    # ``VM_NAME=x launch-mtds-dex-swaps-backfill-vm.sh``), so a genuinely pinning
    # VM trivially runs OFF the prefix list. Such a VM, on a jobs image still
    # carrying the pre-2026-07-20 UTL (no ``metadata`` key at the provider
    # boundary) and predating the durable registry (no ``TARBALL_PINS.json``),
    # was neither SEEN by Leg A nor GATED by this check: pins came back empty,
    # nothing raised, the sweep proceeded, and a live pin was evicted — the
    # original incident, reached by a second route. Steady-state the name was
    # irrelevant (metadata is read regardless of it); the hole was the
    # TRANSITION window, which is exactly when a name heuristic is load-bearing.
    #
    # Observability is the property that actually matters: if a VM is RUNNING and
    # we cannot see its pins by ANY route, we cannot prove ANY tarball is unused.
    # "Unprovable" must resolve to "do not delete" whatever the VM is called.
    #
    # This does NOT block on a VM that legitimately pins nothing: ``metadata ==
    # {}`` is an OBSERVATION ("this VM pinned nothing") and never enters
    # ``unknown_metadata_vms`` — only a row carrying no ``metadata`` key at all,
    # i.e. one the transport never reported on, does. Retention therefore resumes
    # by itself the moment the metadata-carrying UTL reaches the jobs image;
    # until then the sweep fails closed daily, which is the safe direction.
    unprovable = sorted(vm for vm in running_names if vm not in covered_by_record and vm in unknown_metadata_vms)
    if unprovable:
        raise InUsePinsUnavailableError(
            f"{len(unprovable)} RUNNING VM(s) with NO observable pin record — instance metadata was not "
            f"reported for them and neither the durable registry nor LAUNCH_PARAMS covers them: {unprovable} "
            "— refusing to delete against an incomplete view (a pinning launcher's --vm-name/VM_NAME override "
            "means ANY name can be a pinning VM). Expected until the metadata-carrying UTL reaches the jobs image."
        )

    logger.info(
        "collect_in_use_pins: %d running instance(s), %d recorded VM(s) -> %d protected pin(s) (%d from live metadata)",
        len(running_names),
        len(recorded),
        len(pins),
        len(metadata_pins),
    )
    return pins


def collect_pins_for_vm(
    storage_client: StorageClient,
    bucket: str,
    vm_name: str,
) -> set[TarballPin]:
    """Every pin recorded for ONE VM, from the durable registry + launch params.

    The relaunch path's view of the same authoritative union. Instance metadata
    is deliberately absent here: by the time a relaunch is being considered the
    instance is gone, which is exactly why the durable registry exists.
    """
    pins: set[TarballPin] = set()
    record = read_pin_record(storage_client, bucket, vm_name)
    if record is not None:
        pins |= record.pins
    env = read_launch_params(storage_client, bucket, vm_name)
    if env:
        pins |= pins_from_launch_env(env)
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


def find_orphan_manifests(
    storage_client: StorageClient,
    bucket: str,
) -> list[str]:
    """Object names of manifests whose sibling ``.tar.gz` is PROVABLY absent.

    These are the residue the incident already minted: the sweep deleted the
    tarball and structurally could not delete the manifest, leaving a pin that
    still resolves onto code that is gone. "Provably" is doing real work — the
    listing is a single pass over the whole ``code/`` prefix, so a manifest is
    only called orphaned when its tarball was absent from that SAME listing,
    never from a per-object existence probe that could race a concurrent upload.
    """
    tarballs: set[str] = set()
    manifests: set[str] = set()
    for blob in storage_client.list_blobs(bucket, prefix=CODE_PREFIX):
        name = blob.name
        if name.endswith(_TARBALL_SUFFIX):
            tarballs.add(name[: -len(_TARBALL_SUFFIX)])
        elif name.endswith(_MANIFEST_SUFFIX):
            manifests.add(name[: -len(_MANIFEST_SUFFIX)])
    # Only ``@sha``-pinned manifests are eligible. The floating
    # ``<svc>-code.manifest.json`` legitimately outlives rebuild gaps.
    return sorted(f"{stem}{_MANIFEST_SUFFIX}" for stem in (manifests - tarballs) if "@" in stem)


__all__ = [
    "CODE_PREFIX",
    "DEFAULT_PIN_GRACE_DAYS",
    "TARBALL_PINS_BLOB",
    "TARBALL_SHA_ENV_TO_NAME",
    "InUsePinsUnavailableError",
    "PinRecord",
    "TarballPin",
    "collect_in_use_pins",
    "collect_pins_for_vm",
    "find_orphan_manifests",
    "is_pin_protected",
    "pins_from_instances",
    "pins_from_launch_env",
    "read_pin_record",
    "recently_recorded_vm_names",
    "resolve_available_pin",
]
