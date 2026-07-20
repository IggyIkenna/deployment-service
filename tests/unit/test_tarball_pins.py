"""Unit tests for deployment_service.vm.tarball_pins.

Regression guards for the 2026-07-20 cefi-migration-fleet outage: the daily
tarball retention sweep reaped a SHA-pinned `.tar.gz` that a running fleet
depended on, left the sibling `.manifest.json` behind (an orphan manifest — a pin
that resolves to deleted code), and thereby made every relaunch of that fleet
impossible while looking perfectly healthy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from deployment_service.vm.tarball_pins import (
    TarballPin,
    collect_in_use_pins,
    is_pin_protected,
    pins_from_launch_env,
    recently_launched_vm_names,
    resolve_available_pin,
)


@dataclass
class _Blob:
    name: str
    last_modified: str | None = None


class _FakeStorageClient:
    """Minimal StorageClient stand-in: list_blobs + blob_exists + download_bytes."""

    def __init__(self, objects: dict[str, str], mtimes: dict[str, str] | None = None) -> None:
        self._objects = objects
        self._mtimes = mtimes or {}

    def list_blobs(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        max_results: int | None = None,
    ) -> list[_Blob]:
        del bucket, delimiter, max_results
        return [_Blob(name=n, last_modified=self._mtimes.get(n)) for n in sorted(self._objects) if n.startswith(prefix)]

    def blob_exists(self, bucket: str, blob_path: str) -> bool:
        del bucket
        return blob_path in self._objects

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        del bucket
        return self._objects[blob_path].encode("utf-8")


def _iso(days_ago: float) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).isoformat()


def _launch_params(**env: str) -> str:
    import json

    return json.dumps({"launcher": "launch-canonical-migration-vm.sh", "env": env})


class TestPinsFromLaunchEnv:
    def test_extracts_every_known_pin_var(self) -> None:
        pins = pins_from_launch_env({"UAC_TARBALL_SHA": "abc", "UTL_TARBALL_SHA": "def", "OTHER": "x"})
        assert pins == {
            TarballPin(tarball_name="unified-api-contracts-code", sha="abc"),
            TarballPin(tarball_name="unified-trading-library-code", sha="def"),
        }

    def test_mtds_maps_to_mtds_code_not_the_repo_name(self) -> None:
        """MTDS_TARBALL_SHA -> `mtds-code`; mirrors setup-data-pipeline-vm.sh's case block."""
        pins = pins_from_launch_env({"MTDS_TARBALL_SHA": "abc"})
        assert pins == {TarballPin(tarball_name="mtds-code", sha="abc")}

    def test_blank_and_absent_pins_ignored(self) -> None:
        assert pins_from_launch_env({"UAC_TARBALL_SHA": "  "}) == set()


class TestIsPinProtected:
    def test_exact_match(self) -> None:
        pins = {TarballPin("uac-code", "abc123")}
        assert is_pin_protected("uac-code", "abc123", pins)

    def test_prefix_tolerant_both_directions(self) -> None:
        full = "acd8714c811027910cc18e730f8b38a6deef7822"
        assert is_pin_protected("uac-code", full, {TarballPin("uac-code", "acd8714c")})
        assert is_pin_protected("uac-code", "acd8714c", {TarballPin("uac-code", full)})

    def test_other_service_not_protected(self) -> None:
        assert not is_pin_protected("utl-code", "abc123", {TarballPin("uac-code", "abc123")})

    def test_empty_sha_never_protected(self) -> None:
        assert not is_pin_protected("uac-code", "", {TarballPin("uac-code", "abc")})


class TestRecentlyLaunchedVmNames:
    def test_includes_recent_excludes_stale(self) -> None:
        client = _FakeStorageClient(
            objects={
                "vm-logs/vm-fresh/LAUNCH_PARAMS.json": "{}",
                "vm-logs/vm-stale/LAUNCH_PARAMS.json": "{}",
                "vm-logs/vm-fresh/run.log": "noise",
            },
            mtimes={
                "vm-logs/vm-fresh/LAUNCH_PARAMS.json": _iso(2),
                "vm-logs/vm-stale/LAUNCH_PARAMS.json": _iso(90),
            },
        )
        assert recently_launched_vm_names(client, "bucket", grace_days=14) == {"vm-fresh"}

    def test_unparseable_mtime_is_included_protection_is_the_safe_default(self) -> None:
        client = _FakeStorageClient(objects={"vm-logs/vm-x/LAUNCH_PARAMS.json": "{}"}, mtimes={})
        assert recently_launched_vm_names(client, "bucket", grace_days=14) == {"vm-x"}


class TestCollectInUsePins:
    def test_protects_pins_of_a_deleted_but_relaunchable_vm(self) -> None:
        """THE incident's core gap.

        A SPOT-preempted / self-deleted VM is absent from the RUNNING instance
        list, yet RelaunchPreemptedVm still needs its pinned tarball. Protecting
        only running VMs would leave the outage unfixed.
        """
        client = _FakeStorageClient(
            objects={
                "vm-logs/gone-vm/LAUNCH_PARAMS.json": _launch_params(UAC_TARBALL_SHA="acd8714c"),
            },
            mtimes={"vm-logs/gone-vm/LAUNCH_PARAMS.json": _iso(3)},
        )
        pins = collect_in_use_pins(client, "bucket", running_vm_names=set())

        assert TarballPin("unified-api-contracts-code", "acd8714c") in pins

    def test_unions_running_and_recent(self) -> None:
        client = _FakeStorageClient(
            objects={
                "vm-logs/running-vm/LAUNCH_PARAMS.json": _launch_params(UTL_TARBALL_SHA="utl111"),
                "vm-logs/recent-vm/LAUNCH_PARAMS.json": _launch_params(UAC_TARBALL_SHA="uac222"),
                "vm-logs/ancient-vm/LAUNCH_PARAMS.json": _launch_params(UAC_TARBALL_SHA="uac999"),
            },
            mtimes={
                "vm-logs/running-vm/LAUNCH_PARAMS.json": _iso(400),
                "vm-logs/recent-vm/LAUNCH_PARAMS.json": _iso(1),
                "vm-logs/ancient-vm/LAUNCH_PARAMS.json": _iso(400),
            },
        )
        pins = collect_in_use_pins(client, "bucket", running_vm_names={"running-vm"}, grace_days=14)

        assert TarballPin("unified-trading-library-code", "utl111") in pins, "running wins over stale mtime"
        assert TarballPin("unified-api-contracts-code", "uac222") in pins
        assert TarballPin("unified-api-contracts-code", "uac999") not in pins


class TestResolveAvailablePin:
    def _client(self, objects: dict[str, str], mtimes: dict[str, str] | None = None) -> _FakeStorageClient:
        return _FakeStorageClient(objects, mtimes)

    def test_intact_pin_returned_unchanged(self) -> None:
        client = self._client(
            {
                "code/uac-code@abc.tar.gz": "x",
                "code/uac-code@abc.manifest.json": "{}",
            }
        )
        assert resolve_available_pin(client, "bucket", "uac-code", "abc") == "abc"

    def test_reaped_pin_resolves_to_newest_complete_pair(self) -> None:
        """The incident's recovery path: the pinned tar.gz is gone, its manifest orphaned."""
        client = self._client(
            objects={
                "code/uac-code@dead.manifest.json": "{}",  # orphan — tar.gz was reaped
                "code/uac-code@old.tar.gz": "x",
                "code/uac-code@old.manifest.json": "{}",
                "code/uac-code@new.tar.gz": "x",
                "code/uac-code@new.manifest.json": "{}",
            },
            mtimes={
                "code/uac-code@old.tar.gz": _iso(9),
                "code/uac-code@new.tar.gz": _iso(1),
            },
        )
        assert resolve_available_pin(client, "bucket", "uac-code", "dead") == "new"

    def test_never_repins_onto_a_half_deleted_pair(self) -> None:
        """A tarball with no manifest is refused by setup ("cannot verify provenance")."""
        client = self._client(
            objects={
                "code/uac-code@dead.manifest.json": "{}",
                "code/uac-code@halfy.tar.gz": "x",  # no manifest sibling
            },
            mtimes={"code/uac-code@halfy.tar.gz": _iso(1)},
        )
        assert resolve_available_pin(client, "bucket", "uac-code", "dead") is None

    def test_returns_none_rather_than_the_floating_tarball(self) -> None:
        """None => caller PAGEs. Falling back to floating is the banned silent degrade."""
        client = self._client({"code/uac-code.tar.gz": "x", "code/uac-code.manifest.json": "{}"})
        assert resolve_available_pin(client, "bucket", "uac-code", "dead") is None
