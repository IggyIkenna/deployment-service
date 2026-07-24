"""Unit tests for deployment_service.vm.tarball_pins.

Regression guards for the 2026-07-20 cefi-migration-fleet outage: the daily
tarball retention sweep reaped a SHA-pinned `.tar.gz` that a running fleet
depended on, left the sibling `.manifest.json` behind (an orphan manifest — a pin
that resolves to deleted code), and thereby made every relaunch of that fleet
impossible while looking perfectly healthy.

**These tests are deliberately driven by the REAL launchers' output**, not by
hand-written fixtures. The first fix attempt passed a suite that fabricated a
`LAUNCH_PARAMS.json` blob no launcher on earth produces, and shipped a
`collect_in_use_pins` that returned an empty set for every VM in production. A
fixture you authored yourself cannot detect that your producer and consumer
disagree — so:

* `test_pin_record_written_by_the_real_bash_helper_is_readable` EXECUTES
  `lc_write_tarball_pin_record` from `launcher_common.sh` with a fake `gsutil`
  and parses whatever it actually emitted;
* `test_every_pinning_launcher_writes_a_durable_pin_record` reads the five
  launcher scripts and asserts they really call it.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from deployment_service.vm.tarball_pins import (
    TARBALL_SHA_ENV_TO_NAME,
    InUsePinsUnavailableError,
    TarballPin,
    collect_in_use_pins,
    collect_pins_for_vm,
    find_orphan_manifests,
    is_pin_protected,
    pins_from_instances,
    pins_from_launch_env,
    read_pin_record,
    recently_recorded_vm_names,
    resolve_available_pin,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VM_SCRIPTS = _REPO_ROOT / "scripts" / "vm"
_LAUNCHER_COMMON = _VM_SCRIPTS / "lib" / "launcher_common.sh"

# The launchers that record a *_TARBALL_SHA pin. Established by
# `grep -rl TARBALL_SHA scripts/vm/` minus the CONSUMER (setup-data-pipeline-vm.sh).
# launch-mdps-backfill-vm.sh + launch-features-vm.sh auto-pin UAC_TARBALL_SHA to the
# current LDR-tip tarball (P0 mdps_vm_stale_uac_contract_propagation_2026_07_20) — the
# contract package was previously left floating on service VMs.
PINNING_LAUNCHERS = (
    "launch-canonical-migration-vm.sh",
    "launch-legacy-bucket-migration-sharded.sh",
    "launch-mdps-backfill-vm.sh",
    "launch-mdps-sharded-backfill.sh",
    "launch-mtds-dex-swaps-backfill-vm.sh",
    "launch-features-vm.sh",
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


class _ExplodingListStorageClient(_FakeStorageClient):
    """list_blobs raises — the "I cannot see the registry" case."""

    def list_blobs(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str | None = None,
        max_results: int | None = None,
    ) -> list[_Blob]:
        raise RuntimeError("GCS 503 backend error")


def _iso(days_ago: float) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).isoformat()


def _running(name: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
    """A RUNNING instance row in the shape the UTL provider now returns."""
    row: dict[str, object] = {"name": name, "status": "RUNNING", "zone": "asia-northeast1-c"}
    if metadata is not None:
        row["metadata"] = metadata
    return row


def _record(**pins: str) -> str:
    return json.dumps({"launcher": "launch-mdps-backfill-vm.sh", "pins": pins, "floating": []})


def _write_real_pin_record(tmp_path: Path, vm_name: str, launcher: str, pairs: list[str]) -> str:
    """Run the REAL `lc_write_tarball_pin_record` and return what it uploaded.

    A fake `gsutil` on PATH captures stdin instead of talking to GCS, so this
    exercises the actual shell + embedded-python producer the launchers call —
    the thing a hand-written fixture cannot vouch for.
    """
    captured = tmp_path / "uploaded.json"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(exist_ok=True)
    gsutil = fake_bin / "gsutil"
    _ = gsutil.write_text(f'#!/bin/sh\ncat > "{captured}"\n')
    gsutil.chmod(0o755)

    env = {**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"}
    script = 'source "$1"; shift; lc_write_tarball_pin_record "$@"'
    result = subprocess.run(  # noqa: S603
        ["/bin/bash", "-c", script, "_", str(_LAUNCHER_COMMON), vm_name, "test-project", launcher, *pairs],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f"lc_write_tarball_pin_record failed: {result.stderr}"
    assert captured.exists(), f"helper uploaded nothing; stderr={result.stderr}"
    return captured.read_text()


# ---------------------------------------------------------------------------
# (a) Collection driven by the REAL launchers' output shape
# ---------------------------------------------------------------------------


class TestRealLauncherOutputShape:
    def test_pin_record_written_by_the_real_bash_helper_is_readable(self, tmp_path: Path) -> None:
        """The producer (bash) and the consumer (this module) must agree.

        THE test the v1 suite lacked: it asserted against a fabricated blob, so
        it stayed green while the real producer wrote nothing of the sort.
        """
        raw = _write_real_pin_record(
            tmp_path,
            "canonical-migration-cefi-20260720-010101",
            "launch-canonical-migration-vm.sh",
            ["UAC_TARBALL_SHA=acd8714c", "UTL_TARBALL_SHA=", "MTDS_TARBALL_SHA=844124f7"],
        )

        client = _FakeStorageClient({"vm-logs/canonical-migration-cefi-20260720-010101/TARBALL_PINS.json": raw})
        record = read_pin_record(client, "bucket", "canonical-migration-cefi-20260720-010101")

        assert record is not None, "the real helper's output must parse"
        assert record.pins == {
            TarballPin("unified-api-contracts-code", "acd8714c"),
            TarballPin("mtds-code", "844124f7"),
        }
        assert record.floating == ("UTL_TARBALL_SHA",), "an unset pin is a DELIBERATE float, not a pin to nothing"

    def test_real_helper_output_flows_all_the_way_into_the_protected_set(self, tmp_path: Path) -> None:
        """End-to-end: real launcher output -> collect_in_use_pins -> NON-EMPTY.

        The single assertion that would have caught v1 being inert.
        """
        vm = "canonical-migration-cefi-20260720-010101"
        raw = _write_real_pin_record(
            tmp_path, vm, "launch-canonical-migration-vm.sh", ["UAC_TARBALL_SHA=acd8714c811027910cc18e730f8b38a6"]
        )
        client = _FakeStorageClient(
            {f"vm-logs/{vm}/TARBALL_PINS.json": raw},
            mtimes={f"vm-logs/{vm}/TARBALL_PINS.json": _iso(1)},
        )

        pins = collect_in_use_pins(client, "bucket", running_instances=[])

        assert pins, "THE v1 bug: this set was empty for every VM, always"
        assert TarballPin("unified-api-contracts-code", "acd8714c811027910cc18e730f8b38a6") in pins

    @pytest.mark.parametrize("launcher", PINNING_LAUNCHERS)
    def test_every_pinning_launcher_writes_a_durable_pin_record(self, launcher: str) -> None:
        """Leg B must be wired into every pinning launcher, or the dead-VM window stays open."""
        source = (_VM_SCRIPTS / launcher).read_text()

        assert "lc_write_tarball_pin_record" in source, f"{launcher} pins a tarball but records nothing durable"

    @pytest.mark.parametrize("launcher", ["launch-mdps-backfill-vm.sh", "launch-features-vm.sh"])
    def test_service_launchers_pin_uac_into_metadata_and_record(self, launcher: str) -> None:
        """P0 mdps_vm_stale_uac_contract_propagation_2026_07_20: service VMs must
        version-pin UAC (the contract package) exactly like UTL/MDPS — into VM metadata
        AND the durable pin record — resolving it via lc_resolve_tarball_sha when the
        operator did not pin it explicitly. A floating UAC is how a stale contract
        reached a service VM despite a correct current tarball."""
        source = (_VM_SCRIPTS / launcher).read_text()

        assert "lc_resolve_tarball_sha" in source, f"{launcher} does not auto-resolve the current UAC tarball sha"
        assert "UAC_TARBALL_SHA=${UAC_TARBALL_SHA}" in source, (
            f"{launcher} does not stamp UAC_TARBALL_SHA into VM metadata"
        )
        assert '"UAC_TARBALL_SHA=${UAC_TARBALL_SHA:-}"' in source, (
            f"{launcher} does not record UAC in the durable pin record"
        )

    @pytest.mark.parametrize("launcher", PINNING_LAUNCHERS)
    def test_launcher_pin_keys_are_all_known_to_the_extractor(self, launcher: str) -> None:
        """Lockstep guard: a key a launcher sets but this module ignores = an unprotected live pin."""
        source = (_VM_SCRIPTS / launcher).read_text()
        # Every `<PREFIX>_TARBALL_SHA` token appearing anywhere in the launcher. `;` is also a
        # valid pair-separator here (2026-07-24): a launcher whose --metadata carries a
        # comma-bearing value (e.g. a comma-separated --protocols list) must join ALL its
        # metadata pairs with `;` + gcloud's `^;^` alternate-delimiter syntax instead of `,` —
        # gcloud requires ONE consistent delimiter per --metadata argument, so once any pair
        # needs `;`, every pair in that string does, including the TARBALL_SHA ones.
        tokens = {
            word.strip("\"'${}(),;")
            for line in source.splitlines()
            for word in line.replace(",", " ").replace(";", " ").replace("=", " ").split()
            if "_TARBALL_SHA" in word
        }
        keys = {t.replace("_PIN", "") for t in tokens if t.replace("_PIN", "").endswith("_TARBALL_SHA")}
        unknown = {k for k in keys if k not in TARBALL_SHA_ENV_TO_NAME}

        assert not unknown, f"{launcher} references pin key(s) the extractor does not map: {sorted(unknown)}"


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


# ---------------------------------------------------------------------------
# Leg A — live instance metadata
# ---------------------------------------------------------------------------


class TestPinsFromInstances:
    def test_reads_pins_out_of_running_instance_metadata(self) -> None:
        """Leg A: day-one protection for already-running VMs, no launcher rollout."""
        pins, unknown = pins_from_instances(
            [_running("canonical-migration-cefi-1", {"UAC_TARBALL_SHA": "acd8714c", "VM_TASK": "migration"})]
        )

        assert pins == {TarballPin("unified-api-contracts-code", "acd8714c")}
        assert not unknown

    def test_non_running_instances_are_ignored(self) -> None:
        row = _running("canonical-migration-cefi-1", {"UAC_TARBALL_SHA": "acd8714c"})
        row["status"] = "TERMINATED"

        pins, unknown = pins_from_instances([row])

        assert pins == set()
        assert not unknown

    def test_empty_metadata_is_a_definite_answer_not_a_gap(self) -> None:
        pins, unknown = pins_from_instances([_running("canonical-migration-cefi-1", {})])

        assert pins == set()
        assert not unknown, "'{}' means 'pinned nothing' — an answer, not a blind spot"

    def test_absent_metadata_key_is_unknown(self) -> None:
        """The distinction the whole fail-closed gate rests on."""
        pins, unknown = pins_from_instances([_running("canonical-migration-cefi-1", None)])

        assert pins == set()
        assert unknown == {"canonical-migration-cefi-1"}


# ---------------------------------------------------------------------------
# Leg B — the durable registry + grace/TTL
# ---------------------------------------------------------------------------


class TestRecentlyRecordedVmNames:
    def test_includes_recent_excludes_stale(self) -> None:
        client = _FakeStorageClient(
            objects={
                "vm-logs/vm-fresh/TARBALL_PINS.json": "{}",
                "vm-logs/vm-stale/TARBALL_PINS.json": "{}",
                "vm-logs/vm-fresh/run.log": "noise",
            },
            mtimes={
                "vm-logs/vm-fresh/TARBALL_PINS.json": _iso(2),
                "vm-logs/vm-stale/TARBALL_PINS.json": _iso(90),
            },
        )
        assert recently_recorded_vm_names(client, "bucket", grace_days=14) == {"vm-fresh"}

    def test_ttl_bounds_the_registry_so_it_cannot_grow_unbounded(self) -> None:
        """Past the grace window a record stops being honoured — the TTL."""
        client = _FakeStorageClient(
            objects={"vm-logs/ancient/TARBALL_PINS.json": "{}"},
            mtimes={"vm-logs/ancient/TARBALL_PINS.json": _iso(15)},
        )
        assert recently_recorded_vm_names(client, "bucket", grace_days=14) == set()

    def test_unparseable_mtime_is_included_protection_is_the_safe_default(self) -> None:
        client = _FakeStorageClient(objects={"vm-logs/vm-x/TARBALL_PINS.json": "{}"}, mtimes={})
        assert recently_recorded_vm_names(client, "bucket", grace_days=14) == {"vm-x"}


class TestCollectInUsePins:
    def test_protects_pins_of_a_deleted_but_relaunchable_vm(self) -> None:
        """THE incident's core gap.

        A SPOT-preempted / self-deleted VM is absent from the RUNNING instance
        list, yet RelaunchPreemptedVm still needs its pinned tarball. Leg A alone
        (instance metadata) cannot see it — the instance no longer exists.
        """
        client = _FakeStorageClient(
            objects={"vm-logs/gone-vm/TARBALL_PINS.json": _record(UAC_TARBALL_SHA="acd8714c")},
            mtimes={"vm-logs/gone-vm/TARBALL_PINS.json": _iso(3)},
        )

        pins = collect_in_use_pins(client, "bucket", running_instances=[])

        assert TarballPin("unified-api-contracts-code", "acd8714c") in pins

    def test_union_of_both_legs(self) -> None:
        """Neither leg alone closes the incident; the protected set is the union."""
        client = _FakeStorageClient(
            objects={"vm-logs/gone-vm/TARBALL_PINS.json": _record(UAC_TARBALL_SHA="from-registry")},
            mtimes={"vm-logs/gone-vm/TARBALL_PINS.json": _iso(1)},
        )
        running = [_running("mdps-backfill-cefi-1", {"MDPS_TARBALL_SHA": "from-metadata"})]

        pins = collect_in_use_pins(client, "bucket", running_instances=running)

        assert TarballPin("unified-api-contracts-code", "from-registry") in pins, "Leg B"
        assert TarballPin("market-data-processing-service-code", "from-metadata") in pins, "Leg A"

    def test_stale_registry_entry_is_dropped(self) -> None:
        client = _FakeStorageClient(
            objects={"vm-logs/ancient/TARBALL_PINS.json": _record(UAC_TARBALL_SHA="uac999")},
            mtimes={"vm-logs/ancient/TARBALL_PINS.json": _iso(400)},
        )

        pins = collect_in_use_pins(client, "bucket", running_instances=[], grace_days=14)

        assert TarballPin("unified-api-contracts-code", "uac999") not in pins


# ---------------------------------------------------------------------------
# (d) FAIL-CLOSED
# ---------------------------------------------------------------------------


class TestFailClosed:
    def test_registry_listing_failure_raises_never_returns_empty(self) -> None:
        """'I could not read' must never collapse into 'nothing is pinned'."""
        client = _ExplodingListStorageClient({})

        with pytest.raises(InUsePinsUnavailableError, match="listing vm-logs/"):
            _ = collect_in_use_pins(client, "bucket", running_instances=[])

    def test_running_pinning_vm_with_no_observable_record_raises(self) -> None:
        """A RUNNING migration VM we can see NOTHING about blocks all deletion."""
        client = _FakeStorageClient({})
        running = [_running("canonical-migration-cefi-1", None)]  # metadata not reported

        with pytest.raises(InUsePinsUnavailableError, match="canonical-migration-cefi-1"):
            _ = collect_in_use_pins(client, "bucket", running_instances=running)

    def test_proven_floating_vm_does_not_block_retention_forever(self) -> None:
        """The escape hatch that keeps fail-closed affordable.

        A VM whose metadata IS readable and simply carries no pin is a definite
        answer. Without this, one deliberately-floating VM would wedge the sweep
        permanently and the fix would be reverted within a week.
        """
        client = _FakeStorageClient({})
        running = [_running("canonical-migration-cefi-1", {"VM_TASK": "canonical-migration"})]

        assert collect_in_use_pins(client, "bucket", running_instances=running) == set()

    def test_explicit_floating_declaration_also_covers_the_vm(self) -> None:
        vm = "canonical-migration-cefi-1"
        client = _FakeStorageClient(
            objects={
                f"vm-logs/{vm}/TARBALL_PINS.json": json.dumps(
                    {"launcher": "x", "pins": {}, "floating": ["UAC_TARBALL_SHA"]}
                )
            },
            mtimes={f"vm-logs/{vm}/TARBALL_PINS.json": _iso(1)},
        )

        assert collect_in_use_pins(client, "bucket", running_instances=[_running(vm, None)]) == set()

    @pytest.mark.parametrize(
        ("vm_name", "override"),
        [
            ("my-thing", "launch-mdps-backfill-vm.sh --vm-name my-thing"),
            ("harshs-oneoff-migration", "VM_NAME=... launch-mtds-dex-swaps-backfill-vm.sh"),
        ],
    )
    def test_pinning_vm_renamed_off_the_prefix_list_still_blocks_deletion(self, vm_name: str, override: str) -> None:
        """THE residual hole in the first fix — a name-prefix gate is evadable.

        The original gate only fired for VM names matching a hard-coded prefix
        tuple derived from the five pinning launchers' DEFAULT name shapes. But
        those launchers ship operator-facing name overrides —
        `launch-mdps-backfill-vm.sh --vm-name` (scripts/vm/launch-mdps-backfill-vm.sh:115,
        applied at :184) and `VM_NAME=` (launch-mtds-dex-swaps-backfill-vm.sh:74) —
        so a genuinely pinning VM runs under ANY name the operator chooses.

        Compose that with the transition window we are in right now:
          * the jobs image still carries the pre-fix UTL, so the instance row has
            NO `metadata` key at all (Leg A blind), and
          * the VM predates `lc_write_tarball_pin_record`, so there is no
            TARBALL_PINS.json (Leg B blind).

        Under the prefix gate this VM was neither SEEN nor GATED: pins came back
        as an empty set, nothing raised, and the sweep deleted its live pin —
        the original incident reached by a second route. The verifier reproduced
        exactly this as "NO raise, pins=set()".

        The gate now keys on OBSERVABILITY, not on the name, so the override
        cannot slip past.
        """
        client = _FakeStorageClient({})  # Leg B blind: no TARBALL_PINS.json
        running = [_running(vm_name, None)]  # Leg A blind: old UTL, no `metadata` key

        with pytest.raises(InUsePinsUnavailableError) as excinfo:
            _ = collect_in_use_pins(client, "bucket", running_instances=running)

        assert vm_name in str(excinfo.value), f"a VM launched via `{override}` must not slip past the fail-closed gate"

    def test_unobservable_vm_blocks_regardless_of_how_ordinary_its_name_looks(self) -> None:
        """No name is safe to assume unpinned — not even the orchestrator's.

        Deliberately inverts the previous `test_non_pinning_vm_without_metadata_
        does_not_block`. That test encoded the assumption the hole rested on:
        that a VM's NAME proves it holds no pin. It does not, and asserting it
        did is what let the prefix gate look correct. A VM we cannot observe at
        all is a blind spot whatever it is called; only an OBSERVATION clears it
        (see the two tests below, which are what keep this affordable).
        """
        client = _FakeStorageClient({})

        with pytest.raises(InUsePinsUnavailableError, match="planning"):
            _ = collect_in_use_pins(client, "bucket", running_instances=[_running("planning", None)])

    def test_observed_empty_metadata_clears_any_name_without_a_record(self) -> None:
        """The distinction that stops the widened gate becoming a permanent wedge.

        `metadata == {}` is an OBSERVATION — "this VM pinned nothing" — and is
        categorically different from a row carrying no `metadata` key, which is
        "the transport never told us". Collapsing the two in either direction is
        the bug: one way reaps live pins, the other way blocks retention forever.
        Once the metadata-carrying UTL reaches the jobs image, every running VM
        lands on this path and the sweep resumes on its own.
        """
        client = _FakeStorageClient({})
        running = [_running("my-thing", {}), _running("planning", {"VM_TASK": "orchestrator"})]

        assert collect_in_use_pins(client, "bucket", running_instances=running) == set()


class TestCollectPinsForVm:
    def test_reads_the_durable_registry_for_one_vm(self) -> None:
        client = _FakeStorageClient({"vm-logs/vm-a/TARBALL_PINS.json": _record(MDPS_TARBALL_SHA="mdps1")})

        assert collect_pins_for_vm(client, "bucket", "vm-a") == {
            TarballPin("market-data-processing-service-code", "mdps1")
        }

    def test_absent_record_is_empty_not_an_error(self) -> None:
        assert collect_pins_for_vm(_FakeStorageClient({}), "bucket", "vm-a") == set()


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


class TestFindOrphanManifests:
    def test_manifest_without_its_tarball_is_an_orphan(self) -> None:
        client = _FakeStorageClient(
            {
                "code/uac-code@dead.manifest.json": "{}",
                "code/uac-code@live.tar.gz": "x",
                "code/uac-code@live.manifest.json": "{}",
            }
        )

        assert find_orphan_manifests(client, "bucket") == ["code/uac-code@dead.manifest.json"]

    def test_floating_manifest_is_never_called_an_orphan(self) -> None:
        """`<svc>-code.manifest.json` legitimately outlives a rebuild gap."""
        client = _FakeStorageClient({"code/uac-code.manifest.json": "{}"})

        assert find_orphan_manifests(client, "bucket") == []


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
        client = self._client(
            objects={
                "code/uac-code@newer.tar.gz": "x",  # no manifest — unverifiable provenance
                "code/uac-code@older.tar.gz": "x",
                "code/uac-code@older.manifest.json": "{}",
            },
            mtimes={"code/uac-code@newer.tar.gz": _iso(1), "code/uac-code@older.tar.gz": _iso(10)},
        )

        assert resolve_available_pin(client, "bucket", "uac-code", "gone") == "older"

    def test_no_complete_pair_returns_none_so_the_caller_pages(self) -> None:
        """Never the floating tarball — that is the silent-degrade trap."""
        client = self._client({"code/uac-code.tar.gz": "x", "code/uac-code.manifest.json": "{}"})

        assert resolve_available_pin(client, "bucket", "uac-code", "gone") is None
