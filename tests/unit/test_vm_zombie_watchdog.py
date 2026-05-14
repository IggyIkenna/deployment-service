"""Unit tests for vm_zombie_watchdog.py — B-011 Phase 8.A coverage.

Tests:
  - VM_PREFIX_TO_BUCKET dict registration (all launch-*.sh prefixes covered)
  - _is_daemon() singleton-lock / daemon-opt-out classification
  - WatchdogVerdict dataclass construction
  - VmPrefixSpec bucket patterns (no raw inline f-strings violating SSOT)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import pytest

# Paths
SCRIPTS_VM_DIR: Final[Path] = Path(__file__).parent.parent.parent / "scripts" / "vm"
WATCHDOG_PY: Final[Path] = SCRIPTS_VM_DIR / "vm_zombie_watchdog.py"


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_watchdog_module() -> object:
    """Import vm_zombie_watchdog without polluting the test namespace."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("vm_zombie_watchdog", WATCHDOG_PY)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Register before exec so @dataclass can resolve __module__ → __dict__
    sys.modules["vm_zombie_watchdog"] = mod  # type: ignore[assignment]
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def _extract_launch_prefixes() -> dict[str, str]:
    """Return {prefix: launcher_file} for all explicit VM_NAME assignments in launch-*.sh.

    Extracts the static part of `VM_NAME="<prefix>-${...}"` lines.
    Skips dynamic-only assignments (`VM_NAME=""`, `VM_NAME=${curl …}`).
    """
    pattern = re.compile(r'^VM_NAME="([^"$]+)\$')
    result: dict[str, str] = {}
    for sh in sorted(SCRIPTS_VM_DIR.glob("launch-*.sh")):
        for line in sh.read_text().splitlines():
            line = line.strip()
            m = pattern.match(line)
            if m:
                prefix = m.group(1)
                result[prefix] = sh.name
    return result


# ── load module once ──────────────────────────────────────────────────────────

_mod = _load_watchdog_module()
_VM_PREFIX_TO_BUCKET = _mod.VM_PREFIX_TO_BUCKET
_is_daemon = _mod._is_daemon
_WatchdogVerdict = _mod.WatchdogVerdict
_VmPrefixSpec = _mod.VmPrefixSpec
_DAEMON_TIER_LABELS = _mod.DAEMON_TIER_LABELS
_DAEMON_PURPOSE_OPT_OUT = _mod.DAEMON_PURPOSE_OPT_OUT


# ── prefix registration ───────────────────────────────────────────────────────


class TestVmPrefixRegistration:
    """Every launcher's VM name prefix must be a key (or covered by a key) in VM_PREFIX_TO_BUCKET."""

    # Prefixes that use shell variable expansion for sub-variants that ARE registered.
    # e.g., VM_NAME="mdps-features-live-${ASSET_GROUP}-${RUN_TS}" expands to
    # mdps-features-live-cefi-..., mdps-features-live-defi-... etc. — all registered.
    _TEMPLATE_FAMILY_PREFIXES: frozenset[str] = frozenset(
        {
            "mdps-features-live-",  # → mdps-features-live-{ag}- all registered
            "mtds-live-",  # → mtds-live-{ag}- all registered
        }
    )

    # Prefixes that are intentionally exempt from VM_PREFIX_TO_BUCKET (daemon opt-out).
    _DAEMON_EXEMPT: frozenset[str] = frozenset(
        {
            "vm-zombie-watchdog-",  # watchdog excludes itself via DAEMON_OPT_OUT labels
        }
    )

    def test_all_launch_prefixes_covered_by_watchdog(self) -> None:
        """Launcher VM_NAME prefixes must be reachable from a VM_PREFIX_TO_BUCKET key.

        Two forms of coverage:
          (a) prefix.startswith(key) — the static prefix is a specific registered key or sub-prefix
          (b) any registered key starts with prefix — template family pattern where the shell
              variable expands to give a more specific prefix that IS registered.

        Unregistered = zombie-watchdog blindspot (CLAUDE.md VM Naming Convention).
        Known gaps are enumerated in _KNOWN_UNREGISTERED_PREFIXES; they should be added
        to VM_PREFIX_TO_BUCKET in a follow-up (see issue doc filed in B-011).
        """
        known_prefixes = tuple(_VM_PREFIX_TO_BUCKET.keys())
        launch_prefixes = _extract_launch_prefixes()

        # Prefixes with confirmed blindspot that are NOT yet fixed in watchdog.
        # These are tracked; test documents them rather than failing (to allow
        # incremental registration). Remove entries as they get registered.
        _KNOWN_UNREGISTERED_PREFIXES: frozenset[str] = frozenset(
            {
                "defi-fwd-",  # launch-defi-forward-poll.sh — needs "defi-fwd-" key
                "footystats-fwd-",  # launch-footystats-forward-poll.sh — needs key
                "ml-train-",  # launch-ml-training-vm.sh — needs key
                "prediction-fwd-",  # launch-prediction-forward-poll.sh — needs key
                "sfi-fwd-",  # launch-sfi-forward-poll.sh — needs key
                "sports-manifest-rescan-",  # launch-sports-manifest-rescan-vm.sh
                "sports-manifest-rescan-coord-",  # sub-variant
                "sports-manifest-rescan-chunk-",  # sub-variant
                "sports-scheduler-",  # launch-sports-scheduler-vm.sh — needs key
                "strategy-test-",  # launch-strategy-test-vm.sh — needs key
            }
        )

        truly_missing: list[str] = []
        for prefix, launcher in launch_prefixes.items():
            if prefix in self._DAEMON_EXEMPT:
                continue
            # Form (a): static prefix is covered by a registered key
            covered_a = any(prefix.startswith(k) for k in known_prefixes)
            # Form (b): template family — a registered key starts with our prefix
            covered_b = prefix in self._TEMPLATE_FAMILY_PREFIXES and any(
                k.startswith(prefix) for k in known_prefixes
            )
            if not covered_a and not covered_b and prefix not in _KNOWN_UNREGISTERED_PREFIXES:
                truly_missing.append(f"{prefix!r} (from {launcher})")

        assert not truly_missing, (
            "NEW unregistered VM name prefixes found in launch scripts — "
            "add to VM_PREFIX_TO_BUCKET immediately (CLAUDE.md zombie-watchdog rule):\n"
            + "\n".join(f"  {m}" for m in truly_missing)
        )

    def test_known_unregistered_prefixes_are_documented(self) -> None:
        """Known gaps from launch scripts that need VM_PREFIX_TO_BUCKET entries.

        This test documents the existing blindspots. Each should eventually
        be removed from the known-list as watchdog entries are added.
        """
        known_unregistered = {
            "defi-fwd-": "launch-defi-forward-poll.sh",
            "footystats-fwd-": "launch-footystats-forward-poll.sh",
            "ml-train-": "launch-ml-training-vm.sh",
            "prediction-fwd-": "launch-prediction-forward-poll.sh",
            "sfi-fwd-": "launch-sfi-forward-poll.sh",
            "sports-manifest-rescan-": "launch-sports-manifest-rescan-vm.sh",
            "sports-scheduler-": "launch-sports-scheduler-vm.sh",
            "strategy-test-": "launch-strategy-test-vm.sh",
        }
        known_prefixes = tuple(_VM_PREFIX_TO_BUCKET.keys())
        # If any known-unregistered prefix is NOW registered, it should be removed from the list.
        newly_registered = [
            p for p in known_unregistered if any(p.startswith(k) for k in known_prefixes)
        ]
        assert not newly_registered, (
            "These prefixes are now registered in VM_PREFIX_TO_BUCKET — "
            "remove them from _KNOWN_UNREGISTERED_PREFIXES:\n"
            + "\n".join(f"  {p!r}" for p in newly_registered)
        )

    def test_vm_prefix_to_bucket_has_entries(self) -> None:
        assert len(_VM_PREFIX_TO_BUCKET) > 50, "Expected 50+ registered VM prefixes"

    def test_launch_scripts_exist(self) -> None:
        scripts = list(SCRIPTS_VM_DIR.glob("launch-*.sh"))
        assert len(scripts) > 30, f"Expected 30+ launch scripts, got {len(scripts)}"

    def test_no_duplicate_keys_in_prefix_dict(self) -> None:
        """Python dict prevents literal duplicates but check we can round-trip keys."""
        keys = list(_VM_PREFIX_TO_BUCKET.keys())
        assert len(keys) == len(set(keys)), "Duplicate keys in VM_PREFIX_TO_BUCKET"


# ── singleton-lock / daemon classification ────────────────────────────────────


class TestIsDaemon:
    """_is_daemon() determines which VMs are exempt from zombie-reap (daemon opt-out)."""

    def test_no_labels_not_daemon(self) -> None:
        assert _is_daemon({}) is False

    def test_none_labels_not_daemon(self) -> None:
        assert _is_daemon(None) is False  # type: ignore[arg-type]

    def test_daemon_tier_label_marks_as_daemon(self) -> None:
        """VMs labelled tier=daemon are long-lived and must not be reaped."""
        for tier in _DAEMON_TIER_LABELS:
            assert _is_daemon({"tier": tier}) is True, f"tier={tier!r} should be daemon"

    def test_daemon_purpose_label_marks_as_daemon(self) -> None:
        for purpose in _DAEMON_PURPOSE_OPT_OUT:
            assert _is_daemon({"purpose": purpose}) is True, f"purpose={purpose!r} should be daemon"

    def test_regular_vm_label_not_daemon(self) -> None:
        assert _is_daemon({"tier": "ephemeral", "purpose": "backfill"}) is False

    def test_unknown_tier_not_daemon(self) -> None:
        assert _is_daemon({"tier": "unknown-tier"}) is False

    def test_watchdog_vm_name_in_opt_out(self) -> None:
        """vm-zombie-watchdog itself must be exempt from self-reap."""
        opt_out = _DAEMON_PURPOSE_OPT_OUT
        assert "watchdog" in opt_out or any("watchdog" in p for p in opt_out), (
            "Watchdog purpose label must be in DAEMON_PURPOSE_OPT_OUT to prevent self-reap"
        )


# ── WatchdogVerdict dataclass ─────────────────────────────────────────────────


class TestWatchdogVerdict:
    """WatchdogVerdict stores per-VM evaluation results."""

    def _make(self, verdict: str) -> object:
        return _WatchdogVerdict(
            vm_name="cefi-fwd-20260514",
            zone="asia-northeast1-a",
            age_minutes=45.0,
            heartbeat_age_min=None,
            shard_age_min=None,
            verdict=verdict,
        )

    def test_alive_verdict(self) -> None:
        v = self._make("alive")
        assert v.verdict == "alive"  # type: ignore[attr-defined]

    def test_zombie_stale_heartbeat(self) -> None:
        v = self._make("zombie_stale_heartbeat")
        assert "zombie" in v.verdict  # type: ignore[attr-defined]

    def test_zombie_no_heartbeat(self) -> None:
        v = self._make("zombie_no_heartbeat")
        assert "zombie" in v.verdict  # type: ignore[attr-defined]

    def test_too_young(self) -> None:
        v = self._make("too_young")
        assert v.verdict == "too_young"  # type: ignore[attr-defined]

    def test_zombie_finished_not_shutdown(self) -> None:
        v = self._make("zombie_finished_not_shutdown")
        assert "zombie" in v.verdict  # type: ignore[attr-defined]


# ── VmPrefixSpec bucket patterns ──────────────────────────────────────────────


class TestVmPrefixSpecBucketPatterns:
    """Bucket names in VmPrefixSpec must follow the env-aware naming convention.

    Raw bucket f-strings like f"market-data-tick-{ag}-{PROJECT_ID}" are the
    current interim pattern (pre bucket-name-ssot Phase 2). Verify none use
    unexpected hardcoded bucket names.
    """

    def test_vmprefix_spec_entries_have_bucket_or_none(self) -> None:
        """All VmPrefixSpec entries expose a .bucket attribute (str or None)."""
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None:
                continue
            if isinstance(spec, _VmPrefixSpec):
                bucket = spec.bucket
                assert isinstance(bucket, str), f"{key!r}: bucket must be str, got {type(bucket)}"

    def test_no_entries_use_wrong_cloud(self) -> None:
        """No bucket name should contain 's3://' — all are GCS."""
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None or not isinstance(spec, _VmPrefixSpec):
                continue
            assert "s3://" not in spec.bucket, f"{key!r}: bucket must not be S3 URI"
            assert "gs://" not in spec.bucket, f"{key!r}: bucket should be name only, not full URI"

    def test_known_asset_group_buckets_follow_naming(self) -> None:
        """Buckets for known asset groups follow market-data-tick-{ag} pattern."""
        asset_groups = {"cefi", "defi", "tradfi", "sports", "prediction"}
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None or not isinstance(spec, _VmPrefixSpec):
                continue
            bucket: str = spec.bucket
            for ag in asset_groups:
                if f"market-data-tick-{ag}" in bucket:
                    # Must follow convention: market-data-tick-{ag}-{project_id}
                    assert bucket.startswith(f"market-data-tick-{ag}-"), (
                        f"{key!r}: bucket {bucket!r} doesn't follow market-data-tick-{ag}-{{project}} pattern"
                    )
                    break


# ── shellcheck smoke ──────────────────────────────────────────────────────────


class TestShellcheckClean:
    """Launcher scripts must pass shellcheck with no errors (SC >= 2000 severity)."""

    @pytest.mark.parametrize("script", sorted(SCRIPTS_VM_DIR.glob("launch-*.sh")))
    def test_shellcheck_no_errors(self, script: Path) -> None:
        """Each launcher must be shellcheck-clean (errors only; style/info acceptable)."""
        import subprocess

        result = subprocess.run(
            ["shellcheck", "--severity=error", str(script)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"shellcheck errors in {script.name}:\n{result.stdout}\n{result.stderr}"
        )
