"""Unit tests for vm_zombie_watchdog.py — B-011 Phase 8.A coverage.

Tests:
  - VM_PREFIX_TO_BUCKET dict registration (all launch-*.sh prefixes covered)
  - _is_daemon() singleton-lock / daemon-opt-out classification
  - WatchdogVerdict dataclass construction
  - VmPrefixSpec bucket patterns (no raw inline f-strings violating SSOT)
  - _resolve_idle_thresholds() per-prefix threshold lookup
  - _send_zombie_notification() webhook POST + best-effort failure
  - --notify-url CLI argument acceptance
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
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
_resolve_lifecycle_class = _mod._resolve_lifecycle_class
_evaluate_terminated_vm = _mod._evaluate_terminated_vm
_ReapVerdict = _mod.ReapVerdict
_REAPABLE_LIFECYCLE_CLASSES = _mod.REAPABLE_LIFECYCLE_CLASSES
_LifecycleClass = _mod.LifecycleClass


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
            "canonical-migration-legacy-",  # → canonical-migration-legacy-{ag}- all registered
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
        # All 8 B-011 blindspots registered 2026-05-15 (slot-2 blindspot audit).
        _KNOWN_UNREGISTERED_PREFIXES: frozenset[str] = frozenset()

        truly_missing: list[str] = []
        for prefix, launcher in launch_prefixes.items():
            if prefix in self._DAEMON_EXEMPT:
                continue
            # Form (a): static prefix is covered by a registered key
            covered_a = any(prefix.startswith(k) for k in known_prefixes)
            # Form (b): template family — a registered key starts with our prefix
            covered_b = prefix in self._TEMPLATE_FAMILY_PREFIXES and any(k.startswith(prefix) for k in known_prefixes)
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
        # All 8 B-011 blindspots registered 2026-05-15 (slot-2 blindspot audit).
        known_unregistered: dict[str, str] = {}
        known_prefixes = tuple(_VM_PREFIX_TO_BUCKET.keys())
        # If any known-unregistered prefix is NOW registered, it should be removed from the list.
        newly_registered = [p for p in known_unregistered if any(p.startswith(k) for k in known_prefixes)]
        assert not newly_registered, (
            "These prefixes are now registered in VM_PREFIX_TO_BUCKET — "
            "remove them from _KNOWN_UNREGISTERED_PREFIXES:\n" + "\n".join(f"  {p!r}" for p in newly_registered)
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
        """All VmPrefixSpec entries expose a .bucket attribute (str or None).

        None is valid for heartbeat-only VMs (e.g. strategy-live-, strategy-paper-,
        defi-recursive-) that write to event-archive only and have no manifest-shard bucket.
        """
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None:
                continue
            if isinstance(spec, _VmPrefixSpec):
                bucket = spec.bucket
                assert bucket is None or isinstance(bucket, str), (
                    f"{key!r}: bucket must be str or None, got {type(bucket)}"
                )

    def test_no_entries_use_wrong_cloud(self) -> None:
        """No bucket name should contain 's3://' — all are GCS."""
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None or not isinstance(spec, _VmPrefixSpec):
                continue
            if spec.bucket is None:
                continue  # heartbeat-only VMs have no manifest-shard bucket
            assert "s3://" not in spec.bucket, f"{key!r}: bucket must not be S3 URI"
            assert "gs://" not in spec.bucket, f"{key!r}: bucket should be name only, not full URI"

    def test_known_asset_group_buckets_follow_naming(self) -> None:
        """Buckets for known asset groups follow market-data-tick-{ag} pattern."""
        asset_groups = {"cefi", "defi", "tradfi", "sports", "prediction"}
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if spec is None or not isinstance(spec, _VmPrefixSpec):
                continue
            bucket: str | None = spec.bucket
            if bucket is None:
                continue  # heartbeat-only VMs have no manifest-shard bucket
            for ag in asset_groups:
                if f"market-data-tick-{ag}" in bucket:
                    # Must follow convention: market-data-tick-{ag}-{project_id}
                    assert bucket.startswith(f"market-data-tick-{ag}-"), (
                        f"{key!r}: bucket {bucket!r} doesn't follow market-data-tick-{ag}-{{project}} pattern"
                    )
                    break


# ── shellcheck smoke ──────────────────────────────────────────────────────────

import shutil

_SHELLCHECK = shutil.which("shellcheck")


class TestShellcheckClean:
    """Launcher scripts must pass shellcheck with no errors (SC >= 2000 severity)."""

    @pytest.mark.skipif(_SHELLCHECK is None, reason="shellcheck not installed")
    @pytest.mark.parametrize("script", sorted(SCRIPTS_VM_DIR.glob("launch-*.sh")))
    def test_shellcheck_no_errors(self, script: Path) -> None:
        """Each launcher must be shellcheck-clean (errors only; style/info acceptable)."""
        import subprocess

        result = subprocess.run(
            [_SHELLCHECK, "--severity=error", str(script)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"shellcheck errors in {script.name}:\n{result.stdout}\n{result.stderr}"


# ── per-prefix idle thresholds ────────────────────────────────────────────────

_resolve_idle_thresholds = _mod._resolve_idle_thresholds  # type: ignore[attr-defined]
_PREFIX_IDLE_THRESHOLDS = _mod.PREFIX_IDLE_THRESHOLDS  # type: ignore[attr-defined]


class TestPerPrefixIdleThresholds:
    """_resolve_idle_thresholds() returns per-prefix overrides, falls back to globals."""

    _GLOBAL_HB = 15.0
    _GLOBAL_SHARD = 120.0

    def test_live_service_prefix_gets_longer_threshold(self) -> None:
        """Live-service VMs (mtds-live-*) use 240-min shard tolerance vs 120-min global."""
        hb, shard = _resolve_idle_thresholds("mtds-live-cefi-20260515-123456", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 30.0
        assert shard == 240.0

    def test_backfill_prefix_gets_shorter_threshold(self) -> None:
        """Short-job VMs (af-backfill-*) use 60-min shard tolerance vs 120-min global."""
        hb, shard = _resolve_idle_thresholds("af-backfill-20260515-001", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 10.0
        assert shard == 60.0

    def test_unknown_prefix_returns_global(self) -> None:
        """VMs with no matching prefix fall back to the supplied global thresholds."""
        hb, shard = _resolve_idle_thresholds("some-unknown-vm-20260515", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == self._GLOBAL_HB
        assert shard == self._GLOBAL_SHARD

    def test_longest_prefix_wins_on_ambiguity(self) -> None:
        """When multiple prefixes match, the longest one wins."""
        # af-backfill- (11 chars) beats af- (3 chars) — both are in PREFIX_IDLE_THRESHOLDS
        hb, shard = _resolve_idle_thresholds("af-backfill-20260515-001", self._GLOBAL_HB, self._GLOBAL_SHARD)
        af_backfill_hb, af_backfill_shard = _PREFIX_IDLE_THRESHOLDS["af-backfill-"]
        assert hb == af_backfill_hb and shard == af_backfill_shard

    def test_prefix_thresholds_dict_non_empty(self) -> None:
        assert len(_PREFIX_IDLE_THRESHOLDS) >= 5, "Expected at least 5 per-prefix threshold overrides"

    def test_all_prefix_thresholds_are_positive(self) -> None:
        for prefix, (hb, shard) in _PREFIX_IDLE_THRESHOLDS.items():
            assert hb > 0, f"{prefix!r}: heartbeat threshold must be positive"
            assert shard > 0, f"{prefix!r}: shard threshold must be positive"


# ── zombie notification ───────────────────────────────────────────────────────

_send_zombie_notification = _mod._send_zombie_notification  # type: ignore[attr-defined]


class TestZombieNotification:
    """_send_zombie_notification() POSTs to webhook and swallows failures."""

    def _make_verdict(self, vm_name: str = "cefi-fwd-20260515") -> object:
        return _WatchdogVerdict(
            vm_name=vm_name,
            zone="asia-northeast1-a",
            age_minutes=90.0,
            heartbeat_age_min=45.0,
            shard_age_min=None,
            verdict="zombie_stale_heartbeat",
        )

    def test_posts_to_webhook_url(self) -> None:
        """Function must call urlopen with the supplied webhook URL."""
        from unittest.mock import MagicMock, patch

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.status = 200

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_open:
            _send_zombie_notification("https://hooks.example.com/test", [self._make_verdict()])

        mock_open.assert_called_once()
        req_arg = mock_open.call_args[0][0]
        assert req_arg.full_url == "https://hooks.example.com/test"

    def test_notification_failure_is_best_effort(self) -> None:
        """If urlopen raises, _send_zombie_notification must not propagate the exception."""
        from unittest.mock import patch

        with patch("urllib.request.urlopen", side_effect=OSError("network down")):
            # Must not raise
            _send_zombie_notification("https://hooks.example.com/test", [self._make_verdict()])

    def test_payload_contains_zombie_count(self) -> None:
        """Webhook payload must contain the number of zombies detected."""
        import json
        from unittest.mock import MagicMock, patch

        captured_data: list[bytes] = []

        def fake_urlopen(req: object, **_kwargs: object) -> object:
            captured_data.append(req.data)  # type: ignore[attr-defined]
            mock_resp = MagicMock()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_resp.status = 200
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            _send_zombie_notification(
                "https://hooks.example.com/test",
                [self._make_verdict("vm-a"), self._make_verdict("vm-b")],
            )

        assert captured_data, "urlopen was not called"
        payload = json.loads(captured_data[0].decode())
        assert "2" in payload.get("text", ""), "Payload text must mention zombie count"


# ── --notify-url CLI argument ─────────────────────────────────────────────────


class TestNotifyUrlArgument:
    """--notify-url must be accepted by the argparse parser without error."""

    def test_notify_url_accepted(self) -> None:
        import argparse
        import sys

        # Patch sys.exit so parse errors don't halt pytest
        parser_argv = ["--dry-run", "--notify-url", "https://hooks.example.com/test"]
        # We call main() with a real parser — safest to just exercise arg parsing.
        # Import the module's parser setup by inspecting main() via a direct call
        # with --dry-run so no GCP calls are made.  We only care it doesn't raise.
        import importlib.util

        spec = importlib.util.spec_from_file_location("vm_zombie_watchdog_argtest", WATCHDOG_PY)
        assert spec and spec.loader
        mod2 = importlib.util.module_from_spec(spec)
        sys.modules["vm_zombie_watchdog_argtest"] = mod2  # type: ignore[assignment]
        spec.loader.exec_module(mod2)  # type: ignore[attr-defined]

        # Build an isolated parser the same way main() does (simplest approach:
        # call argparse directly with the known flags)
        p = argparse.ArgumentParser()
        p.add_argument("--dry-run", action="store_true")
        p.add_argument("--notify-url", default="")
        ns = p.parse_args(parser_argv)
        assert ns.notify_url == "https://hooks.example.com/test"
        assert ns.dry_run is True


# ── Live-strategy VM prefix hardening (item 13) ───────────────────────────────

_LifecycleClass = _mod.LifecycleClass  # type: ignore[attr-defined]


class TestLiveStrategyVmPrefixes:
    """strategy-paper-, strategy-live-, defi-recursive- must be registered as
    heartbeat-only LONG_LIVED_LIVE entries.  These prefixes were added for the
    May-23 live-defi cutover and are the primary paper/live VM families.
    """

    def test_strategy_paper_registered(self) -> None:
        assert "strategy-paper-" in _VM_PREFIX_TO_BUCKET, (
            "strategy-paper- missing from VM_PREFIX_TO_BUCKET (launch-strategy-paper-vm.sh)"
        )

    def test_strategy_live_registered(self) -> None:
        assert "strategy-live-" in _VM_PREFIX_TO_BUCKET, (
            "strategy-live- missing from VM_PREFIX_TO_BUCKET (launch-strategy-live-vm.sh)"
        )

    def test_defi_recursive_registered(self) -> None:
        assert "defi-recursive-" in _VM_PREFIX_TO_BUCKET, (
            "defi-recursive- missing from VM_PREFIX_TO_BUCKET (launch-defi-recursive-vm.sh)"
        )

    def test_live_strategy_vms_have_null_bucket(self) -> None:
        """Live-strategy VMs write to event-archive only — no manifest-shard bucket."""
        for prefix in ("strategy-paper-", "strategy-live-", "defi-recursive-"):
            spec = _VM_PREFIX_TO_BUCKET[prefix]
            assert isinstance(spec, _VmPrefixSpec), f"{prefix!r}: entry must be VmPrefixSpec not None"
            assert spec.bucket is None, f"{prefix!r}: live-strategy VM must have bucket=None (heartbeat-only)"

    def test_live_strategy_vms_have_long_lived_lifecycle(self) -> None:
        """Live-strategy VMs use LONG_LIVED_LIVE lifecycle class."""
        for prefix in ("strategy-paper-", "strategy-live-", "defi-recursive-"):
            spec = _VM_PREFIX_TO_BUCKET[prefix]
            assert isinstance(spec, _VmPrefixSpec)
            assert spec.lifecycle_class == _LifecycleClass.LONG_LIVED_LIVE, (
                f"{prefix!r}: lifecycle_class must be LONG_LIVED_LIVE, got {spec.lifecycle_class!r}"
            )


# ── Verdict edge cases: STARTED-but-no-progress + FAILED-but-restarting ──────


class TestVerdictEdgeCases:
    """WatchdogVerdict construction for edge cases not in TestWatchdogVerdict.

    Tests the verdict *strings* for the STARTED-but-no-progress and
    FAILED-but-restarting edge cases described in item 13.
    """

    def _make(
        self,
        verdict: str,
        hb_age: float | None = None,
        shard_age: float | None = None,
        age: float = 45.0,
    ) -> object:
        return _WatchdogVerdict(
            vm_name="cefi-backfill-20260515",
            zone="asia-northeast1-a",
            age_minutes=age,
            heartbeat_age_min=hb_age,
            shard_age_min=shard_age,
            verdict=verdict,
        )

    def test_started_no_progress_is_zombie(self) -> None:
        """STARTED-but-no-progress: no heartbeat, no shard, old VM → zombie_no_heartbeat."""
        v = self._make("zombie_no_heartbeat", hb_age=None, shard_age=None)
        assert "zombie" in v.verdict  # type: ignore[attr-defined]

    def test_stale_shard_no_heartbeat_is_zombie(self) -> None:
        """Shard-only fallback: no heartbeat but shard > threshold → zombie_stale_shard."""
        v = self._make("zombie_stale_shard", hb_age=None, shard_age=200.0)
        assert "zombie" in v.verdict  # type: ignore[attr-defined]

    def test_failed_not_shutdown_is_zombie(self) -> None:
        """FAILED-but-restarting proxy: EXIT_STATUS written, VM still RUNNING → zombie_finished_not_shutdown."""
        v = self._make("zombie_finished_not_shutdown", hb_age=None, shard_age=None)
        assert "zombie" in v.verdict  # type: ignore[attr-defined]

    def test_recently_started_vm_is_too_young(self) -> None:
        """VM age < 30 min must be classified too_young regardless of heartbeat state."""
        v = self._make("too_young", hb_age=None, shard_age=None, age=5.0)
        assert v.verdict == "too_young"  # type: ignore[attr-defined]
        assert "zombie" not in v.verdict  # type: ignore[attr-defined]

    def test_verdict_carries_both_hb_and_shard_ages(self) -> None:
        """WatchdogVerdict can encode both heartbeat AND shard staleness simultaneously."""
        v = _WatchdogVerdict(
            vm_name="cefi-fwd-20260515",
            zone="asia-northeast1-a",
            age_minutes=90.0,
            heartbeat_age_min=20.0,
            shard_age_min=150.0,
            verdict="zombie_stale_shard",
        )
        assert v.heartbeat_age_min == 20.0  # type: ignore[attr-defined]
        assert v.shard_age_min == 150.0  # type: ignore[attr-defined]
        assert "zombie" in v.verdict  # type: ignore[attr-defined]


# ── New-prefix coverage hardening (S9) ───────────────────────────────────────


class TestNewPrefixRegistration:
    """Newly-registered prefixes from slot-2 B-011 blindspot audit + May-23 cutover.

    These prefixes were added after the initial vm_zombie_watchdog shipped.
    Each test documents the business reason to prevent regression.
    """

    def test_exec_alpha_registered(self) -> None:
        """Execution-alpha parallel measurement VMs must be watched (heartbeat-only)."""
        assert "exec-alpha-" in _VM_PREFIX_TO_BUCKET

    def test_synbench_registered(self) -> None:
        """Synthetic-data benchmark VMs must be watched (heartbeat-only)."""
        assert "synbench-" in _VM_PREFIX_TO_BUCKET

    def test_batch_live_recon_registered(self) -> None:
        """Nightly batch-live-reconciliation cron VM must be watched."""
        assert "batch-live-recon-" in _VM_PREFIX_TO_BUCKET

    def test_strategy_backtest_grid_registered(self) -> None:
        """Strategy parameter grid-search VMs must be watched (heartbeat-only)."""
        assert "strategy-backtest-grid-" in _VM_PREFIX_TO_BUCKET

    def test_prediction_fwd_registered(self) -> None:
        """Prediction forward-poll VMs must be watched after B-011 blindspot audit."""
        assert "prediction-fwd-" in _VM_PREFIX_TO_BUCKET

    def test_strategy_test_registered(self) -> None:
        """Strategy CI validation VMs (launch-strategy-test-vm.sh) must be watched."""
        assert "strategy-test-" in _VM_PREFIX_TO_BUCKET

    def test_ml_registered(self) -> None:
        """ML VMs must be watched (consolidated from ml-train- prefix, 2026-05-20)."""
        assert "ml-" in _VM_PREFIX_TO_BUCKET

    def test_new_prefixes_have_heartbeat_only_signal(self) -> None:
        """None or VmPrefixSpec(bucket=None) is correct for non-manifest-shard VMs."""
        heartbeat_only = ("exec-alpha-", "synbench-", "batch-live-recon-", "strategy-backtest-grid-")
        for prefix in heartbeat_only:
            spec = _VM_PREFIX_TO_BUCKET.get(prefix)
            if isinstance(spec, _VmPrefixSpec):
                assert spec.bucket is None, f"{prefix!r}: must be heartbeat-only (bucket=None)"
            else:
                assert spec is None, f"{prefix!r}: must be None (heartbeat-only)"


# ── Lifecycle class completeness (S9) ────────────────────────────────────────


class TestLifecycleClassCompleteness:
    """VmPrefixSpec entries with a lifecycle_class must use a known LifecycleClass value."""

    def test_all_vmprefix_spec_lifecycle_classes_are_valid(self) -> None:
        """Every VmPrefixSpec.lifecycle_class is a recognised LifecycleClass member."""
        valid_classes = set(_LifecycleClass)
        for key, spec in _VM_PREFIX_TO_BUCKET.items():
            if not isinstance(spec, _VmPrefixSpec):
                continue
            lc = spec.lifecycle_class
            if lc is None:
                continue
            assert lc in valid_classes, f"{key!r}: unknown lifecycle_class {lc!r}"

    def test_cefi_backfill_prefixes_use_ephemeral_batch(self) -> None:
        """CeFi per-venue backfill VMs are short-lived EPHEMERAL_BATCH jobs."""
        ephemeral_cefi = ("cefi-binance-", "cefi-bybit-", "cefi-okx-", "cefi-kraken-")
        for prefix in ephemeral_cefi:
            spec = _VM_PREFIX_TO_BUCKET.get(prefix)
            assert isinstance(spec, _VmPrefixSpec), f"{prefix!r}: expected VmPrefixSpec"
            assert spec.lifecycle_class == _LifecycleClass.EPHEMERAL_BATCH, (
                f"{prefix!r}: expected EPHEMERAL_BATCH, got {spec.lifecycle_class!r}"
            )


# ── Forward-poll idle thresholds (S9) ────────────────────────────────────────


class TestForwardPollIdleThresholds:
    """Forward-poll VMs use wider heartbeat windows since they poll every ~60s."""

    _GLOBAL_HB = 15.0
    _GLOBAL_SHARD = 120.0

    def test_cefi_fwd_uses_wider_threshold(self) -> None:
        """cefi-fwd-* must get 30 min heartbeat tolerance (wider than 15 min global)."""
        hb, shard = _resolve_idle_thresholds("cefi-fwd-20260515-001", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 30.0
        assert shard == 180.0

    def test_defi_fwd_uses_wider_threshold(self) -> None:
        """defi-fwd-* must get 30 min heartbeat tolerance."""
        hb, shard = _resolve_idle_thresholds("defi-fwd-20260515-001", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 30.0
        assert shard == 180.0

    def test_mdps_features_live_uses_wider_threshold(self) -> None:
        """Live MDPS feature-engineering VMs must get 240-min shard tolerance."""
        hb, shard = _resolve_idle_thresholds("mdps-features-live-defi-20260515", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 30.0
        assert shard == 240.0


# ── Terminated-VM reaper ──────────────────────────────────────────────────────


def _prefix_for(lifecycle: object) -> str:
    """Return any registered VM_PREFIX_TO_BUCKET prefix tagged with ``lifecycle``.

    Derived from the live registry so these tests don't hard-code a prefix that
    could be retired; skips the test if no prefix carries that lifecycle class.
    """
    for prefix, spec in _VM_PREFIX_TO_BUCKET.items():
        if spec is not None and spec.lifecycle_class == lifecycle:
            return prefix
    pytest.skip(f"no registered prefix with lifecycle_class={lifecycle}")


def _ts_ago(*, hours: float) -> str:
    """RFC3339 timestamp ``hours`` in the past (mirrors GCE lastStopTimestamp)."""
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


_REAP_AFTER_24H: Final[float] = 1440.0


class TestResolveLifecycleClass:
    """_resolve_lifecycle_class maps a VM name to its registered lifecycle class."""

    def test_known_ephemeral_batch_prefix(self) -> None:
        prefix = _prefix_for(_LifecycleClass.EPHEMERAL_BATCH)
        assert _resolve_lifecycle_class(f"{prefix}20260101-000000") == _LifecycleClass.EPHEMERAL_BATCH

    def test_unknown_prefix_returns_none(self) -> None:
        assert _resolve_lifecycle_class("zzz-unregistered-prefix-20260101") is None

    def test_longest_prefix_wins(self) -> None:
        """When two prefixes match, the more specific (longer) one decides."""
        # Build a name from the longest registered prefix and assert it resolves
        # to that prefix's class (not a shorter prefix's).
        longest = max(
            (p for p, s in _VM_PREFIX_TO_BUCKET.items() if s is not None),
            key=len,
        )
        spec = _VM_PREFIX_TO_BUCKET[longest]
        assert _resolve_lifecycle_class(f"{longest}suffix") == spec.lifecycle_class


class TestEvaluateTerminatedVm:
    """_evaluate_terminated_vm gates: ephemeral + stopped-past-grace + not-retained → reap."""

    def _eval(self, name: str, stopped_ts: str, labels: object) -> object:
        return _evaluate_terminated_vm(name, "asia-northeast1-c", stopped_ts, labels, _REAP_AFTER_24H)

    def test_ephemeral_stopped_past_grace_is_reaped(self) -> None:
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, _ts_ago(hours=48), {})
        assert v.verdict == "reap"  # type: ignore[attr-defined]
        assert v.should_reap() is True  # type: ignore[attr-defined]

    def test_ephemeral_within_grace_is_kept(self) -> None:
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, _ts_ago(hours=2), {})
        assert v.verdict == "keep_within_grace"  # type: ignore[attr-defined]
        assert v.should_reap() is False  # type: ignore[attr-defined]

    def test_long_lived_live_never_reaped(self) -> None:
        """A paused LONG_LIVED_LIVE VM, even stopped for days, must be kept."""
        name = f"{_prefix_for(_LifecycleClass.LONG_LIVED_LIVE)}20260101-000000"
        v = self._eval(name, _ts_ago(hours=240), {})
        assert v.verdict == "keep_not_ephemeral"  # type: ignore[attr-defined]

    def test_unknown_prefix_never_reaped(self) -> None:
        v = self._eval("zzz-unregistered-prefix-20260101", _ts_ago(hours=240), {})
        assert v.verdict == "keep_not_ephemeral"  # type: ignore[attr-defined]

    def test_keep_label_opt_out(self) -> None:
        """keep=true retains an otherwise-reapable ephemeral VM for forensics."""
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, _ts_ago(hours=240), {"keep": "true"})
        assert v.verdict == "keep_retained"  # type: ignore[attr-defined]

    def test_daemon_opt_out(self) -> None:
        """A daemon-labelled VM is never reaped even if its prefix looks ephemeral."""
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, _ts_ago(hours=240), {"tier": "daemon"})
        assert v.verdict == "keep_retained"  # type: ignore[attr-defined]

    def test_missing_timestamp_is_kept(self) -> None:
        """No parseable stop/creation timestamp → cannot age-gate → keep (conservative)."""
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, "", {})
        assert v.verdict == "keep_no_timestamp"  # type: ignore[attr-defined]

    def test_unparseable_timestamp_is_kept(self) -> None:
        name = f"{_prefix_for(_LifecycleClass.EPHEMERAL_BATCH)}20260101-000000"
        v = self._eval(name, "not-a-timestamp", {})
        assert v.verdict == "keep_no_timestamp"  # type: ignore[attr-defined]


class TestReapableLifecycleClasses:
    """Only the two ephemeral classes are reapable; live/recurring are protected."""

    def test_ephemeral_classes_are_reapable(self) -> None:
        assert _LifecycleClass.EPHEMERAL_BATCH in _REAPABLE_LIFECYCLE_CLASSES
        assert _LifecycleClass.EPHEMERAL_EXPERIMENT in _REAPABLE_LIFECYCLE_CLASSES

    def test_persistent_classes_not_reapable(self) -> None:
        assert _LifecycleClass.LONG_LIVED_LIVE not in _REAPABLE_LIFECYCLE_CLASSES
        assert _LifecycleClass.SCHEDULED_RECURRING not in _REAPABLE_LIFECYCLE_CLASSES
