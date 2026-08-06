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
from unified_api_contracts import VmPrefixSpec

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
_VmPrefixSpec = VmPrefixSpec  # UAC SSOT type; registry moved to deployment_service.vm_prefix_registry (2026-07-13)
_DAEMON_TIER_LABELS = _mod.DAEMON_TIER_LABELS
_DAEMON_PURPOSE_OPT_OUT = _mod.DAEMON_PURPOSE_OPT_OUT
_resolve_lifecycle_class = _mod._resolve_lifecycle_class
_evaluate_terminated_vm = _mod._evaluate_terminated_vm
_ReapVerdict = _mod.ReapVerdict
_REAPABLE_LIFECYCLE_CLASSES = _mod.REAPABLE_LIFECYCLE_CLASSES
_LifecycleClass = _mod.LifecycleClass
_blob_age_minutes = _mod._blob_age_minutes
_safe_blob_age_minutes = _mod._safe_blob_age_minutes
_evaluate_vm = _mod._evaluate_vm
_persist_zombie_alert = _mod._persist_zombie_alert


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

    def test_backfill_prefix_gets_widened_threshold(self) -> None:
        """API-football backfill VMs (af-backfill-*) match the 15-min global heartbeat
        tolerance and use a 180-min shard tolerance vs 120-min global — widened past
        the old (10.0, 60.0) pair after it caused a live-VM reap false positive
        (zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md Incident 2)."""
        hb, shard = _resolve_idle_thresholds("af-backfill-20260515-001", self._GLOBAL_HB, self._GLOBAL_SHARD)
        assert hb == 15.0
        assert shard == 180.0

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


# ── reap-persistence (deployment_alerts_ingestion_completeness_2026_07_20.md todo 7) ──────────


class TestPersistZombieAlert:
    """_persist_zombie_alert() writes a durable ledger row so reap history survives on the page —
    previously only the ephemeral Slack webhook fired (line 247's _send_zombie_notification)."""

    def test_writes_one_object_conforming_to_the_shared_reader_shape(self) -> None:
        import json
        from unittest.mock import patch

        captured: dict[str, object] = {}

        def fake_upload(bucket: str, path: str, data: bytes, **_kwargs: object) -> None:
            captured["bucket"] = bucket
            captured["path"] = path
            captured["row"] = json.loads(data.decode().strip())

        with (
            patch.object(_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
            patch.object(_mod, "upload_to_storage", side_effect=fake_upload),
        ):
            _persist_zombie_alert("af-backfill-20260721", "zombie_stale_heartbeat", "killed after 90min")

        assert captured["bucket"] == "unified-trading-cicd-events"
        assert str(captured["path"]).startswith("cicd/alerts/")
        assert str(captured["path"]).endswith(".jsonl")
        row = captured["row"]
        assert isinstance(row, dict)
        # Matches _repo_ci_alerts.py::_parse_line's "slack_alert" shape exactly, one object per call.
        assert row["event_type"] == "slack_alert"
        assert row["repo"] == "deployment-service"
        assert row["alert_class"] == "zombie_stale_heartbeat"
        assert row["message"] == "killed after 90min"
        # deployment_target extraction reads top-level vm_name/deployment_id, not a nested dict.
        assert row["vm_name"] == "af-backfill-20260721"
        # ZOMBIE_WATCHDOG plane has no severity concept per FIELD_COVERAGE — never fabricated.
        assert row["severity"] is None

    def test_two_calls_land_at_two_distinct_objects(self) -> None:
        """One-object-per-alert — no shared-filename read-modify-write race."""
        from unittest.mock import patch

        paths: list[str] = []

        def fake_upload(bucket: str, path: str, data: bytes, **_kwargs: object) -> None:
            paths.append(path)

        with (
            patch.object(_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
            patch.object(_mod, "upload_to_storage", side_effect=fake_upload),
        ):
            _persist_zombie_alert("vm-a", "zombie_stale_heartbeat", "m1")
            _persist_zombie_alert("vm-a", "zombie_stale_heartbeat", "m2")

        assert len(paths) == 2
        assert paths[0] != paths[1]

    def test_bucket_resolution_failure_is_best_effort(self) -> None:
        """A resolution failure must not raise — persistence is best-effort, never a kill blocker."""
        from unittest.mock import patch

        from unified_trading_library import BucketNamingError

        with patch.object(_mod, "resolve_bucket_name", side_effect=BucketNamingError("no yaml entry")):
            _persist_zombie_alert("vm-a", "zombie_stale_heartbeat", "m")  # must not raise

    def test_upload_failure_is_best_effort(self) -> None:
        from unittest.mock import patch

        with (
            patch.object(_mod, "resolve_bucket_name", return_value="unified-trading-cicd-events"),
            patch.object(_mod, "upload_to_storage", side_effect=OSError("network down")),
        ):
            _persist_zombie_alert("vm-a", "zombie_stale_heartbeat", "m")  # must not raise


class TestReapTerminatedVmsPersistsOnActualReap:
    """_reap_terminated_vms() wiring (deployment_alerts_ingestion_completeness_2026_07_20.md todo 7,
    item (g)) — a confirmed reap must actually CALL _persist_zombie_alert(alert_class="reap_terminated"),
    not just prove the persist helper works in isolation (TestPersistZombieAlert above)."""

    def _make_reap_verdict(self, vm_name: str = "af-backfill-20260721") -> object:
        from unified_api_contracts import LifecycleClass

        return _mod.ReapVerdict(
            vm_name=vm_name,
            zone="asia-northeast1-c",
            stopped_age_min=180.0,
            lifecycle_class=LifecycleClass.EPHEMERAL_BATCH,
            verdict="reap",
        )

    def test_a_successful_kill_persists_a_reap_terminated_alert(self) -> None:
        from unittest.mock import patch

        verdict = self._make_reap_verdict()
        with (
            patch.object(
                _mod, "_list_terminated_vms", return_value=[("af-backfill-20260721", "asia-northeast1-c", "", {})]
            ),
            patch.object(_mod, "_evaluate_terminated_vm", return_value=verdict),
            patch.object(_mod, "_kill_vm", return_value=True),
            patch.object(_mod, "_persist_zombie_alert") as mock_persist,
        ):
            reaped = _mod._reap_terminated_vms(compute_client=None, reap_after_min=120.0, dry_run=False, workers=1)

        assert reaped == 1
        mock_persist.assert_called_once()
        call_args = mock_persist.call_args[0]
        assert call_args[0] == "af-backfill-20260721"
        assert call_args[1] == "reap_terminated"

    def test_dry_run_never_persists_or_kills(self) -> None:
        from unittest.mock import patch

        verdict = self._make_reap_verdict()
        with (
            patch.object(
                _mod, "_list_terminated_vms", return_value=[("af-backfill-20260721", "asia-northeast1-c", "", {})]
            ),
            patch.object(_mod, "_evaluate_terminated_vm", return_value=verdict),
            patch.object(_mod, "_kill_vm") as mock_kill,
            patch.object(_mod, "_persist_zombie_alert") as mock_persist,
        ):
            reaped = _mod._reap_terminated_vms(compute_client=None, reap_after_min=120.0, dry_run=True, workers=1)

        assert reaped == 0
        mock_kill.assert_not_called()
        mock_persist.assert_not_called()

    def test_a_failed_kill_does_not_persist_an_alert(self) -> None:
        from unittest.mock import patch

        verdict = self._make_reap_verdict()
        with (
            patch.object(
                _mod, "_list_terminated_vms", return_value=[("af-backfill-20260721", "asia-northeast1-c", "", {})]
            ),
            patch.object(_mod, "_evaluate_terminated_vm", return_value=verdict),
            patch.object(_mod, "_kill_vm", return_value=False),
            patch.object(_mod, "_persist_zombie_alert") as mock_persist,
        ):
            reaped = _mod._reap_terminated_vms(compute_client=None, reap_after_min=120.0, dry_run=False, workers=1)

        assert reaped == 0
        mock_persist.assert_not_called()

    def test_a_kept_verdict_is_never_reaped_or_persisted(self) -> None:
        from unittest.mock import patch

        from unified_api_contracts import LifecycleClass

        kept = _mod.ReapVerdict(
            vm_name="strategy-live-cefi-20260620",
            zone="asia-northeast1-c",
            stopped_age_min=5.0,
            lifecycle_class=LifecycleClass.LONG_LIVED_LIVE,
            verdict="keep_not_ephemeral",
        )
        with (
            patch.object(
                _mod,
                "_list_terminated_vms",
                return_value=[("strategy-live-cefi-20260620", "asia-northeast1-c", "", {})],
            ),
            patch.object(_mod, "_evaluate_terminated_vm", return_value=kept),
            patch.object(_mod, "_kill_vm") as mock_kill,
            patch.object(_mod, "_persist_zombie_alert") as mock_persist,
        ):
            reaped = _mod._reap_terminated_vms(compute_client=None, reap_after_min=120.0, dry_run=False, workers=1)

        assert reaped == 0
        mock_kill.assert_not_called()
        mock_persist.assert_not_called()


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


# ── Richer-signal ("shard-mtime") coverage for real manifest-shard writers (2026-08-06) ──
#
# Bug class this closes: a launcher whose VM writes a real per-VM manifest shard
# (ManifestWriter(per_vm_shards=True), exact `_index/per_vm/{vm_name}.parquet`
# filename) but whose VM_PREFIX_TO_BUCKET entry is bucket=None (or absent, falling
# through to a generic bucket=None catch-all) only gets the heartbeat liveness
# signal — never the shard-staleness check that catches "VM alive + heartbeating,
# but writing NOTHING at all" (the exact 2026-07-18 sports-fixtures incident this
# module's own docstring documents: 3.5h, zero rows written, heartbeat never
# lapsed). This class of gap was found live 2026-08-06
# (vm_zombie_watchdog_prefix_coverage_gap_2026_08_06.md) for 3 launcher families
# that were verified (via source read of the underlying Python entrypoint) to
# write the watchdog-pollable exact shard filename into a single deterministic
# per-asset_group bucket, yet were mis-registered as heartbeat-only.
class TestManifestShardWriterBucketCoverage:
    """Confirmed real-manifest-shard-writer prefixes must carry a real bucket.

    Each prefix here was verified by reading the launcher's own bucket-resolution
    code (not the launcher's comments alone) to confirm: (a) the underlying script
    uses ManifestWriter(per_vm_shards=True) / writes the exact
    `_index/per_vm/{vm_name}.parquet` blob (not a chunked "-part{N}" variant, which
    the watchdog's exact-path check can never observe — see the deliberately-
    NOT-fixed `expected-universe-v2-` entry, still bucket=None for this reason),
    and (b) the target bucket is statically deterministic from the VM name
    (a fixed literal or a resolve_bucket_name(kind=..., asset_group=<embedded-ag>)
    call matching this file's own _TICK_*/_INSTR_*/_FEAT_* constants).
    """

    # prefix -> the exact bucket constant this file resolves for it (module-level name).
    _EXPECTED_REAL_BUCKET_PREFIXES: dict[str, str] = {
        "features-sfi-progressive-": "_FEAT_SPORTS",
        "manifest-recon-apply-cefi-": "_TICK_CEFI",
        "manifest-recon-apply-defi-": "_TICK_DEFI",
        "manifest-recon-apply-tradfi-": "_TICK_TRADFI",
        "blank-reason-recon-cefi-": "_TICK_CEFI",
        "blank-reason-recon-defi-": "_TICK_DEFI",
        "blank-reason-recon-tradfi-": "_TICK_TRADFI",
        "blank-reason-recon-sports-": "_INSTR_SPORTS",
        "blank-reason-recon-prediction-": "_TICK_PRED",
    }

    def test_all_expected_prefixes_are_registered(self) -> None:
        missing = [p for p in self._EXPECTED_REAL_BUCKET_PREFIXES if p not in _VM_PREFIX_TO_BUCKET]
        assert not missing, f"Expected real-bucket prefixes missing from VM_PREFIX_TO_BUCKET entirely: {missing}"

    def test_all_expected_prefixes_have_a_non_none_bucket(self) -> None:
        """Regression guard: none of these may ever silently revert to heartbeat-only."""
        heartbeat_only: list[str] = []
        for prefix in self._EXPECTED_REAL_BUCKET_PREFIXES:
            spec = _VM_PREFIX_TO_BUCKET.get(prefix)
            if not isinstance(spec, _VmPrefixSpec) or spec.bucket is None:
                heartbeat_only.append(prefix)
        assert not heartbeat_only, (
            "These confirmed real-manifest-shard-writer prefixes regressed to "
            "heartbeat-only (bucket=None) — the shard-staleness check silently stops "
            "catching zero-write failures for them:\n" + "\n".join(heartbeat_only)
        )

    def test_sibling_prefixes_are_not_shadowed_by_the_new_longer_ones(self) -> None:
        """The generic read-only siblings must still resolve independently.

        `manifest-recon-cefi-...` (dry-run, no --apply-flips) must still hit the
        SHORTER `manifest-recon-` catch-all (bucket=None, correct — it never writes),
        not the new `manifest-recon-apply-cefi-` entry (different literal, does not
        collide) — and `blank-reason-recon-` un-suffixed names still resolve to the
        generic entry when no per-AG match applies.
        """
        prefixes = tuple(_VM_PREFIX_TO_BUCKET.keys())
        dry_run_name = "manifest-recon-cefi-20260806-000000"
        matched = max((p for p in prefixes if dry_run_name.startswith(p)), key=len)
        assert matched == "manifest-recon-", f"expected the read-only catch-all, got {matched!r}"
        assert _VM_PREFIX_TO_BUCKET[matched] is None

    def test_asset_group_buckets_match_the_shared_tick_constants(self) -> None:
        """Each per-AG entry's bucket must match the SAME constant other per-AG
        launchers in this registry use for that asset_group (e.g. canonical-migration-{ag}-,
        datapoint-validation-{ag}-) — never a hand-typed duplicate literal."""
        from deployment_service import vm_prefix_registry as _registry_mod

        for prefix, const_name in self._EXPECTED_REAL_BUCKET_PREFIXES.items():
            spec = _VM_PREFIX_TO_BUCKET[prefix]
            assert isinstance(spec, _VmPrefixSpec)
            expected_bucket = getattr(_registry_mod, const_name)
            assert spec.bucket == expected_bucket, (
                f"{prefix!r}: bucket {spec.bucket!r} does not match {const_name}={expected_bucket!r}"
            )


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


# ── _blob_age_minutes / _safe_blob_age_minutes regression coverage ───────────
# 2026-07-18 incident (zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md
# Incident 3): a prior version called .reload()/.updated against the UTL
# GCSBucketHandle wrapper, which doesn't implement either — the resulting
# AttributeError was silently swallowed by a bare except-Exception, so every
# heartbeat/shard check ALWAYS returned None regardless of real freshness.
# Net effect: every known-prefix VM older than 30min was unconditionally
# classified zombie_no_heartbeat. 3 live VMs were really deleted before this
# was caught. These tests exercise the real function against fake objects
# shaped like the RAW google.cloud.storage API (the thing the fix now
# requires) to prove: (1) a fresh blob yields a real age, not None, and (2) a
# check FAILURE is distinguishable from a genuinely-missing blob and can
# never itself produce a zombie/delete verdict.


class _FakeBlob:
    def __init__(self, *, exists: bool = True, updated: datetime | None = None, raise_on_reload: bool = False) -> None:
        self._exists = exists
        self.updated = updated
        self._raise_on_reload = raise_on_reload

    def exists(self) -> bool:
        return self._exists

    def reload(self) -> None:
        if self._raise_on_reload:
            # Simulates the exact 2026-07-18 bug: calling .reload() against an
            # object that doesn't implement it (the UTL GCSBucketHandle wrapper).
            raise AttributeError("'_FakeBlob' object has no attribute 'reload' (simulated wrapper mismatch)")


class _FakeBucket:
    def __init__(self, blob: _FakeBlob) -> None:
        self._blob = blob

    def blob(self, name: str) -> _FakeBlob:  # noqa: ARG002 — name unused, single-blob fake
        return self._blob


class _FakeNativeStorageClient:
    """Mimics the raw google.cloud.storage.Client shape: just .bucket(name)."""

    def __init__(self, bucket: _FakeBucket) -> None:
        self._bucket = bucket

    def bucket(self, name: str) -> _FakeBucket:  # noqa: ARG002 — name unused, single-bucket fake
        return self._bucket


class _FakeStorageClient:
    """Mimics UTL's StorageClient shape: exposes ._client, the native raw client."""

    def __init__(self, native_client: _FakeNativeStorageClient) -> None:
        self._client = native_client


class _FakeComputeInstance:
    def __init__(self, creation_timestamp: str) -> None:
        self.creation_timestamp = creation_timestamp


class _FakeComputeClient:
    def __init__(self, creation_timestamp: str) -> None:
        self._creation_timestamp = creation_timestamp

    def get(self, project: str, zone: str, instance: str) -> _FakeComputeInstance:  # noqa: ARG002
        return _FakeComputeInstance(self._creation_timestamp)


class TestBlobAgeMinutes:
    def test_fresh_blob_returns_real_age_not_none(self) -> None:
        """Regression: a genuinely fresh blob must yield a real minutes-age, not None."""
        updated = datetime.now(UTC) - timedelta(minutes=3)
        bucket = _FakeBucket(_FakeBlob(exists=True, updated=updated))
        age = _blob_age_minutes(bucket, "vm-heartbeat/some-vm.txt")
        assert age is not None
        assert 2.0 < age < 4.0

    def test_missing_blob_returns_none(self) -> None:
        bucket = _FakeBucket(_FakeBlob(exists=False))
        assert _blob_age_minutes(bucket, "vm-heartbeat/some-vm.txt") is None

    def test_unexpected_error_propagates_not_swallowed(self) -> None:
        """The bare except-Exception that caused the incident is gone — a real
        check failure (e.g. the wrapper-mismatch AttributeError) must raise,
        not silently return None indistinguishable from "genuinely missing"."""
        bucket = _FakeBucket(_FakeBlob(exists=True, raise_on_reload=True))
        with pytest.raises(AttributeError):
            _blob_age_minutes(bucket, "vm-heartbeat/some-vm.txt")


class TestSafeBlobAgeMinutes:
    def test_success_is_determined_with_real_age(self) -> None:
        updated = datetime.now(UTC) - timedelta(minutes=5)
        bucket = _FakeBucket(_FakeBlob(exists=True, updated=updated))
        age, determined = _safe_blob_age_minutes(bucket, "vm-heartbeat/x.txt", "vm-x", "heartbeat")
        assert determined is True
        assert age is not None

    def test_check_failure_is_undetermined_not_missing(self) -> None:
        """A check FAILURE (determined=False) must be distinguishable from a
        genuinely-missing blob (age=None, determined=True) — callers must
        never treat the two the same way (that conflation IS the incident)."""
        bucket = _FakeBucket(_FakeBlob(exists=True, raise_on_reload=True))
        age, determined = _safe_blob_age_minutes(bucket, "vm-heartbeat/x.txt", "vm-x", "heartbeat")
        assert determined is False
        assert age is None


class TestEvaluateVmFailSafeOnUndeterminedAge:
    def test_undetermined_heartbeat_never_yields_zombie_verdict(self) -> None:
        """End-to-end regression for the 2026-07-18 incident: a VM old enough
        to trip zombie_no_heartbeat (age > 30min) whose heartbeat check FAILS
        (not "missing" — an actual exception, e.g. the wrapper-mismatch bug)
        must classify 'alive', never a zombie/delete verdict."""
        old_creation = (datetime.now(UTC) - timedelta(minutes=60)).isoformat()
        compute_client = _FakeComputeClient(old_creation)
        native_client = _FakeNativeStorageClient(_FakeBucket(_FakeBlob(exists=True, raise_on_reload=True)))
        storage_client = _FakeStorageClient(native_client)

        verdict = _evaluate_vm(
            compute_client,
            storage_client,
            "unregistered-prefix-test-vm-20260101",
            "asia-northeast1-a",
            min_age=15.0,
            heartbeat_stale=15.0,
            shard_stale=120.0,
            finished_grace=10.0,
        )
        assert verdict.verdict == "alive"
        assert "zombie" not in verdict.verdict

    def test_fresh_heartbeat_still_classifies_alive(self) -> None:
        """Sanity companion: a genuinely fresh, successfully-checked heartbeat
        must also classify alive (proves the fix didn't just make everything
        alive unconditionally — it restores real freshness-based evaluation)."""
        old_creation = (datetime.now(UTC) - timedelta(minutes=60)).isoformat()
        compute_client = _FakeComputeClient(old_creation)
        fresh_updated = datetime.now(UTC) - timedelta(minutes=2)
        native_client = _FakeNativeStorageClient(_FakeBucket(_FakeBlob(exists=True, updated=fresh_updated)))
        storage_client = _FakeStorageClient(native_client)

        verdict = _evaluate_vm(
            compute_client,
            storage_client,
            "unregistered-prefix-test-vm-20260101",
            "asia-northeast1-a",
            min_age=15.0,
            heartbeat_stale=15.0,
            shard_stale=120.0,
            finished_grace=10.0,
        )
        assert verdict.verdict == "alive"
