"""Unit tests for the Layer-0 ``refetch_feed`` active self-healing action.

Mocks the owning-service CLI + the clock + the breaker probe — credential-free,
no real subprocess, no real freshness staleness. Phase 2 of
``plans/active/data_feed_sla_registry_and_active_self_healing_2026_06_19.md``.
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from unified_api_contracts.incident import ActionStatus, ActionType

from scripts.recovery.refetch_feed import (
    RefetchFeed,
    UnroutableFeedError,
)

_FIXED_NOW = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)


def _ok_cli(_cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=_cmd, returncode=0, stdout="fetched 3 shards", stderr="")


def _fail_cli(_cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=_cmd, returncode=1, stdout="", stderr="venue auth failed")


def _make(
    tmp_path: Path,
    *,
    breaker_open: bool = False,
    run_cli=_ok_cli,
    now: datetime = _FIXED_NOW,
) -> RefetchFeed:
    return RefetchFeed(
        cooldown_dir=tmp_path / "cooldown",
        cooldown_seconds=120,
        max_per_window=1,
        breaker_open=lambda _feed: breaker_open,
        now=lambda: now,
        run_cli=run_cli,
    )


def _args(feed_id: str) -> argparse.Namespace:
    return argparse.Namespace(feed_id=feed_id)


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------


def test_action_type_is_refetch_feed(tmp_path: Path) -> None:
    script = _make(tmp_path)
    assert script.ACTION_TYPE is ActionType.REFETCH_FEED
    # registry entry resolves (proves UTL registry has it)
    assert script.registry_entry.runbook_id == "RB-CONN-001"
    assert script.registry_entry.scope_required == ("feed_id",)


# ---------------------------------------------------------------------------
# CLI build — reuses the real MTDS CLI, coarse --day window
# ---------------------------------------------------------------------------


def test_build_refetch_cmd_market_feed(tmp_path: Path) -> None:
    script = _make(tmp_path)
    cmd = script._build_refetch_cmd("binance", "crypto_cefi", "binance")
    assert cmd == [
        "market-tick-data-service",
        "--operation",
        "download",
        "--mode",
        "batch",
        "--asset-group",
        "cefi",
        "--venues",
        "binance",
        "--day",
        "2026-06-20",
    ]


def test_build_refetch_cmd_defi_feed(tmp_path: Path) -> None:
    script = _make(tmp_path)
    cmd = script._build_refetch_cmd("aave_v3", "crypto_defi", "aave_v3")
    assert "--asset-group" in cmd
    assert cmd[cmd.index("--asset-group") + 1] == "defi"
    assert cmd[cmd.index("--venues") + 1] == "aave_v3"


def test_dry_run_plan_includes_cli(tmp_path: Path) -> None:
    script = _make(tmp_path)
    plan = script.dry_run_plan(_args("binance"))
    assert plan["feed_id"] == "binance"
    assert plan["refetch_action"] == "refetch-feed:binance"
    assert "market-tick-data-service" in plan["cli"]


# ---------------------------------------------------------------------------
# Happy path — CLI fires, SUCCEEDED
# ---------------------------------------------------------------------------


def test_happy_refetch_succeeds(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _spy(cmd: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return _ok_cli(cmd)

    script = _make(tmp_path, run_cli=_spy)
    status, post = script.execute_action(_args("binance"))
    assert status is ActionStatus.SUCCEEDED
    assert post["feed_id"] == "binance"
    assert "refetch_skipped" not in post
    assert len(calls) == 1  # the CLI actually ran


def test_cli_failure_returns_failed(tmp_path: Path) -> None:
    script = _make(tmp_path, run_cli=_fail_cli)
    status, post = script.execute_action(_args("binance"))
    assert status is ActionStatus.FAILED
    assert post["returncode"] == 1
    assert "venue auth failed" in str(post["stderr"])


# ---------------------------------------------------------------------------
# Storm guard — breaker OPEN, cooldown, window cap
# ---------------------------------------------------------------------------


def test_breaker_open_skips_refetch(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    script = _make(tmp_path, breaker_open=True, run_cli=lambda c: (calls.append(c), _ok_cli(c))[1])
    status, post = script.execute_action(_args("binance"))
    assert status is ActionStatus.SUCCEEDED  # skip is not a failure
    assert post["refetch_skipped"] == "breaker_open"
    assert calls == []  # breaker owns it — no CLI fired


def test_cooldown_skips_second_refetch_in_window(tmp_path: Path) -> None:
    script = _make(tmp_path)
    # First refetch fires + stamps the cooldown sentinel.
    status1, post1 = script.execute_action(_args("binance"))
    assert status1 is ActionStatus.SUCCEEDED
    assert "refetch_skipped" not in post1
    # Second refetch 30s later (inside 120s cooldown) → window_cap (max_per_window=1).
    later = RefetchFeed(
        cooldown_dir=tmp_path / "cooldown",
        cooldown_seconds=120,
        max_per_window=1,
        breaker_open=lambda _f: False,
        now=lambda: datetime(2026, 6, 20, 12, 0, 30, tzinfo=UTC),
        run_cli=_ok_cli,
    )
    status2, post2 = later.execute_action(_args("binance"))
    assert status2 is ActionStatus.SUCCEEDED
    assert post2["refetch_skipped"] == "cooldown"


def test_cooldown_window_allows_second_refetch_with_higher_cap(tmp_path: Path) -> None:
    # max_per_window=2 → second refetch inside window is allowed (cooldown bumps count).
    first = RefetchFeed(
        cooldown_dir=tmp_path / "cd",
        cooldown_seconds=120,
        max_per_window=2,
        breaker_open=lambda _f: False,
        now=lambda: _FIXED_NOW,
        run_cli=_ok_cli,
    )
    s1, p1 = first.execute_action(_args("binance"))
    assert "refetch_skipped" not in p1
    second = RefetchFeed(
        cooldown_dir=tmp_path / "cd",
        cooldown_seconds=120,
        max_per_window=2,
        breaker_open=lambda _f: False,
        now=lambda: datetime(2026, 6, 20, 12, 0, 30, tzinfo=UTC),
        run_cli=_ok_cli,
    )
    s2, p2 = second.execute_action(_args("binance"))
    assert s2 is ActionStatus.SUCCEEDED
    assert "refetch_skipped" not in p2  # under cap=2 → fires
    # Third refetch inside the window → window_cap (count=2 >= cap=2).
    third = RefetchFeed(
        cooldown_dir=tmp_path / "cd",
        cooldown_seconds=120,
        max_per_window=2,
        breaker_open=lambda _f: False,
        now=lambda: datetime(2026, 6, 20, 12, 0, 45, tzinfo=UTC),
        run_cli=_ok_cli,
    )
    s3, p3 = third.execute_action(_args("binance"))
    assert s3 is ActionStatus.SUCCEEDED
    assert p3["refetch_skipped"] == "window_cap"


def test_refetch_allowed_after_cooldown_expires(tmp_path: Path) -> None:
    first = _make(tmp_path)
    first.execute_action(_args("binance"))
    # 200s later — cooldown (120s) expired → fires again.
    after = RefetchFeed(
        cooldown_dir=tmp_path / "cooldown",
        cooldown_seconds=120,
        max_per_window=1,
        breaker_open=lambda _f: False,
        now=lambda: datetime(2026, 6, 20, 12, 3, 20, tzinfo=UTC),
        run_cli=_ok_cli,
    )
    status, post = after.execute_action(_args("binance"))
    assert status is ActionStatus.SUCCEEDED
    assert "refetch_skipped" not in post


# ---------------------------------------------------------------------------
# Unroutable feeds
# ---------------------------------------------------------------------------


def test_unknown_feed_id_raises(tmp_path: Path) -> None:
    script = _make(tmp_path)
    with pytest.raises(UnroutableFeedError):
        script.execute_action(_args("not_a_real_feed"))


def test_execution_feed_unroutable(tmp_path: Path) -> None:
    # account_snapshot is asset_group=execution — no wired MTDS route → raises
    # (the base class converts the raise to a structured FAILED event).
    script = _make(tmp_path)
    with pytest.raises(UnroutableFeedError):
        script.execute_action(_args("account_snapshot"))
