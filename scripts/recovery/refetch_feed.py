# Epic: observability_master
# Lifecycle: permanent
"""Layer-0 ``refetch_feed`` — active self-healing for a stale data feed.

Runbook: RB-CONN-001.

A stale ``critical`` feed (age >= ``DataFreshnessContract.max_age_seconds``)
triggers an active, mapped re-fetch through the feed's OWNING service CLI before
/ while the alerting ladder escalates. This is the "map stale feed → specific
re-fetch method" verb the autonomous-recovery-matrix previously lacked (recovery
was passive: circuit-breaker backoff + HALF_OPEN probe + consolidator
stale-fallback).

Distinct from ``failover_feed`` (RB-CONN-001): FAILOVER flips an MTDS handler's
primary→backup subscription; REFETCH re-pulls the SAME feed (the stale feed has
no backup, just a gap to close).

Binding
-------
The feed's re-fetch is bound in the Phase-1 freshness registry —
``unified_api_contracts.internal.ALL_FRESHNESS_CONTRACTS[feed_id].refetch_action``
== ``refetch-feed:<source>``. This script resolves ``feed_id`` → contract →
owning-service CLI invocation (no new fetch path; reuses the existing
``market-tick-data-service`` CLI for market-data feeds).

Storm guard (idempotency)
-------------------------
A persistently-stale feed must NOT spawn a refetch storm. Three guards, mirroring
the circuit-breaker ``auto_cooldown`` pattern:

1. **Per-feed cooldown** — a file sentinel in ``tempfile.gettempdir()`` records
   the last refetch time per ``feed_id``; a refetch inside the cooldown window is
   skipped (SUCCEEDED, ``refetch_skipped=cooldown``).
2. **Per-window cap** — at most ``_MAX_REFETCHES_PER_WINDOW`` refetches per feed
   inside the cooldown window.
3. **Breaker-OPEN skip** — never refetch a feed whose circuit breaker is OPEN
   (let the breaker own the backoff); skipped (SUCCEEDED,
   ``refetch_skipped=breaker_open``).

The shared ``_common.RepeatedRepairLoopDetector`` still guards the
process-local 3-in-15min loop on top of these.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

from unified_api_contracts.incident import ActionStatus, ActionType
from unified_api_contracts.internal import ALL_FRESHNESS_CONTRACTS

from ._common import Layer0Script

# Per-feed refetch cooldown (seconds). Mirrors the circuit-breaker auto_cooldown
# idiom — a persistently-stale feed gets at most a handful of active refetches
# before the alerting ladder + breaker take over. 120s ≈ 2× the tightest
# critical max_age (account/positions 120s) so a healthy feed never re-trips.
_COOLDOWN_SECONDS: int = 120
# Cap per feed inside one cooldown window (belt-and-braces against a clock skew
# that lets two near-simultaneous invocations slip the cooldown).
_MAX_REFETCHES_PER_WINDOW: int = 1

# asset_group → owning-service CLI route. Market-data domains all re-pull through
# the market-tick-data-service `--operation download` handler (the generic
# TickDataHandler). The MTDS `--asset-group` Literal is lowercase
# cefi/defi/tradfi/sports/prediction, so the DataFreshnessContract crypto_cefi/
# crypto_defi labels map down here.
_MTDS_ASSET_GROUP: dict[str, str] = {
    "crypto_cefi": "cefi",
    "crypto_defi": "defi",
    "tradfi": "tradfi",
    "sports": "sports",
    "onchain": "defi",  # on-chain analytics feeds re-pull via the defi MTDS path
}


class FeedRefetchSkipped(Exception):
    """Raised to short-circuit ``execute_action`` with a SUCCEEDED-skip verdict.

    Carries the storm-guard reason (cooldown / breaker_open / window_cap) so the
    AgentActionEvent records WHY no refetch fired without looking like a failure.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class UnroutableFeedError(Exception):
    """Raised when a feed has no wired owning-service re-fetch CLI route.

    Currently only market-data asset_groups (cefi/defi/tradfi/sports/onchain) are
    wired to the MTDS CLI. execution / feature / ml feeds route to peer services
    whose CLIs are out of deployment-service's reuse scope — they surface as a
    structured FAILED so the escalation ladder owns the stale feed instead.
    """


def _default_cooldown_dir() -> Path:
    return Path(tempfile.gettempdir()) / "uts_refetch_cooldown"


def _default_breaker_open(feed_id: str) -> bool:
    """Default breaker-OPEN probe: a runtime writes a sentinel file when a feed's
    circuit breaker opens. Absence → assume CLOSED (refetch allowed). Injectable
    for unit tests + for a richer runtime hook later.
    """
    sentinel = Path(tempfile.gettempdir()) / "uts_breaker_open" / f"{feed_id}.open"
    return sentinel.exists()


class RefetchFeed(Layer0Script):
    ACTION_TYPE: ClassVar[ActionType] = ActionType.REFETCH_FEED

    def __init__(
        self,
        *,
        cooldown_dir: Path | None = None,
        cooldown_seconds: int = _COOLDOWN_SECONDS,
        max_per_window: int = _MAX_REFETCHES_PER_WINDOW,
        breaker_open: Callable[[str], bool] | None = None,
        now: Callable[[], datetime] | None = None,
        run_cli: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        super().__init__()
        self._cooldown_dir = cooldown_dir or _default_cooldown_dir()
        self._cooldown_seconds = cooldown_seconds
        self._max_per_window = max_per_window
        self._breaker_open = breaker_open or _default_breaker_open
        self._now = now or (lambda: datetime.now(UTC))
        self._run_cli = run_cli or self._subprocess_run

    # ── scope ────────────────────────────────────────────────────────────────

    def add_action_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--feed_id",
            required=True,
            help=(
                "Stale feed key — a key of ALL_FRESHNESS_CONTRACTS "
                "(e.g. binance, aave_v3, account_snapshot). The bound refetch is "
                "ALL_FRESHNESS_CONTRACTS[feed_id].refetch_action."
            ),
        )

    def compute_scope_key(self, args: argparse.Namespace) -> str:
        return f"feed:{args.feed_id}"

    def dry_run_plan(self, args: argparse.Namespace) -> dict[str, str]:
        contract = self._resolve_contract(args.feed_id)
        cmd = self._build_refetch_cmd(args.feed_id, contract.asset_group, contract.source)
        return {
            "action": "refetch_feed",
            "feed_id": args.feed_id,
            "asset_group": contract.asset_group,
            "refetch_action": contract.refetch_action or "",
            "cli": " ".join(cmd),
            "effect": "re-pull the stale feed's most-recent window via its owning service CLI",
        }

    # ── execute ──────────────────────────────────────────────────────────────

    def execute_action(
        self, args: argparse.Namespace
    ) -> tuple[ActionStatus, dict[str, str | bool | int | float | None]]:
        feed_id: str = args.feed_id
        contract = self._resolve_contract(feed_id)

        # Storm guard 3 — never refetch a feed whose breaker is OPEN.
        if self._breaker_open(feed_id):
            return ActionStatus.SUCCEEDED, {
                "feed_id": feed_id,
                "refetch_skipped": "breaker_open",
                "detail": "circuit breaker OPEN — breaker owns the backoff; no refetch issued",
            }

        # Storm guards 1 + 2 — per-feed cooldown + per-window cap.
        skip_reason = self._cooldown_skip_reason(feed_id)
        if skip_reason is not None:
            return ActionStatus.SUCCEEDED, {
                "feed_id": feed_id,
                "refetch_skipped": skip_reason,
                "cooldown_seconds": self._cooldown_seconds,
                "detail": f"refetch rate-limited ({skip_reason}) — no refetch issued",
            }

        cmd = self._build_refetch_cmd(feed_id, contract.asset_group, contract.source)
        self._stamp_refetch(feed_id)
        result = self._run_cli(cmd)
        if result.returncode != 0:
            return ActionStatus.FAILED, {
                "feed_id": feed_id,
                "cli": " ".join(cmd),
                "returncode": result.returncode,
                "stderr": (result.stderr or "")[-500:],
            }
        return ActionStatus.SUCCEEDED, {
            "feed_id": feed_id,
            "asset_group": contract.asset_group,
            "cli": " ".join(cmd),
            "stdout_tail": (result.stdout or "")[-300:],
        }

    # ── helpers ──────────────────────────────────────────────────────────────

    def _resolve_contract(self, feed_id: str):  # noqa: ANN202 — DataFreshnessContract (avoid deep import)
        contract = ALL_FRESHNESS_CONTRACTS.get(feed_id)
        if contract is None:
            raise UnroutableFeedError(
                f"feed_id {feed_id!r} not in ALL_FRESHNESS_CONTRACTS — "
                f"no freshness contract → no bound refetch"
            )
        return contract

    def _build_refetch_cmd(self, feed_id: str, asset_group: str, source: str) -> list[str]:
        """Coarsest real re-fetch the owning service CLI supports.

        Market-data feeds → ``market-tick-data-service --operation download
        --mode batch --asset-group <ag> --venues <source> --day <today-UTC>``.
        ``--day`` is the surgical shard-targeting flag the MTDS CLI exposes
        today (infrastructure_master B.2 Phase 5); finer-grained
        ``--shard-key`` targeting is a future tightening — for a stale-feed
        re-pull the current UTC day is the right coarse window.
        """
        mtds_ag = _MTDS_ASSET_GROUP.get(asset_group)
        if mtds_ag is None:
            raise UnroutableFeedError(
                f"feed_id {feed_id!r} asset_group={asset_group!r} has no wired "
                f"owning-service re-fetch CLI (only market-data domains route to "
                f"the MTDS CLI today); escalation ladder owns this stale feed"
            )
        today = self._now().strftime("%Y-%m-%d")
        return [
            "market-tick-data-service",
            "--operation",
            "download",
            "--mode",
            "batch",
            "--asset-group",
            mtds_ag,
            "--venues",
            source,
            "--day",
            today,
        ]

    def _cooldown_path(self, feed_id: str) -> Path:
        return self._cooldown_dir / f"{feed_id}.json"

    def _cooldown_skip_reason(self, feed_id: str) -> str | None:
        path = self._cooldown_path(feed_id)
        if not path.exists():
            return None
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None  # corrupt/unreadable sentinel → treat as no cooldown
        last_ts = float(state.get("last_refetch_epoch", 0.0))
        count = int(state.get("count_in_window", 0))
        elapsed = self._now().timestamp() - last_ts
        if elapsed >= self._cooldown_seconds:
            return None  # window expired → fire freely
        # Inside the cooldown window: allow up to ``max_per_window`` refetches,
        # skip the rest. ``window_cap`` once the cap is hit; ``cooldown`` is the
        # single-refetch-per-window default (max_per_window == 1).
        if count >= self._max_per_window:
            return "window_cap" if self._max_per_window > 1 else "cooldown"
        return None

    def _stamp_refetch(self, feed_id: str) -> None:
        self._cooldown_dir.mkdir(parents=True, exist_ok=True)
        path = self._cooldown_path(feed_id)
        now_epoch = self._now().timestamp()
        count = 1
        if path.exists():
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                last_ts = float(state.get("last_refetch_epoch", 0.0))
                if now_epoch - last_ts < self._cooldown_seconds:
                    count = int(state.get("count_in_window", 0)) + 1
            except (OSError, json.JSONDecodeError):
                count = 1
        path.write_text(
            json.dumps({"last_refetch_epoch": now_epoch, "count_in_window": count}),
            encoding="utf-8",
        )

    @staticmethod
    def _subprocess_run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)


if __name__ == "__main__":
    import sys

    sys.exit(RefetchFeed().run())
