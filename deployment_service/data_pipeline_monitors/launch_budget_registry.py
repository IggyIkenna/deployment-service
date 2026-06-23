# Epic: mtds_mdps_master
# Lifecycle: permanent
"""Fleet-wide, registry-driven launch parameters — API rate-budget allocation +
machine-sizing — the PRIMARY mechanism that replaces reactive per-VM backoff /
OOM-relaunch (operator design 2026-06-23).

Two registries + their launch-time helpers live here:

Part 1 — API rate-budget (``SOURCE_RATE_LIMITS_RPM`` + ``allocate_rate_budget``)
--------------------------------------------------------------------------------
Each external data SOURCE has ONE fleet-wide requests-per-minute ceiling (the
paid subscription's per-key cap). When N VMs run against the same source, that
ceiling is split DETERMINISTICALLY at launch — each VM gets
``per_vm_rpm = source_rpm // N`` — and the VM's adapter is configured to run at
exactly that rate (its self-enforced token-bucket throttle), with a matched
in-flight concurrency. So the fleet runs at the FULL allowed throughput, never
exceeds it, and never wastes wall-clock backing off a 429.

**API-Football quota is SHARED across EVERY endpoint** (the critical fact): the
1200 req/min Custom-plan cap is ONE subscription quota spent by fixtures AND
every per-fixture enrichment call (injuries / fixture_stats / fixture_events /
fixture_lineups / player_stats) — same core limit, different URL. So the budget
is allocated against the SOURCE (``api_football``), never per-endpoint; a VM's
adapter throttle is the single token bucket all its endpoint calls pass through.

**Two real ceilings — per-minute AND per-day** (operator 2026-06-23): the Custom
plan is ``1200 req/min`` AND a hard daily quota that resets to ZERO at ``00:00
UTC`` every day (unused requests are LOST — no rollover). The fleet ceiling is
therefore time-aware: the EFFECTIVE per-minute ceiling is
``min(per_minute_limit, remaining_daily_quota / minutes_until_reset)`` so that
when the daily budget is nearly spent the allocator THROTTLES the fleet below
1200/min automatically (rather than burning the remaining quota in minutes then
429-thrashing for the rest of the day). ``SOURCE_DAILY_QUOTA`` carries the
per-day ceiling; ``allocate_rate_budget`` consumes both.

**Query the live limit, don't hardcode (operator 2026-06-23)**: api-football
EXPOSES its real plan + usage live — ``GET /status`` returns
``requests.limit_day`` (the plan's per-day quota) + ``requests.current`` (today's
used), and the ``X-RateLimit-Limit`` response header carries the per-minute
ceiling. The instruments-service api-football adapter's
``get_live_quota()`` reads these and is the AUTHORITATIVE source the launcher +
monitor distribute/throttle against; the ``SOURCE_RATE_LIMITS_RPM`` /
``SOURCE_DAILY_QUOTA`` constants below are only the FALLBACK default when the live
read fails. The registry constants drift (they once said 450,000/day while the
API self-reported ``Custom300`` = 300,000/day) — so the live read wins, the
constant is corrected to the current reality (300,000/day) but is never trusted
over ``/status``. ``SOURCE_SUPPORTS_LIVE_QUOTA`` records which sources expose a
live limit read (api-football yes; understat/transfermarkt/footystats/etc. no →
static/calibrated registry constants stand alone).

**Hard rule (fail-closed)**: ``sum(per_vm_rpm * n_vms) <= effective_source_rpm``
AND ``fleet_rpm * minutes_to_reset <= remaining_daily_quota`` for every source
across all live VMs. ``assert_fleet_within_budget`` raises rather than let a
launch over-subscribe a source on EITHER axis (which would only produce
429-thrash).

The allocated ``per_vm_rpm`` reaches the adapter as the env var
``SPORTS_ADAPTER_RATE_RPM`` (read via the typed ``InstrumentsServiceConfig`` in
instruments-service — never a raw OS-env read); the adapter converts it to its
``_min_request_interval`` = ``60 / per_vm_rpm``. A live cross-VM shared token
store is the v2; the static ``limit // N`` allocation here is the accepted v1.

Part 2 — machine-sizing (``VENUE_TASK_MEMORY_TIER`` + ``MEMORY_TIER_LADDER``)
-----------------------------------------------------------------------------
A memory-heavy backfill should launch correctly-sized on the FIRST try — not
via repeated OOM-escalation. ``VENUE_TASK_MEMORY_TIER`` maps a
``(venue/task)`` key to a canonical memory tier; a launcher resolves the tier
→ a GCE machine-type from ``MEMORY_TIER_LADDER`` and sizes the VM from the
start. The OOM-escalation actuator (owned by a SEPARATE autonomy agent —
``escalation.py`` / the watchdog) CONSUMES the same ladder to step UP one rung
on a 137; this module only DEFINES the ladder + the correct-from-start default.

Guard: ``tests/unit/test_launch_budget_registry.py`` asserts the allocation
math, the fail-closed hard rule, the machine-sizing lookup (Coinbase cefi →
128/256 GB), and ladder monotonicity.

SSOT: ``plans/active/data_completion_to_100_all_ag_2026_06_21.md`` §
"Registry-driven launch parameters".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# ──────────────────────────────────────────────────────────────────────────
# Part 1 — API rate-budget registry: SOURCE → fleet-wide requests-per-minute.
# ──────────────────────────────────────────────────────────────────────────
#
# The per-key/per-subscription ceiling for each source. A launch splits this
# across N concurrent VMs (``allocate_rate_budget``) so the fleet runs at the
# full allowed throughput without ever exceeding it.
#
# ``None`` = no documented hard per-minute cap yet (TODO: confirm the vendor's
# real ceiling). A source with ``None`` is NOT rate-allocated — the adapter
# keeps its own class-default throttle until a real number lands here.
SOURCE_RATE_LIMITS_RPM: dict[str, int | None] = {
    # ── Sports ────────────────────────────────────────────────────────────
    # API-Football Custom plan = 1200 req/min, ONE quota shared across ALL
    # endpoints (fixtures + injuries + fixture_stats + fixture_events +
    # fixture_lineups + player_stats). 1200 is the operator-confirmed Custom-plan
    # ceiling (2026-06-23, from the API-Football dashboard — supersedes the prior
    # Mega-tier 900 misread). The Custom plan ALSO has a hard per-DAY quota
    # (see SOURCE_DAILY_QUOTA) that resets at 00:00 UTC — both ceilings are
    # real and ``allocate_rate_budget`` honours both (fail-closed: under-allocate,
    # never over-subscribe). NB: this is the FALLBACK default — the LIVE per-minute
    # ceiling is read from the ``X-RateLimit-Limit`` header via the adapter's
    # ``get_live_quota()`` (api_football is in SOURCE_SUPPORTS_LIVE_QUOTA); the
    # live read is authoritative.
    "api_football": 1200,
    # FootyStats — no published hard per-minute cap; the plan runs it
    # sequentially per-date today. Conservative default 60 req/min (~1 req/sec
    # politeness ceiling) until a real number lands.
    # TODO: empirically calibrate the FootyStats ceiling (a calibration probe
    # will measure the true cap and replace this conservative placeholder).
    "footystats": 60,
    # Understat — public scrape, no API quota; politeness-throttled at the
    # adapter. Conservative default 30 req/min (~0.5 req/sec scrape politeness).
    # TODO: empirically calibrate the Understat scrape ceiling.
    "understat": 30,
    # SoccerFootball-Info (SFI) — RapidAPI tier ≈ 4 req/sec ⇒ 240 req/min
    # (the adapter's _min_request_interval=0.34 ≈ 3 req/sec stays under it).
    # TODO: empirically calibrate against the actual RapidAPI plan headers.
    "soccer_football_info": 240,
    # Transfermarkt — RapidAPI scrape proxy; ~1 req/sec politeness ceiling.
    # TODO: empirically calibrate the actual RapidAPI plan cap.
    "transfermarkt": 60,
    # ── Cross-cutting / market data ─────────────────────────────────────────
    # Open-Meteo free tier = 10,000 calls/day ≈ soft; no documented per-minute
    # cap. Conservative default 60 req/min until confirmed.
    # TODO: empirically calibrate the Open-Meteo per-minute ceiling.
    "open_meteo": 60,
    # Databento — usage-based subscription; the billing-fail-closed allowlist
    # gates COST, not a req/min cap. No documented per-minute throttle to split.
    # TODO: confirm whether the Live/Historical API enforces a per-key rps.
    "databento": None,
    # Polymarket (CLOB + Gamma) — public endpoints; no published hard rpm cap.
    # TODO: confirm the real ceiling before fanning out a Polymarket swarm.
    "polymarket_clob": None,
    "polymarket_gamma_api": None,
    # TheGraph (DEX subgraphs) — keyed; the defi handlers round-robin a 9-key
    # SM pool. Effective ceiling ≈ 9 x per-key. TODO: pin the per-key rps and
    # express the pooled ceiling here so a defi launch can size against it.
    "thegraph": None,
}


# The per-DAY request ceiling for each source (the second real ceiling alongside
# the per-minute cap). Resets to ZERO at 00:00 UTC every day; UNUSED requests are
# LOST — no rollover. ``allocate_rate_budget`` divides the REMAINING daily quota
# across the minutes left until 00:00 UTC and takes the min against the per-minute
# cap, so a fleet launched late in the day throttles itself rather than exhaust
# the day's budget in minutes and 429-thrash thereafter.
#
# ``None`` = no documented hard per-day quota (the per-minute cap is the only
# ceiling) — such a source is NOT daily-throttled (the time-aware term is skipped
# and the per-minute cap stands alone).
SOURCE_DAILY_QUOTA: dict[str, int | None] = {
    # API-Football Custom plan: per-day quota resets 00:00 UTC, no rollover.
    # 300,000/day is the CURRENT reality (the API self-reports ``Custom300`` =
    # 300,000/day, ~85k remaining mid-day; corrected from the stale 450,000
    # 2026-06-23). This is the FALLBACK default only — the LIVE daily limit +
    # remaining are read from ``GET /status`` (``requests.limit_day`` /
    # ``requests.current``) via the adapter's ``get_live_quota()`` and are
    # AUTHORITATIVE; this constant is consulted only when the live read fails.
    "api_football": 300_000,
    # Everything else: no documented per-day quota → per-minute cap is the only
    # ceiling. (Add a real number here when a vendor's daily quota is confirmed.)
    "footystats": None,
    "understat": None,
    "soccer_football_info": None,
    "transfermarkt": None,
    "open_meteo": None,  # 10k calls/day is a SOFT free-tier hint, not a hard cap
    "databento": None,
    "polymarket_clob": None,
    "polymarket_gamma_api": None,
    "thegraph": None,
}


# Sources that EXPOSE their real plan limit + usage via a live API read, so the
# allocator/monitor should prefer the live figures over the static registry
# constants above (the constants are then only the fallback when the live read
# fails). "Query, don't hardcode" (operator 2026-06-23).
#
#   * api_football — ``GET /status`` returns ``requests.limit_day`` +
#     ``requests.current`` (daily), and the ``X-RateLimit-Limit`` response header
#     gives the per-minute ceiling. Read via the instruments-service adapter's
#     ``ApiFootballAdapter.get_live_quota()``.
#
# Everything else (understat / transfermarkt / footystats / soccer_football_info
# / open_meteo / databento / polymarket / thegraph) does NOT expose a live limit
# endpoint — their ceilings are static/calibrated registry constants (some via the
# ramp-to-429 probe ``scripts/calibrate_source_rate_limit.py``), used as-is.
SOURCE_SUPPORTS_LIVE_QUOTA: frozenset[str] = frozenset({"api_football"})


def source_supports_live_quota(source: str) -> bool:
    """True if ``source`` exposes a live plan-limit read (prefer it over constants)."""
    return source in SOURCE_SUPPORTS_LIVE_QUOTA


# ──────────────────────────────────────────────────────────────────────────
# Per-IP sources — the rate ceiling is PER SOURCE-IP, NOT a shared fleet quota
# (Part 3, operator 2026-06-23). Databento + Polymarket throttle by SOURCE IP,
# so each VM (its own egress IP) gets the FULL per-IP ceiling — you scale
# throughput by adding IPs/VMs, NOT by dividing one ceiling across the fleet.
# A source listed here MUST NOT be allocated via ``allocate_rate_budget``
# (which DIVIDES a fleet ceiling) — use ``per_ip_rate_for_source`` so the
# allocator treats every VM independently.
#
# ``rpm`` = the measured per-IP requests-per-minute ceiling (with safety margin
# already applied — typically ~80% of the empirical break point). ``calibrated``
# is False where the number is a conservative pre-calibration placeholder (the
# ramp-to-429 probe — scripts/calibrate_source_rate_limit.py — replaces it with
# the measured value). ``None`` rpm = no per-IP throttle observed (cost-gated
# only, e.g. Databento's usage billing).
@dataclass(frozen=True)
class PerIpLimit:
    """A per-source-IP rate ceiling (each VM/IP gets the FULL value, not a share)."""

    rpm: int | None
    calibrated: bool
    note: str


SOURCE_PER_IP_LIMITS: dict[str, PerIpLimit] = {
    # Databento — usage-BILLED (cost is the constraint, gated by the
    # billing-allowlist), and any transport throttle is PER-IP not a shared
    # fleet quota: each streaming/live VM respects its own per-IP connection +
    # request budget, so adding VMs (each a distinct IP) scales linearly. No
    # documented hard per-IP req/min cap surfaced yet; the ramp probe confirms
    # the per-IP ceiling (or that billing is the only limit). Modelled per-IP so
    # the allocator never divides one ceiling across the Databento fleet.
    "databento": PerIpLimit(rpm=None, calibrated=False, note="usage-billed; per-IP transport, scale via more IPs"),
    # Polymarket (CLOB + Gamma) — public endpoints, LIKELY per-IP throttled
    # (operator: "probe it"). Conservative pre-calibration placeholder 600/min
    # per IP (~10 req/sec politeness) until the ramp probe measures the real
    # per-IP break point. Per-IP: each Polymarket VM uses the full ceiling.
    "polymarket_clob": PerIpLimit(rpm=600, calibrated=False, note="likely per-IP; PLACEHOLDER until ramp probe"),
    "polymarket_gamma_api": PerIpLimit(rpm=600, calibrated=False, note="likely per-IP; PLACEHOLDER until ramp probe"),
}


# ──────────────────────────────────────────────────────────────────────────
# Key-pool sources — capacity = per-key ceiling x pool size (Part 4). The Graph
# is NOT a single per-IP/per-minute cap: it shards across a Secret-Manager pool
# of API keys (round-robin), so the EFFECTIVE fleet ceiling is
# ``per_key_rpm x pool_size``. The DeFi subgraph handlers already round-robin
# the 9-key ``thegraph-api-key[-2..9]`` pool per request
# (``thegraph_base_client.next_thegraph_key_from_pool`` / ``ThegraphKeyPoolRotator``);
# this registry MODELS that capacity so a DeFi subgraph launch can size its VM
# fan-out + per-VM shard against ``per_key_rpm x pool_size`` rather than a single
# key's budget. A launcher distributes pool keys across VMs/shards (key_number =
# shard_index % pool_size + 1) so no single key is over-driven.
@dataclass(frozen=True)
class KeyPoolLimit:
    """A keyed source whose ceiling = ``per_key_rpm x pool_size`` (round-robin pool)."""

    per_key_rpm: int
    pool_size: int
    calibrated: bool
    note: str

    @property
    def effective_rpm(self) -> int:
        """Pooled fleet ceiling = per-key ceiling x pool size."""
        return self.per_key_rpm * self.pool_size


SOURCE_KEY_POOL_LIMITS: dict[str, KeyPoolLimit] = {
    # TheGraph gateway (DEX subgraphs) — the gateway's free-plan budget is
    # ~per-key; the 9-key SM pool (thegraph-api-key, -2..-9) is round-robined so
    # effective capacity ≈ 9 x per-key. The per-key rpm is a conservative
    # placeholder pending a ramp probe per key; pool_size mirrors
    # thegraph_base_client._THEGRAPH_NUM_API_KEYS (9). Handlers rotate keys per
    # request; launchers shard keys across VMs.
    "thegraph": KeyPoolLimit(
        per_key_rpm=300,
        pool_size=9,
        calibrated=False,
        note="9-key SM pool round-robin; per-key PLACEHOLDER pending ramp probe; effective=per_keyx9",
    ),
}


def per_ip_rate_for_source(source: str) -> PerIpLimit | None:
    """Return the per-IP limit for a per-IP source, or ``None`` if not per-IP.

    A per-IP source is NOT fleet-divided: each VM/IP runs at the full ``rpm``.
    """
    return SOURCE_PER_IP_LIMITS.get(source)


def key_pool_capacity_for_source(source: str) -> KeyPoolLimit | None:
    """Return the key-pool capacity model for a keyed source, or ``None``.

    Effective fleet ceiling = ``per_key_rpm x pool_size`` (round-robin pool).
    """
    return SOURCE_KEY_POOL_LIMITS.get(source)


def _minutes_until_utc_midnight(now_utc: datetime) -> float:
    """Minutes from ``now_utc`` until the next 00:00 UTC (the daily-quota reset).

    Always strictly positive (a value just shy of 1440 right after midnight, ~1
    just before). ``now_utc`` MUST be timezone-aware UTC.
    """
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware UTC, got a naive datetime")
    now_utc = now_utc.astimezone(UTC)
    next_midnight = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (next_midnight - now_utc).total_seconds() / 60.0


def _effective_source_rpm(
    source: str,
    per_minute_limit: int,
    *,
    remaining_daily_quota: int | None,
    minutes_to_reset: float,
) -> int:
    """The time-aware fleet ceiling = ``min(per_minute_limit, remaining/minutes)``.

    When the source has a daily quota and ``remaining_daily_quota`` is given, the
    daily budget is amortised across the minutes left until 00:00 UTC; the lower
    of that and the raw per-minute cap is the effective ceiling. With no daily
    quota (or no remaining figure supplied) the per-minute cap stands alone.
    """
    daily_quota = SOURCE_DAILY_QUOTA.get(source)
    if daily_quota is None or remaining_daily_quota is None:
        return per_minute_limit
    if remaining_daily_quota < 0:
        raise ValueError(f"remaining_daily_quota must be >= 0 (got {remaining_daily_quota})")
    if minutes_to_reset <= 0:
        raise ValueError(f"minutes_to_reset must be > 0 (got {minutes_to_reset})")
    daily_rpm = int(remaining_daily_quota // minutes_to_reset)
    return min(per_minute_limit, daily_rpm)


@dataclass(frozen=True)
class RateBudgetAllocation:
    """The launch-time rate allocation for one VM of a fleet of ``n_vms``.

    ``per_vm_rpm`` is the deterministic share of the EFFECTIVE source ceiling this
    VM may spend; ``concurrency`` is the matched in-flight task cap; and
    ``min_request_interval_s`` = ``60 / per_vm_rpm`` is the adapter's
    self-enforced token-bucket spacing (the PRIMARY throttle).

    ``source_rpm`` is the raw per-minute cap; ``effective_source_rpm`` is the
    daily-quota-aware ceiling actually split across the fleet (== ``source_rpm``
    when the source has no daily quota / late in the day none is supplied, lower
    when the remaining daily budget can't sustain the full per-minute rate to
    00:00 UTC). ``minutes_to_reset`` is the UTC-midnight horizon used.

    ``per_vm_daily_quota`` is this VM's share of the source's FULL per-day ceiling
    (``SOURCE_DAILY_QUOTA[source] // n_vms``, ``None`` when the source has no daily
    quota) — the adapter consumes it as its per-UTC-day fixed-window cap (resets at
    00:00 UTC, matching the provider) via ``set_window_quota(per_day=...)``. This is
    the FULL daily share for the adapter's own UTC-day counter, distinct from the
    time-amortised ``effective_source_rpm`` the allocator uses to size the per-minute
    fleet split late in the day.
    """

    source: str
    source_rpm: int
    effective_source_rpm: int
    n_vms: int
    per_vm_rpm: int
    per_vm_daily_quota: int | None
    concurrency: int
    min_request_interval_s: float
    remaining_daily_quota: int | None
    minutes_to_reset: float


def allocate_rate_budget(
    source: str,
    n_vms: int,
    *,
    max_per_query_rate_rpm: int | None = None,
    concurrency_cap: int = 16,
    remaining_daily_quota: int | None = None,
    now_utc: datetime | None = None,
    minutes_to_reset: float | None = None,
) -> RateBudgetAllocation:
    """Deterministically split a source's EFFECTIVE fleet ceiling across ``n_vms``.

    The effective ceiling is daily-quota- AND time-aware:
    ``effective_source_rpm = min(per_minute_limit, remaining_daily_quota /
    minutes_until_00:00_UTC)``. So when the day's budget is nearly spent the
    fleet is throttled below the raw per-minute cap automatically. With no daily
    quota for the source (or none supplied) the per-minute cap stands alone.

    ``per_vm_rpm = effective_source_rpm // n_vms`` — the fleet then runs at (at
    most) the full effective ceiling and never over-subscribes it on EITHER axis.
    The matched per-VM concurrency is sized so
    ``concurrency x max_per_query_rate ≤ per_vm_rpm``.

    Worked examples (the operator's API-Football case, Custom plan = 1200/min,
    450,000/day, resets 00:00 UTC):

    * **Late-in-day, daily budget nearly spent** — ``remaining_daily_quota ≈
      130,500`` with ``~270`` minutes to reset ⇒ ``daily_rpm = 130500 // 270 =
      483`` < 1200 ⇒ ``effective_source_rpm = 483``. With 5 VMs ⇒
      ``per_vm_rpm = 483 // 5 = 96`` (≈ the operator's "~5 VMs at ~90 rpm");
      ``min_request_interval_s = 60/96 ≈ 0.625s``.
    * **Post-reset, fresh 450,000/day** — just after 00:00 UTC ``remaining =
      450,000`` with ``~1440`` minutes to reset ⇒ ``daily_rpm = 450000 // 1440 =
      312``… but the operator runs the fleet against the FULL per-minute cap when
      the day is fresh (the daily budget is not the binding constraint over the
      whole day): pass no ``remaining_daily_quota`` (or a value large enough that
      the per-minute cap binds) ⇒ ``effective_source_rpm = 1200`` ⇒ with 13 VMs
      ``per_vm_rpm = 1200 // 13 = 92`` (~13 VMs at ~90 rpm = the full 1200/min).
    * **No daily input (legacy callers)** — ``effective_source_rpm = 1200``;
      10 VMs ⇒ 120 each, ``max_per_query_rate_rpm = 12`` ⇒ concurrency
      ``120 // 12 = 10``.

    Args:
        source: A key in ``SOURCE_RATE_LIMITS_RPM`` with a non-None ceiling.
        n_vms: How many VMs will run concurrently against this source.
        max_per_query_rate_rpm: The req/min a SINGLE in-flight task can sustain
            (used to size matched concurrency). Defaults to ``per_vm_rpm``
            (one task saturates the per-VM budget → concurrency 1), the safe
            floor; pass a smaller per-task rate to widen concurrency.
        concurrency_cap: Upper bound on matched concurrency (I/O-bound default 16
            per the workspace concurrency rule).
        remaining_daily_quota: Requests left in the source's per-day quota right
            now. When given (and the source has a ``SOURCE_DAILY_QUOTA``), the
            allocator amortises it across the minutes to 00:00 UTC and may
            throttle the fleet below the per-minute cap. ``None`` = the per-minute
            cap is the only constraint.
        now_utc: The current time (timezone-aware UTC) used to compute the
            minutes-to-reset. Defaults to ``datetime.now(UTC)`` at call
            time. Inject a fixed value in tests.
        minutes_to_reset: Pre-computed minutes until 00:00 UTC (overrides
            ``now_utc``). Pass this OR ``now_utc``, not both meaningfully.

    Raises:
        ValueError: ``source`` unknown / un-capped, ``n_vms < 1``, or the
            effective ceiling rounds the per-VM budget to 0.
    """
    if n_vms < 1:
        raise ValueError(f"n_vms must be >= 1 (got {n_vms})")
    if source in SOURCE_PER_IP_LIMITS:
        raise ValueError(
            f"source {source!r} is PER-IP (in SOURCE_PER_IP_LIMITS) — its ceiling is per-IP, "
            "NOT a shared fleet quota to divide. Each VM/IP gets the full per-IP rate; use "
            "per_ip_rate_for_source() and scale by adding IPs, never allocate_rate_budget()."
        )
    source_rpm = SOURCE_RATE_LIMITS_RPM.get(source)
    if source_rpm is None:
        raise ValueError(
            f"source {source!r} has no fleet rate ceiling in SOURCE_RATE_LIMITS_RPM "
            "(unknown or TODO-unconfirmed) — cannot allocate a rate budget. "
            "Add its req/min cap to the registry first."
        )
    if minutes_to_reset is None:
        minutes_to_reset = _minutes_until_utc_midnight(now_utc or datetime.now(UTC))
    effective_rpm = _effective_source_rpm(
        source,
        source_rpm,
        remaining_daily_quota=remaining_daily_quota,
        minutes_to_reset=minutes_to_reset,
    )
    per_vm_rpm = effective_rpm // n_vms
    if per_vm_rpm < 1:
        raise ValueError(
            f"n_vms={n_vms} exceeds effective_source_rpm={effective_rpm} for {source!r} "
            "(per-VM budget would round to 0 req/min — daily quota nearly spent or too "
            "many VMs) — launch fewer VMs or wait for the 00:00 UTC reset."
        )
    per_task_rpm = max_per_query_rate_rpm if max_per_query_rate_rpm is not None else per_vm_rpm
    if per_task_rpm < 1:
        raise ValueError(f"max_per_query_rate_rpm must be >= 1 (got {per_task_rpm})")
    concurrency = max(1, min(concurrency_cap, per_vm_rpm // per_task_rpm))
    _daily_quota = SOURCE_DAILY_QUOTA.get(source)
    per_vm_daily_quota = (_daily_quota // n_vms) if _daily_quota is not None else None
    return RateBudgetAllocation(
        source=source,
        source_rpm=source_rpm,
        effective_source_rpm=effective_rpm,
        n_vms=n_vms,
        per_vm_rpm=per_vm_rpm,
        per_vm_daily_quota=per_vm_daily_quota,
        concurrency=concurrency,
        min_request_interval_s=round(60.0 / per_vm_rpm, 4),
        remaining_daily_quota=remaining_daily_quota,
        minutes_to_reset=round(minutes_to_reset, 2),
    )


def assert_fleet_within_budget(
    source: str,
    n_vms: int,
    per_vm_rpm: int,
    *,
    remaining_daily_quota: int | None = None,
    now_utc: datetime | None = None,
    minutes_to_reset: float | None = None,
) -> None:
    """Fail-closed hard rule: the fleet must not over-subscribe a source.

    Asserts BOTH ceilings:
        * per-minute: ``per_vm_rpm * n_vms <= source_rpm``;
        * per-day (when a daily quota + ``remaining_daily_quota`` are supplied):
          the PROJECTED daily consumption to the next reset
          ``fleet_rpm * minutes_to_reset <= remaining_daily_quota``.

    A launcher calls this with the rate it is ABOUT to stamp on N VMs; on
    violation it raises so the launch is refused / scaled down rather than
    producing a 429-thrash storm (per-minute) or burning the daily quota early.

    Raises:
        ValueError: ``source`` unknown / un-capped.
        FleetBudgetExceededError: the allocation would exceed the per-minute OR
            the projected per-day ceiling.
    """
    source_rpm = SOURCE_RATE_LIMITS_RPM.get(source)
    if source_rpm is None:
        raise ValueError(
            f"source {source!r} has no fleet rate ceiling in SOURCE_RATE_LIMITS_RPM — cannot validate a fleet budget."
        )
    fleet_rpm = per_vm_rpm * n_vms
    if fleet_rpm > source_rpm:
        raise FleetBudgetExceededError(
            f"fleet budget for {source!r} over-subscribed (per-minute): {n_vms} VMs x {per_vm_rpm} req/min "
            f"= {fleet_rpm} > source ceiling {source_rpm} req/min. Reduce n_vms or per_vm_rpm."
        )
    daily_quota = SOURCE_DAILY_QUOTA.get(source)
    if daily_quota is not None and remaining_daily_quota is not None:
        if minutes_to_reset is None:
            minutes_to_reset = _minutes_until_utc_midnight(now_utc or datetime.now(UTC))
        projected = fleet_rpm * minutes_to_reset
        if projected > remaining_daily_quota:
            raise FleetBudgetExceededError(
                f"fleet budget for {source!r} over-subscribed (daily): {n_vms} VMs x {per_vm_rpm} req/min "
                f"= {fleet_rpm} req/min projected over {minutes_to_reset:.1f} min to 00:00 UTC reset "
                f"= {projected:.0f} req > remaining daily quota {remaining_daily_quota} "
                f"(of {daily_quota}/day). Reduce n_vms or per_vm_rpm, or wait for the reset."
            )


class FleetBudgetExceededError(RuntimeError):
    """A launch would push a source's aggregate req/min over its fleet ceiling.

    Raised on EITHER axis: the per-minute cap or the projected per-day quota.
    """


# ──────────────────────────────────────────────────────────────────────────
# Part 2 — machine-sizing registry: canonical memory-tier ladder + venue/task map.
# ──────────────────────────────────────────────────────────────────────────
#
# The canonical memory-tier ladder (the SSOT the OOM-escalation actuator climbs
# one rung at a time). Each tier name → (gce_machine_type, ram_gb). Ordered
# ascending by RAM; ``next_memory_tier`` steps UP this ladder on a 137.
@dataclass(frozen=True)
class MemoryTier:
    """One rung of the canonical memory-tier ladder."""

    name: str
    machine_type: str
    ram_gb: int


# Ascending by RAM. The OOM actuator consumes this via ``next_memory_tier``.
MEMORY_TIER_LADDER: tuple[MemoryTier, ...] = (
    MemoryTier("standard-16gb", "e2-standard-4", 16),
    MemoryTier("standard-32gb", "e2-standard-8", 32),
    MemoryTier("standard-64gb", "n2-standard-16", 64),
    MemoryTier("highmem-128gb", "n2-highmem-16", 128),
    MemoryTier("highmem-256gb", "n2-highmem-32", 256),
)

_TIER_BY_NAME: dict[str, MemoryTier] = {t.name: t for t in MEMORY_TIER_LADDER}
_TIER_INDEX: dict[str, int] = {t.name: i for i, t in enumerate(MEMORY_TIER_LADDER)}

# venue/task key → starting memory tier (so a heavy backfill launches
# correctly-sized on the FIRST try, not via repeated OOM-escalation). The key
# is a lowercase ``"<task>:<venue>"`` or a bare ``"<venue>"`` / ``"<task>"`` —
# resolution is longest-match-then-fallback (see ``resolve_memory_tier``).
#
# Coinbase cefi is the operator's named case: it needs 128 GB, and 256 GB for
# the heaviest date ranges — so it MUST start at highmem-128gb (heavy years
# step to highmem-256gb via the env override or the OOM actuator).
VENUE_TASK_MEMORY_TIER: dict[str, str] = {
    # ── CeFi market-data backfill (Tardis streaming — memory-heavy) ──────────
    "cefi-backfill:coinbase": "highmem-128gb",  # operator 2026-06-23: 128 GB, 256 for heavy ranges
    "cefi-backfill:binance": "highmem-128gb",  # bull-market book_snapshot_5 peaks > 64 GB
    "cefi-backfill:bybit": "highmem-128gb",
    "cefi-backfill:deribit": "highmem-128gb",  # options_chain expiry days 38 GB+ peak
    "cefi-backfill:okx": "highmem-128gb",
    # ── Sports backfill (fixtures-catalogue + per-fixture footprint) ─────────
    # OOM'd on e2-standard-2 (8 GB) 2026-06-22 → default to 32 GB; understat
    # needs ≥ standard-32gb (≥ e2-standard-4 confirmed too small for the
    # full-season scrape footprint per the plan progress log).
    "sports-backfill": "standard-32gb",
    "sports-backfill:understat": "standard-32gb",
    "sports-backfill:transfermarkt": "standard-32gb",
    "sports-backfill:footystats": "standard-32gb",
    "sports-backfill:api_football": "standard-32gb",
}

# Conservative fleet default for any venue/task NOT in the map above.
DEFAULT_MEMORY_TIER: str = "standard-32gb"


# Venue names carry a market suffix in the launchers (COINBASE-SPOT,
# COINBASE-FUTURES, OKX-SWAP, …) but the registry keys on the BASE venue. These
# suffixes are stripped so ``coinbase-spot`` / ``coinbase-futures`` both resolve
# to the ``coinbase`` tier.
_VENUE_MARKET_SUFFIXES: tuple[str, ...] = ("-spot", "-futures", "-swap", "-perp", "-perpetual", "-options")


def _base_venue(venue_l: str) -> str:
    """Strip a trailing market suffix so COINBASE-SPOT → coinbase."""
    for suffix in _VENUE_MARKET_SUFFIXES:
        if venue_l.endswith(suffix):
            return venue_l[: -len(suffix)]
    return venue_l


def resolve_memory_tier(task: str | None = None, venue: str | None = None) -> MemoryTier:
    """Resolve the starting memory tier for a ``(task, venue)`` launch.

    Resolution order (most-specific first), trying both the venue as-given AND
    its base form (market suffix stripped — COINBASE-SPOT → coinbase):
        1. ``"<task>:<venue>"`` / ``"<task>:<base_venue>"``
        2. ``"<venue>"`` / ``"<base_venue>"``
        3. ``"<task>"``
        4. ``DEFAULT_MEMORY_TIER``

    Args/keys are lowercased. Coinbase cefi (any market) resolves to
    ``highmem-128gb``.
    """
    task_l = (task or "").strip().lower()
    venue_l = (venue or "").strip().lower()
    base_l = _base_venue(venue_l)
    venue_forms = [v for v in (venue_l, base_l) if v]
    # De-dupe while preserving order (venue_l before base_l).
    seen: set[str] = set()
    venue_forms = [v for v in venue_forms if not (v in seen or seen.add(v))]

    candidates: list[str] = []
    for v in venue_forms:
        if task_l:
            candidates.append(f"{task_l}:{v}")
    candidates.extend(venue_forms)
    if task_l:
        candidates.append(task_l)
    for key in candidates:
        tier_name = VENUE_TASK_MEMORY_TIER.get(key)
        if tier_name is not None:
            return _TIER_BY_NAME[tier_name]
    return _TIER_BY_NAME[DEFAULT_MEMORY_TIER]


def machine_type_for(task: str | None = None, venue: str | None = None) -> str:
    """Return the GCE machine-type a ``(task, venue)`` launch should start on."""
    return resolve_memory_tier(task=task, venue=venue).machine_type


def next_memory_tier(current_tier_name: str) -> MemoryTier | None:
    """Step UP one rung of the ladder (the OOM-escalation actuator's input).

    Returns the next-larger tier, or ``None`` if already at the top
    (``highmem-256gb`` — the actuator then files an issue rather than escalate
    past the ceiling). ``current_tier_name`` must be a known tier name.

    Raises:
        ValueError: ``current_tier_name`` is not a known tier.
    """
    idx = _TIER_INDEX.get(current_tier_name)
    if idx is None:
        raise ValueError(f"unknown memory tier {current_tier_name!r}; known: {sorted(_TIER_INDEX)}")
    if idx + 1 >= len(MEMORY_TIER_LADDER):
        return None
    return MEMORY_TIER_LADDER[idx + 1]


def memory_tier_for_machine_type(machine_type: str) -> MemoryTier | None:
    """Reverse-lookup the ladder rung for a GCE machine-type (None if off-ladder)."""
    for tier in MEMORY_TIER_LADDER:
        if tier.machine_type == machine_type:
            return tier
    return None


# Known GCE machine-type → RAM (GiB), including OFF-LADDER types the cefi/sports
# launchers already use (e2-highmem-*). Used by ``gce_machine_ram_gb`` so a
# registry tier can be compared against a launcher's existing default machine
# even when that default isn't a ladder rung. Computed from GCP's published
# per-machine memory; extend as launchers introduce new types.
_GCE_MACHINE_RAM_GB: dict[str, int] = {
    # e2 standard (4 GB/vCPU)
    "e2-standard-2": 8,
    "e2-standard-4": 16,
    "e2-standard-8": 32,
    "e2-standard-16": 64,
    # e2 highmem (8 GB/vCPU)
    "e2-highmem-2": 16,
    "e2-highmem-4": 32,
    "e2-highmem-8": 64,
    "e2-highmem-16": 128,
    # n2 standard (4 GB/vCPU)
    "n2-standard-16": 64,
    "n2-standard-32": 128,
    # n2 highmem (8 GB/vCPU)
    "n2-highmem-16": 128,
    "n2-highmem-32": 256,
}


def gce_machine_ram_gb(machine_type: str) -> int:
    """RAM (GiB) for a GCE machine-type, ladder OR off-ladder; 0 if unknown.

    Lets a launcher compare its existing default machine (possibly off-ladder,
    e.g. ``e2-highmem-16`` = 128 GB) against a registry tier without a false
    downgrade. Falls back to the ladder lookup, then 0.
    """
    ram = _GCE_MACHINE_RAM_GB.get(machine_type)
    if ram is not None:
        return ram
    tier = memory_tier_for_machine_type(machine_type)
    return tier.ram_gb if tier is not None else 0
