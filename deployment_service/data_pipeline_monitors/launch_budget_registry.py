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
900 req/min Mega-plan cap is ONE subscription quota spent by fixtures AND every
per-fixture enrichment call (injuries / fixture_stats / fixture_events /
fixture_lineups / player_stats) — same core limit, different URL. So the budget
is allocated against the SOURCE (``api_football``), never per-endpoint; a VM's
adapter throttle is the single token bucket all its endpoint calls pass through.

**Hard rule (fail-closed)**: ``sum(per_vm_rpm * n_vms) <= source_rpm`` for every
source across all live VMs. ``assert_fleet_within_budget`` raises rather than
let a launch over-subscribe a source (which would only produce 429-thrash).

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
    # API-Football Mega plan = 900 req/min, ONE quota shared across ALL endpoints
    # (fixtures + injuries + fixture_stats + fixture_events + fixture_lineups +
    # player_stats). 900 is the value DOCUMENTED in the adapter
    # (api_football.py:154 "Mega tier rate limit: 900 req/min"); the operator is
    # still confirming a possible higher tier — until confirmed, 900 is the
    # authoritative ceiling (fail-closed: under-allocate, never over-subscribe).
    "api_football": 900,
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


@dataclass(frozen=True)
class RateBudgetAllocation:
    """The launch-time rate allocation for one VM of a fleet of ``n_vms``.

    ``per_vm_rpm`` is the deterministic share of the source ceiling this VM may
    spend; ``concurrency`` is the matched in-flight task cap; and
    ``min_request_interval_s`` = ``60 / per_vm_rpm`` is the adapter's
    self-enforced token-bucket spacing (the PRIMARY throttle).
    """

    source: str
    source_rpm: int
    n_vms: int
    per_vm_rpm: int
    concurrency: int
    min_request_interval_s: float


def allocate_rate_budget(
    source: str,
    n_vms: int,
    *,
    max_per_query_rate_rpm: int | None = None,
    concurrency_cap: int = 16,
) -> RateBudgetAllocation:
    """Deterministically split a source's fleet ceiling across ``n_vms`` VMs.

    ``per_vm_rpm = source_rpm // n_vms`` — the fleet then runs at (at most) the
    full ``source_rpm`` and never over-subscribes it. The matched per-VM
    concurrency is sized so ``concurrency x max_per_query_rate ≤ per_vm_rpm``:
    enough in-flight tasks to keep the pipe full at the allocated rate, never so
    many that a burst could exceed the budget.

    Worked example (the operator's case): 10 API-Football VMs against the
    900 req/min source ceiling ⇒ ``per_vm_rpm = 90`` each (10 x 90 = 900,
    exactly the ceiling, 0 waste); ``min_request_interval_s = 60/90 ≈ 0.6667s``;
    with ``max_per_query_rate_rpm = 90`` (one in-flight request streams the full
    per-VM budget) ``concurrency = max(1, 90 // 90) = 1`` capped at
    ``concurrency_cap``. A looser ``max_per_query_rate_rpm = 12`` (the operator's
    "each slot ~12 req/min" framing) yields ``concurrency = 90 // 12 = 7`` →
    10 VMs x concurrency 7 = 70 in-flight against 900/min, the operator's
    stated steady state.

    Args:
        source: A key in ``SOURCE_RATE_LIMITS_RPM`` with a non-None ceiling.
        n_vms: How many VMs will run concurrently against this source.
        max_per_query_rate_rpm: The req/min a SINGLE in-flight task can sustain
            (used to size matched concurrency). Defaults to ``per_vm_rpm``
            (one task saturates the per-VM budget → concurrency 1), which is the
            safe floor; pass a smaller per-task rate to widen concurrency.
        concurrency_cap: Upper bound on matched concurrency (I/O-bound default 16
            per the workspace concurrency rule).

    Raises:
        ValueError: ``source`` unknown / un-capped, or ``n_vms < 1``.
    """
    if n_vms < 1:
        raise ValueError(f"n_vms must be >= 1 (got {n_vms})")
    source_rpm = SOURCE_RATE_LIMITS_RPM.get(source)
    if source_rpm is None:
        raise ValueError(
            f"source {source!r} has no fleet rate ceiling in SOURCE_RATE_LIMITS_RPM "
            "(unknown or TODO-unconfirmed) — cannot allocate a rate budget. "
            "Add its req/min cap to the registry first."
        )
    per_vm_rpm = source_rpm // n_vms
    if per_vm_rpm < 1:
        raise ValueError(
            f"n_vms={n_vms} exceeds source_rpm={source_rpm} for {source!r} "
            "(per-VM budget would round to 0 req/min) — launch fewer VMs."
        )
    per_task_rpm = max_per_query_rate_rpm if max_per_query_rate_rpm is not None else per_vm_rpm
    if per_task_rpm < 1:
        raise ValueError(f"max_per_query_rate_rpm must be >= 1 (got {per_task_rpm})")
    concurrency = max(1, min(concurrency_cap, per_vm_rpm // per_task_rpm))
    return RateBudgetAllocation(
        source=source,
        source_rpm=source_rpm,
        n_vms=n_vms,
        per_vm_rpm=per_vm_rpm,
        concurrency=concurrency,
        min_request_interval_s=round(60.0 / per_vm_rpm, 4),
    )


def assert_fleet_within_budget(source: str, n_vms: int, per_vm_rpm: int) -> None:
    """Fail-closed hard rule: the fleet must not over-subscribe a source.

    Asserts ``per_vm_rpm * n_vms <= source_rpm``. A launcher calls this with the
    rate it is ABOUT to stamp on N VMs; on violation it raises so the launch is
    refused / scaled down rather than producing a 429-thrash storm.

    Raises:
        ValueError: ``source`` unknown / un-capped.
        FleetBudgetExceededError: the allocation would exceed the source ceiling.
    """
    source_rpm = SOURCE_RATE_LIMITS_RPM.get(source)
    if source_rpm is None:
        raise ValueError(
            f"source {source!r} has no fleet rate ceiling in SOURCE_RATE_LIMITS_RPM — cannot validate a fleet budget."
        )
    total = per_vm_rpm * n_vms
    if total > source_rpm:
        raise FleetBudgetExceededError(
            f"fleet budget for {source!r} over-subscribed: {n_vms} VMs x {per_vm_rpm} req/min "
            f"= {total} > source ceiling {source_rpm} req/min. Reduce n_vms or per_vm_rpm."
        )


class FleetBudgetExceededError(RuntimeError):
    """A launch would push a source's aggregate req/min over its fleet ceiling."""


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
