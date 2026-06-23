"""Guard + behaviour tests for the registry-driven launch-parameter module.

Covers the two registries the operator designed 2026-06-23 to REPLACE reactive
backoff / OOM-escalation as the PRIMARY mechanism:

Part 1 — API rate-budget:
    - ``allocate_rate_budget`` splits a source ceiling deterministically
      (per_vm_rpm = effective_source_rpm // n_vms), with matched concurrency;
    - the worked API-Football examples (Custom plan = 1200/min + 450k/day);
    - the daily-quota- AND time-aware effective ceiling (late-in-day throttle +
      post-reset full 1200/min);
    - the fail-closed hard rule (``assert_fleet_within_budget`` raises on
      per-minute OR projected-per-day over-subscription);
    - API-Football's ONE-quota-across-all-endpoints fact (the source, not the
      endpoint, is the budgeted unit).

Part 2 — machine-sizing:
    - Coinbase cefi resolves to the 128 GB tier (NOT a small default);
    - the canonical ladder is RAM-monotonic with the operator's exact rungs;
    - ``next_memory_tier`` steps up one rung and stops at the 256 GB top.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from deployment_service.data_pipeline_monitors.launch_budget_registry import (
    DEFAULT_MEMORY_TIER,
    MEMORY_TIER_LADDER,
    SOURCE_DAILY_QUOTA,
    SOURCE_KEY_POOL_LIMITS,
    SOURCE_PER_IP_LIMITS,
    SOURCE_RATE_LIMITS_RPM,
    FleetBudgetExceededError,
    allocate_rate_budget,
    assert_fleet_within_budget,
    gce_machine_ram_gb,
    key_pool_capacity_for_source,
    machine_type_for,
    memory_tier_for_machine_type,
    next_memory_tier,
    per_ip_rate_for_source,
    resolve_memory_tier,
)


# ──────────────────────────────────────────────────────────────────────────
# Part 1 — rate-budget allocation.
# ──────────────────────────────────────────────────────────────────────────
def test_api_football_ceiling_is_1200() -> None:
    """API-Football's per-minute fleet ceiling is the Custom-plan 1200/min.

    Operator-confirmed 2026-06-23 from the API-Football dashboard (supersedes the
    prior Mega-tier 900 misread). Fail-closed: under-allocate, never
    over-subscribe.
    """
    assert SOURCE_RATE_LIMITS_RPM["api_football"] == 1200


def test_api_football_daily_quota_is_450k() -> None:
    """API-Football's per-DAY quota is 450,000 (resets 00:00 UTC, no rollover)."""
    assert SOURCE_DAILY_QUOTA["api_football"] == 450_000


def test_api_football_worked_example_10_vms() -> None:
    """10 API-Football VMs (no daily input) → 120 req/min each, full 1200/min, 0 waste.

    With max_per_query_rate_rpm=12 ("each slot ~12 req/min"), concurrency =
    120 // 12 = 10 → 10 VMs × concurrency 10 = 100 in-flight against the 1200/min
    ceiling. No remaining_daily_quota supplied ⇒ effective ceiling == 1200.
    """
    alloc = allocate_rate_budget("api_football", n_vms=10, max_per_query_rate_rpm=12)
    assert alloc.effective_source_rpm == 1200
    assert alloc.per_vm_rpm == 120
    assert alloc.per_vm_rpm * alloc.n_vms == 1200  # exactly the ceiling — 0 waste
    assert alloc.concurrency == 10
    assert alloc.min_request_interval_s == pytest.approx(0.5, abs=1e-3)  # 60 / 120


def test_allocation_default_concurrency_one_when_task_saturates_budget() -> None:
    """With no per-task rate, one in-flight task saturates the per-VM budget → concurrency 1."""
    alloc = allocate_rate_budget("api_football", n_vms=4)
    assert alloc.per_vm_rpm == 300  # 1200 // 4
    assert alloc.concurrency == 1  # 300 // 300
    assert alloc.min_request_interval_s == pytest.approx(0.2, abs=1e-3)  # 60 / 300


def test_allocation_floor_divides_no_oversubscription() -> None:
    """Floor division never over-allocates: 7 VMs of 1200 ⇒ 171 each, 7×171=1197 ≤ 1200."""
    alloc = allocate_rate_budget("api_football", n_vms=7)
    assert alloc.per_vm_rpm == 171
    assert alloc.per_vm_rpm * alloc.n_vms <= 1200


def test_concurrency_capped() -> None:
    """Matched concurrency never exceeds the I/O-bound cap (16 default)."""
    alloc = allocate_rate_budget("api_football", n_vms=1, max_per_query_rate_rpm=1, concurrency_cap=16)
    assert alloc.concurrency == 16  # 1200 // 1 = 1200, capped to 16


# ──────────────────────────────────────────────────────────────────────────
# Part 1 — daily-quota- AND time-aware effective ceiling (operator's live case).
# ──────────────────────────────────────────────────────────────────────────
def test_late_in_day_daily_budget_throttles_fleet_below_1200() -> None:
    """Operator's live scenario: remaining≈130,500, ~270 min to reset ⇒ ~483/min → ~5 VMs at ~90 rpm.

    130500 // 270 = 483 < 1200 ⇒ effective ceiling 483/min (the daily budget
    binds, NOT the per-minute cap). 5 VMs ⇒ 483 // 5 = 96 rpm/VM (≈ the operator's
    "~5 VMs at 90 rpm"); the fleet self-throttles below 1200 because the day is
    nearly spent. now_utc = 19:30 UTC ⇒ 270 min to the 00:00 reset.
    """
    now = datetime(2026, 6, 23, 19, 30, tzinfo=UTC)
    alloc = allocate_rate_budget(
        "api_football", n_vms=5, remaining_daily_quota=130_500, now_utc=now, max_per_query_rate_rpm=90
    )
    assert alloc.minutes_to_reset == pytest.approx(270.0, abs=0.5)
    assert alloc.effective_source_rpm == 483  # 130500 // 270, throttled below 1200
    assert alloc.per_vm_rpm == 96  # 483 // 5 ≈ 90 rpm
    # The fleet stays within BOTH ceilings.
    assert_fleet_within_budget(
        "api_football", n_vms=5, per_vm_rpm=alloc.per_vm_rpm, remaining_daily_quota=130_500, now_utc=now
    )


def test_post_reset_full_1200_supports_13_vms() -> None:
    """Post-reset, fresh 450,000/day ⇒ full 1200/min → ~13 VMs at ~90 rpm.

    With the day fresh the per-minute cap is the binding constraint (not the daily
    budget over the whole day), so the launcher allocates against 1200/min:
    1200 // 13 = 92 rpm/VM (~90); 13 × 92 = 1196 ≤ 1200. No remaining_daily_quota
    supplied ⇒ effective ceiling == the raw 1200/min.
    """
    alloc = allocate_rate_budget("api_football", n_vms=13, max_per_query_rate_rpm=90)
    assert alloc.effective_source_rpm == 1200
    assert alloc.per_vm_rpm == 92  # 1200 // 13 ≈ 90 rpm
    assert alloc.per_vm_rpm * alloc.n_vms <= 1200


def test_effective_ceiling_uses_per_minute_cap_when_daily_budget_ample() -> None:
    """A large remaining daily budget early in the day ⇒ per-minute cap binds (1200)."""
    now = datetime(2026, 6, 23, 0, 5, tzinfo=UTC)  # just after reset
    alloc = allocate_rate_budget("api_football", n_vms=10, remaining_daily_quota=450_000, now_utc=now)
    # 450000 // ~1435 min ≈ 313/min < 1200 → daily binds just after midnight.
    assert alloc.effective_source_rpm == 313
    # Confirm that with a smaller VM count + ample budget the per-minute cap binds.
    later = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)  # 720 min left
    alloc2 = allocate_rate_budget("api_football", n_vms=10, remaining_daily_quota=900_000, now_utc=later)
    assert alloc2.effective_source_rpm == 1200  # 900000 // 720 = 1250 > 1200 → cap binds


def test_minutes_to_reset_override_takes_precedence() -> None:
    """An explicit minutes_to_reset overrides now_utc (testable + launcher-injectable)."""
    alloc = allocate_rate_budget("api_football", n_vms=5, remaining_daily_quota=130_500, minutes_to_reset=270.0)
    assert alloc.effective_source_rpm == 483
    assert alloc.minutes_to_reset == pytest.approx(270.0)


def test_naive_now_utc_raises() -> None:
    """A naive (tz-unaware) now_utc is rejected — UTC discipline."""
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        allocate_rate_budget("api_football", n_vms=5, remaining_daily_quota=130_500, now_utc=datetime(2026, 6, 23))


def test_daily_quota_exhausted_per_vm_rounds_to_zero_raises() -> None:
    """When almost no daily budget remains the effective ceiling can't size a VM → refuse."""
    now = datetime(2026, 6, 23, 23, 59, tzinfo=UTC)  # 1 min to reset
    with pytest.raises(ValueError, match="per-VM budget would round to 0"):
        allocate_rate_budget("api_football", n_vms=10, remaining_daily_quota=5, now_utc=now)


def test_source_with_no_daily_quota_ignores_remaining() -> None:
    """A source with no SOURCE_DAILY_QUOTA isn't daily-throttled even if remaining is given.

    footystats has a per-minute cap (60) but no daily quota → the daily term is
    skipped; the per-minute cap stands alone regardless of remaining_daily_quota.
    """
    assert SOURCE_DAILY_QUOTA["footystats"] is None
    now = datetime(2026, 6, 23, 23, 59, tzinfo=UTC)
    alloc = allocate_rate_budget("footystats", n_vms=2, remaining_daily_quota=10, now_utc=now)
    assert alloc.effective_source_rpm == 60  # the per-minute cap, NOT daily-throttled
    assert alloc.per_vm_rpm == 30


def test_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        allocate_rate_budget("nonexistent_source", n_vms=2)


def test_uncapped_source_raises() -> None:
    """A source whose fleet ceiling is still TODO (None) AND is not per-IP cannot
    be rate-allocated. ``thegraph`` is None in SOURCE_RATE_LIMITS_RPM (it is a
    key-pool source, modelled separately) and not in SOURCE_PER_IP_LIMITS, so it
    hits the generic no-ceiling guard. (databento is now PER-IP → a different,
    more-specific raise — see test_per_ip_source_refuses_fleet_allocation.)"""
    assert SOURCE_RATE_LIMITS_RPM["thegraph"] is None
    assert "thegraph" not in SOURCE_PER_IP_LIMITS
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        allocate_rate_budget("thegraph", n_vms=2)


def test_too_many_vms_raises() -> None:
    """More VMs than the ceiling (per-VM rounds to 0) is refused."""
    with pytest.raises(ValueError, match="per-VM budget would round to 0"):
        allocate_rate_budget("api_football", n_vms=2000)


def test_n_vms_must_be_positive() -> None:
    with pytest.raises(ValueError, match="n_vms must be"):
        allocate_rate_budget("api_football", n_vms=0)


# ──────────────────────────────────────────────────────────────────────────
# Part 1 — fail-closed hard rule.
# ──────────────────────────────────────────────────────────────────────────
def test_fleet_within_budget_passes_at_ceiling() -> None:
    """Exactly at the per-minute ceiling is allowed (10 × 120 = 1200)."""
    assert_fleet_within_budget("api_football", n_vms=10, per_vm_rpm=120)  # no raise


def test_fleet_over_budget_raises() -> None:
    """Per-minute over-subscription is fail-closed (10 × 130 = 1300 > 1200)."""
    with pytest.raises(FleetBudgetExceededError, match="per-minute"):
        assert_fleet_within_budget("api_football", n_vms=10, per_vm_rpm=130)


def test_fleet_over_daily_budget_raises() -> None:
    """Projected daily consumption over the remaining quota is fail-closed.

    Within the per-minute cap (10 × 90 = 900 ≤ 1200) but the projection over the
    270 min to reset (900 × 270 = 243,000) exceeds the remaining 130,500 quota.
    """
    now = datetime(2026, 6, 23, 19, 30, tzinfo=UTC)  # 270 min to reset
    with pytest.raises(FleetBudgetExceededError, match="daily"):
        assert_fleet_within_budget("api_football", n_vms=10, per_vm_rpm=90, remaining_daily_quota=130_500, now_utc=now)


def test_fleet_within_daily_budget_passes() -> None:
    """A fleet whose projection fits the remaining daily quota passes both axes."""
    now = datetime(2026, 6, 23, 19, 30, tzinfo=UTC)  # 270 min to reset
    # 5 × 96 = 480 rpm × 270 min = 129,600 ≤ 130,500 remaining, and 480 ≤ 1200.
    assert_fleet_within_budget("api_football", n_vms=5, per_vm_rpm=96, remaining_daily_quota=130_500, now_utc=now)


def test_fleet_budget_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        assert_fleet_within_budget("nope", n_vms=1, per_vm_rpm=10)


def test_allocate_then_assert_round_trips() -> None:
    """An allocation always satisfies the hard rule (the launcher contract).

    Round-trips on BOTH axes: per-minute (no daily input) and the daily-aware
    path (the allocator's effective ceiling + the same remaining/now feed the
    assert, so the projection fits by construction).
    """
    for n in (1, 2, 5, 10, 24):
        alloc = allocate_rate_budget("api_football", n_vms=n)
        assert_fleet_within_budget("api_football", n_vms=n, per_vm_rpm=alloc.per_vm_rpm)
    now = datetime(2026, 6, 23, 19, 30, tzinfo=UTC)
    for n in (1, 2, 5):
        alloc = allocate_rate_budget("api_football", n_vms=n, remaining_daily_quota=130_500, now_utc=now)
        assert_fleet_within_budget(
            "api_football", n_vms=n, per_vm_rpm=alloc.per_vm_rpm, remaining_daily_quota=130_500, now_utc=now
        )


# ──────────────────────────────────────────────────────────────────────────
# Part 2 — machine-sizing registry.
# ──────────────────────────────────────────────────────────────────────────
def test_coinbase_cefi_starts_at_128gb() -> None:
    """Coinbase cefi must launch correctly-sized at 128 GB (operator 2026-06-23)."""
    tier = resolve_memory_tier(task="cefi-backfill", venue="COINBASE")
    assert tier.name == "highmem-128gb"
    assert tier.ram_gb == 128
    assert tier.machine_type == "n2-highmem-16"
    assert machine_type_for(task="cefi-backfill", venue="coinbase") == "n2-highmem-16"


def test_coinbase_market_suffixes_resolve_to_128gb() -> None:
    """COINBASE-SPOT / COINBASE-FUTURES (the launcher venue names) → 128 GB.

    The cefi launcher passes market-suffixed venue names; the registry keys on
    the base venue, so the suffix must be stripped.
    """
    for v in ("COINBASE-SPOT", "COINBASE-FUTURES", "coinbase-spot"):
        assert machine_type_for(task="cefi-backfill", venue=v) == "n2-highmem-16"


def test_gce_machine_ram_off_ladder() -> None:
    """gce_machine_ram_gb knows off-ladder types so a launcher avoids a false downgrade.

    The cefi heavy default e2-highmem-16 is 128 GB (off the n2 ladder) — it must
    NOT compare as 0 GB against a registry tier.
    """
    assert gce_machine_ram_gb("e2-highmem-16") == 128
    assert gce_machine_ram_gb("n2-highmem-16") == 128  # on-ladder
    assert gce_machine_ram_gb("n2-highmem-32") == 256
    assert gce_machine_ram_gb("totally-unknown") == 0


def test_256gb_tier_exists_for_heavy_coinbase_ranges() -> None:
    """The 256 GB rung exists (heavy Coinbase ranges / OOM-escalation target)."""
    top = MEMORY_TIER_LADDER[-1]
    assert top.name == "highmem-256gb"
    assert top.ram_gb == 256
    assert top.machine_type == "n2-highmem-32"


def test_canonical_ladder_is_ram_monotonic_with_operator_rungs() -> None:
    """Ladder = the exact operator ladder, strictly ascending by RAM."""
    rungs = [(t.machine_type, t.ram_gb) for t in MEMORY_TIER_LADDER]
    assert rungs == [
        ("e2-standard-4", 16),
        ("e2-standard-8", 32),
        ("n2-standard-16", 64),
        ("n2-highmem-16", 128),
        ("n2-highmem-32", 256),
    ]
    rams = [t.ram_gb for t in MEMORY_TIER_LADDER]
    assert rams == sorted(set(rams))  # strictly ascending, no dupes


def test_unknown_venue_falls_back_to_default() -> None:
    tier = resolve_memory_tier(task="some-task", venue="unknown-venue")
    assert tier.name == DEFAULT_MEMORY_TIER


def test_understat_sports_needs_at_least_32gb() -> None:
    """Understat sports ≥ standard-32gb (≥ e2-standard-4 was too small)."""
    tier = resolve_memory_tier(task="sports-backfill", venue="understat")
    assert tier.ram_gb >= 32


def test_next_memory_tier_steps_up_one_rung() -> None:
    assert next_memory_tier("standard-32gb").name == "standard-64gb"
    assert next_memory_tier("highmem-128gb").name == "highmem-256gb"


def test_next_memory_tier_stops_at_top() -> None:
    """At the 256 GB top there is no higher rung (actuator files an issue instead)."""
    assert next_memory_tier("highmem-256gb") is None


def test_next_memory_tier_unknown_raises() -> None:
    with pytest.raises(ValueError, match="unknown memory tier"):
        next_memory_tier("not-a-tier")


def test_memory_tier_reverse_lookup() -> None:
    tier = memory_tier_for_machine_type("n2-highmem-16")
    assert tier is not None
    assert tier.ram_gb == 128
    assert memory_tier_for_machine_type("e2-micro") is None


# ──────────────────────────────────────────────────────────────────────────
# Part 1 (window) — per-VM daily-quota share for the adapter's UTC-day window.
# ──────────────────────────────────────────────────────────────────────────
def test_per_vm_daily_quota_is_full_share_for_daily_source() -> None:
    """api_football has a 450k/day quota → per_vm_daily_quota = 450000 // n_vms."""
    a = allocate_rate_budget("api_football", n_vms=10)
    assert a.per_vm_daily_quota == 450_000 // 10


def test_per_vm_daily_quota_none_for_no_daily_quota_source() -> None:
    """A source without a SOURCE_DAILY_QUOTA carries no per-VM daily share."""
    a = allocate_rate_budget("footystats", n_vms=2)
    assert a.per_vm_daily_quota is None


# ──────────────────────────────────────────────────────────────────────────
# Part 3 — per-IP sources (databento / polymarket). NOT fleet-divided.
# ──────────────────────────────────────────────────────────────────────────
def test_per_ip_sources_registered() -> None:
    assert "databento" in SOURCE_PER_IP_LIMITS
    assert "polymarket_clob" in SOURCE_PER_IP_LIMITS
    assert "polymarket_gamma_api" in SOURCE_PER_IP_LIMITS


def test_databento_per_ip_is_usage_billed_no_rpm() -> None:
    lim = per_ip_rate_for_source("databento")
    assert lim is not None
    assert lim.rpm is None  # usage-billed, scale via more IPs


def test_polymarket_per_ip_has_placeholder_pending_calibration() -> None:
    lim = per_ip_rate_for_source("polymarket_clob")
    assert lim is not None
    assert lim.rpm == 600
    assert lim.calibrated is False  # ramp probe replaces this


def test_per_ip_source_refuses_fleet_allocation() -> None:
    """A per-IP source MUST NOT be fleet-divided (each VM/IP gets the full rate)."""
    with pytest.raises(ValueError, match="PER-IP"):
        allocate_rate_budget("polymarket_clob", n_vms=4)


def test_non_per_ip_source_has_no_per_ip_limit() -> None:
    assert per_ip_rate_for_source("api_football") is None


# ──────────────────────────────────────────────────────────────────────────
# Part 4 — TheGraph key-pool capacity (per-key × pool size).
# ──────────────────────────────────────────────────────────────────────────
def test_thegraph_key_pool_registered() -> None:
    assert "thegraph" in SOURCE_KEY_POOL_LIMITS


def test_thegraph_effective_is_per_key_times_pool() -> None:
    k = key_pool_capacity_for_source("thegraph")
    assert k is not None
    assert k.pool_size == 9  # mirrors thegraph_base_client._THEGRAPH_NUM_API_KEYS
    assert k.effective_rpm == k.per_key_rpm * k.pool_size
    assert k.calibrated is False  # per-key placeholder pending ramp probe


def test_key_pool_unknown_source_returns_none() -> None:
    assert key_pool_capacity_for_source("api_football") is None
