"""Guard + behaviour tests for the registry-driven launch-parameter module.

Covers the two registries the operator designed 2026-06-23 to REPLACE reactive
backoff / OOM-escalation as the PRIMARY mechanism:

Part 1 — API rate-budget:
    - ``allocate_rate_budget`` splits a source ceiling deterministically
      (per_vm_rpm = source_rpm // n_vms), with matched concurrency;
    - the worked API-Football example (10 VMs → 90 req/min each, 0 waste);
    - the fail-closed hard rule (``assert_fleet_within_budget`` raises on
      over-subscription);
    - API-Football's ONE-quota-across-all-endpoints fact (the source, not the
      endpoint, is the budgeted unit).

Part 2 — machine-sizing:
    - Coinbase cefi resolves to the 128 GB tier (NOT a small default);
    - the canonical ladder is RAM-monotonic with the operator's exact rungs;
    - ``next_memory_tier`` steps up one rung and stops at the 256 GB top.
"""

from __future__ import annotations

import pytest

from deployment_service.data_pipeline_monitors.launch_budget_registry import (
    DEFAULT_MEMORY_TIER,
    MEMORY_TIER_LADDER,
    SOURCE_RATE_LIMITS_RPM,
    FleetBudgetExceededError,
    allocate_rate_budget,
    assert_fleet_within_budget,
    gce_machine_ram_gb,
    machine_type_for,
    memory_tier_for_machine_type,
    next_memory_tier,
    resolve_memory_tier,
)


# ──────────────────────────────────────────────────────────────────────────
# Part 1 — rate-budget allocation.
# ──────────────────────────────────────────────────────────────────────────
def test_api_football_ceiling_is_900() -> None:
    """API-Football's fleet ceiling is the documented Mega-plan 900/min (api_football.py:154).

    Operator is still confirming a possible higher tier; until confirmed 900 is
    authoritative (fail-closed: under-allocate, never over-subscribe).
    """
    assert SOURCE_RATE_LIMITS_RPM["api_football"] == 900


def test_api_football_worked_example_10_vms() -> None:
    """10 API-Football VMs → 90 req/min each, exactly the ceiling, 0 waste.

    The operator's worked example. With max_per_query_rate_rpm=12 ("each slot
    ~12 req/min"), concurrency = 90 // 12 = 7 → 10 VMs × concurrency 7 = 70
    in-flight against the 900/min ceiling (the stated steady state).
    """
    alloc = allocate_rate_budget("api_football", n_vms=10, max_per_query_rate_rpm=12)
    assert alloc.per_vm_rpm == 90
    assert alloc.per_vm_rpm * alloc.n_vms == 900  # exactly the ceiling — 0 waste
    assert alloc.concurrency == 7
    assert alloc.min_request_interval_s == pytest.approx(0.6667, abs=1e-3)  # 60 / 90


def test_allocation_default_concurrency_one_when_task_saturates_budget() -> None:
    """With no per-task rate, one in-flight task saturates the per-VM budget → concurrency 1."""
    alloc = allocate_rate_budget("api_football", n_vms=4)
    assert alloc.per_vm_rpm == 225  # 900 // 4
    assert alloc.concurrency == 1  # 225 // 225
    assert alloc.min_request_interval_s == pytest.approx(0.2667, abs=1e-3)  # 60 / 225


def test_allocation_floor_divides_no_oversubscription() -> None:
    """Floor division never over-allocates: 7 VMs of 900 ⇒ 128 each, 7×128=896 ≤ 900."""
    alloc = allocate_rate_budget("api_football", n_vms=7)
    assert alloc.per_vm_rpm == 128
    assert alloc.per_vm_rpm * alloc.n_vms <= 900


def test_concurrency_capped() -> None:
    """Matched concurrency never exceeds the I/O-bound cap (16 default)."""
    alloc = allocate_rate_budget("api_football", n_vms=1, max_per_query_rate_rpm=1, concurrency_cap=16)
    assert alloc.concurrency == 16  # 900 // 1 = 900, capped to 16


def test_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        allocate_rate_budget("nonexistent_source", n_vms=2)


def test_uncapped_source_raises() -> None:
    """A source whose ceiling is still TODO (None) cannot be rate-allocated."""
    assert SOURCE_RATE_LIMITS_RPM["databento"] is None
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        allocate_rate_budget("databento", n_vms=2)


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
    """Exactly at the ceiling is allowed (10 × 90 = 900)."""
    assert_fleet_within_budget("api_football", n_vms=10, per_vm_rpm=90)  # no raise


def test_fleet_over_budget_raises() -> None:
    """Over-subscription is fail-closed (10 × 100 = 1000 > 900)."""
    with pytest.raises(FleetBudgetExceededError, match="over-subscribed"):
        assert_fleet_within_budget("api_football", n_vms=10, per_vm_rpm=100)


def test_fleet_budget_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="no fleet rate ceiling"):
        assert_fleet_within_budget("nope", n_vms=1, per_vm_rpm=10)


def test_allocate_then_assert_round_trips() -> None:
    """An allocation always satisfies the hard rule (the launcher contract)."""
    for n in (1, 2, 5, 10, 24):
        alloc = allocate_rate_budget("api_football", n_vms=n)
        assert_fleet_within_budget("api_football", n_vms=n, per_vm_rpm=alloc.per_vm_rpm)


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
