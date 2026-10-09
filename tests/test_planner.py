from __future__ import annotations

from pathlib import Path

from event_discovery.ingest.planner import Region, RegionState, load_regions, plan_refresh


def region(name: str, weight: float) -> Region:
    return Region(name=name, lat=0.0, lon=0.0, radius_km=50, weight=weight)


def names(plan: list[Region]) -> list[str]:
    return [r.name for r in plan]


def test_a_region_with_no_refresh_goes_first() -> None:
    regions = [region("old", 10), region("new", 1)]
    states = {"old": RegionState(last_refreshed_at=0.0), "new": RegionState()}
    assert names(plan_refresh(regions, states, now=9999.0, allowance=100)) == ["new", "old"]


def test_order_is_weight_times_staleness_for_each_call() -> None:
    regions = [region("a", 4), region("b", 1), region("c", 4)]
    states = {
        "a": RegionState(last_refreshed_at=900.0, expected_cost=1),  # 4 * 100 / 1 = 400
        "b": RegionState(last_refreshed_at=0.0, expected_cost=1),  # 1 * 1000 / 1 = 1000
        "c": RegionState(last_refreshed_at=0.0, expected_cost=8),  # 4 * 1000 / 8 = 500
    }
    assert names(plan_refresh(regions, states, now=1000.0, allowance=100)) == ["b", "c", "a"]


def test_the_plan_stays_in_the_allowance() -> None:
    regions = [region("a", 3), region("b", 2), region("c", 1)]
    states = {
        "a": RegionState(last_refreshed_at=0.0, expected_cost=6),
        "b": RegionState(last_refreshed_at=0.0, expected_cost=6),
        "c": RegionState(last_refreshed_at=0.0, expected_cost=3),
    }
    # "a" uses 6 of 10 calls. "b" does not fit in the 4 that remain. "c" does.
    assert names(plan_refresh(regions, states, now=1000.0, allowance=10)) == ["a", "c"]


def test_the_first_region_is_planned_also_when_it_costs_more_than_the_allowance() -> None:
    regions = [region("large", 5), region("small", 1)]
    states = {
        "large": RegionState(last_refreshed_at=0.0, expected_cost=50),  # 5 * 1000 / 50 = 100
        "small": RegionState(last_refreshed_at=900.0, expected_cost=2),  # 1 * 100 / 2 = 50
    }
    assert names(plan_refresh(regions, states, now=1000.0, allowance=10)) == ["large"]
    assert plan_refresh(regions, states, now=1000.0, allowance=0) == []


def test_the_regions_file_loads() -> None:
    regions = load_regions(Path(__file__).parent.parent / "config" / "regions.toml")
    assert len(regions) == 30
    assert regions[0] == Region("New York", 40.7128, -74.006, 50, 19.5)
