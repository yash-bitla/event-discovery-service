"""Select the regions to refresh in one cycle."""

from __future__ import annotations

import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_EXPECTED_COST = 5


@dataclass(frozen=True)
class Region:
    name: str
    lat: float
    lon: float
    radius_km: int
    weight: float  # relative demand: a region with more weight gets fresher data


@dataclass
class RegionState:
    last_refreshed_at: float | None = None
    expected_cost: int = DEFAULT_EXPECTED_COST  # calls that the last refresh used


Planner = Callable[[Sequence[Region], Mapping[str, RegionState], float, int], list[Region]]


def plan_refresh(
    regions: Sequence[Region],
    states: Mapping[str, RegionState],
    now: float,
    allowance: int,
) -> list[Region]:
    """Select regions in order of value for each call, until the allowance is used.

    The value of a refresh is `weight * staleness`. A region that has no refresh yet
    goes before all others. The first region is always in the plan, also when it
    costs more than the allowance. Without this rule, a region that costs more than
    one allowance gets no refresh while smaller regions fit. The next allowance is
    smaller as a result, because it comes from the calls that remain.
    """

    def priority(region: Region) -> tuple[int, float]:
        state = states[region.name]
        if state.last_refreshed_at is None:
            return (0, -region.weight / state.expected_cost)
        staleness = now - state.last_refreshed_at
        return (1, -region.weight * staleness / state.expected_cost)

    ordered = sorted(regions, key=priority)
    plan: list[Region] = []
    remaining = allowance
    if allowance <= 0:
        return plan
    for region in ordered:
        cost = states[region.name].expected_cost
        if cost <= remaining or not plan:
            plan.append(region)
            remaining -= cost
    return plan


def plan_all(
    regions: Sequence[Region],
    states: Mapping[str, RegionState],
    now: float,
    allowance: int,
) -> list[Region]:
    """Baseline: refresh each region in each cycle."""
    return list(regions)


def load_regions(path: Path) -> list[Region]:
    with path.open("rb") as file:
        data = tomllib.load(file)
    return [Region(**entry) for entry in data["region"]]
