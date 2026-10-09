"""Simulate one day of hourly ingestion cycles against the simulated upstream.

Two policies run on the same events and the same quota:

- all, each hour:    each cycle refreshes each region, and no budget is checked.
- all, each 3 hours: the same, but only each third cycle runs. Three hours is the
                     shortest fixed interval that fits the quota for the default events.
- budget:            the planner selects regions for each cycle from the quota allowance.

Time comes from a manual clock, so the day runs in seconds and the result is the
same on each run.

    python benchmarks/quota_day.py
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from event_discovery.clock import ManualClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import Region, plan_all, plan_refresh
from event_discovery.ingest.worker import IngestWorker, InMemorySink
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import METROS, SimEvent, generate_events

BASE = datetime(2026, 1, 1, tzinfo=UTC)
CYCLE_S = 3600
CYCLES = 24
SAMPLE_S = 300


@dataclass(frozen=True)
class DayResult:
    policy: str
    calls_accepted: int
    calls_rejected: int
    refreshes: int
    mean_staleness_h: float  # mean across the day, with the region weight as the weight
    max_staleness_h: float


def run_day(
    policy: str, events: list[SimEvent], quota: int, reserve: int, every: int = 1
) -> DayResult:
    """Run one day. Only each `every`-th cycle runs."""
    clock = ManualClock(BASE.timestamp())
    sim = TestClient(create_app(events, clock, daily_quota=quota))
    regions = [Region(m.name, m.lat, m.lon, 50, m.weight) for m in METROS]
    budget = QuotaBudget(clock, daily_limit=quota, reserve=reserve) if policy == "budget" else None
    client = DiscoveryClient(sim, "demo", RatePacer(clock, per_second=4), budget)
    sink = InMemorySink()
    worker = IngestWorker(
        client,
        sink,
        regions,
        clock,
        budget=budget,
        planner=plan_refresh if policy == "budget" else plan_all,
    )

    total_weight = sum(region.weight for region in regions)
    weighted_sum = 0.0
    samples = 0
    worst = 0.0
    refreshes = 0

    def sample(at: float) -> None:
        """Record the staleness of each region. Time before the first refresh counts."""
        nonlocal weighted_sum, samples, worst
        for region in regions:
            last = worker.states[region.name].last_refreshed_at
            staleness = at - (BASE.timestamp() if last is None else last)
            weighted_sum += region.weight * staleness
            worst = max(worst, staleness)
        samples += 1

    for cycle in range(CYCLES):
        cycle_start = BASE.timestamp() + cycle * CYCLE_S
        clock.advance_to(cycle_start)
        if cycle % every == 0:
            refreshes += worker.run_cycle(CYCLE_S * every).refreshed
        # The states now hold the refresh times of this cycle. A sample at a time
        # before a refresh of this cycle would be negative, so start after the cycle.
        cycle_end = clock.now()
        at = cycle_start + SAMPLE_S
        while at <= cycle_start + CYCLE_S:
            if at >= cycle_end:
                sample(at)
            at += SAMPLE_S

    stats = sim.get("/_sim/stats").json()
    return DayResult(
        policy=policy if every == 1 and policy == "budget" else f"{policy}, each {every} h",
        calls_accepted=stats["accepted"],
        calls_rejected=stats["rejected_quota"] + stats["rejected_spike"],
        refreshes=refreshes,
        mean_staleness_h=weighted_sum / (samples * total_weight) / 3600,
        max_staleness_h=worst / 3600,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0] if __doc__ else None)
    parser.add_argument("--events", type=int, default=80_000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--quota", type=int, default=5000)
    parser.add_argument("--reserve", type=int, default=500)
    args = parser.parse_args()

    events = generate_events(args.seed, BASE, args.events)
    print(f"events: {args.events}, regions: {len(METROS)}, quota: {args.quota} calls a day")
    print(
        "| Policy | Calls accepted | Calls rejected | Region refreshes "
        "| Mean staleness (h) | Max staleness (h) |"
    )
    print("|---|---|---|---|---|---|")
    for policy, every in (("all", 1), ("all", 3), ("budget", 1)):
        r = run_day(policy, events, args.quota, args.reserve, every)
        print(
            f"| {r.policy} | {r.calls_accepted} | {r.calls_rejected} | {r.refreshes} "
            f"| {r.mean_staleness_h:.2f} | {r.max_staleness_h:.2f} |"
        )


if __name__ == "__main__":
    main()
