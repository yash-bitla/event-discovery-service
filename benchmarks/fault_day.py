"""Simulate one day of ingestion while the simulated upstream fails.

Two fault scenarios:

- outage: the upstream returns HTTP 503 for each call. The outage starts at 06:20.
          It runs four times, with an end at 08:05, 08:20, 08:35 and 08:50, so
          that the result does not depend on where the end is in the hourly cycle.
- errors: one call in five returns HTTP 503 for the full day.

Three clients run each scenario with the same quota budget and planner:

- no retry:        a failed call fails the refresh of its region.
- retry:           a maximum of 4 attempts for each call, with backoff.
- retry + breaker: the same, with the circuit breaker. After the breaker stops a
                   cycle, the next cycle starts at the time of the next probe.

Time comes from a manual clock, so each run gives the same numbers.

    python benchmarks/fault_day.py
"""

from __future__ import annotations

import argparse
import logging
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi.testclient import TestClient

from event_discovery.clock import ManualClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import Region
from event_discovery.ingest.resilience import CircuitBreaker, Retrier
from event_discovery.ingest.worker import IngestWorker, InMemorySink, seconds_to_next_cycle
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import METROS, SimEvent, generate_events

BASE = datetime(2026, 1, 1, tzinfo=UTC)
T0 = BASE.timestamp()
DAY_S = 86_400
CYCLE_S = 3600
SAMPLE_S = 300
OUTAGE_START = T0 + 6 * 3600 + 20 * 60
OUTAGE_ENDS = [T0 + 8 * 3600 + minutes * 60 for minutes in (5, 20, 35, 50)]
CLIENTS = ("no retry", "retry", "retry + breaker")


@dataclass(frozen=True)
class DayResult:
    client: str
    calls: int  # calls that used the quota
    failed_calls: int  # calls that got HTTP 503
    refreshes: int
    failed_refreshes: int
    mean_staleness_h: float
    max_staleness_h: float
    recovery_min: float | None  # from the end of the outage to the first refresh
    breaker_opened: int


def run_day(
    client_name: str,
    faults: dict[str, Any],
    events: list[SimEvent],
    outage_end: float | None = None,
) -> DayResult:
    clock = ManualClock(T0)
    sim = TestClient(create_app(events, clock, fault_seed=7))
    sim.put("/_sim/faults", json=faults)
    regions = [Region(m.name, m.lat, m.lon, 50, m.weight) for m in METROS]
    budget = QuotaBudget(clock)
    retrier = Retrier(clock, seed=7) if client_name != "no retry" else None
    breaker = CircuitBreaker(clock) if client_name == "retry + breaker" else None
    client = DiscoveryClient(
        sim, "demo", RatePacer(clock, per_second=4), budget, retrier=retrier, breaker=breaker
    )
    worker = IngestWorker(client, InMemorySink(), regions, clock, budget=budget)

    total_weight = sum(region.weight for region in regions)
    weighted_sum = worst = 0.0
    samples = refreshes = failed_refreshes = 0
    recovery: float | None = None
    next_regular = T0  # the hourly schedule
    next_cycle = T0
    next_sample = T0 + SAMPLE_S

    while next_cycle < T0 + DAY_S or next_sample <= T0 + DAY_S:
        if next_sample <= next_cycle or next_cycle >= T0 + DAY_S:
            clock.advance_to(next_sample)
            for region in regions:
                last = worker.states[region.name].last_refreshed_at
                staleness = next_sample - (T0 if last is None else last)
                weighted_sum += region.weight * staleness
                worst = max(worst, staleness)
            samples += 1
            next_sample += SAMPLE_S
            continue

        clock.advance_to(next_cycle)
        if next_cycle >= next_regular:
            next_regular += CYCLE_S
        report = worker.run_cycle(CYCLE_S)
        refreshes += report.refreshed
        failed_refreshes += report.failed
        if (
            outage_end is not None
            and recovery is None
            and report.refreshed
            and next_cycle >= outage_end
        ):
            recovery = clock.now() - outage_end
        wait = seconds_to_next_cycle(report, CYCLE_S, breaker, clock.now())
        next_cycle = next_regular if wait == CYCLE_S else min(next_regular, clock.now() + wait)
        # A cycle can end after some sample times. Those samples are not taken.
        while next_sample < clock.now():
            next_sample += SAMPLE_S

    stats = sim.get("/_sim/stats").json()
    return DayResult(
        client=client_name,
        calls=stats["accepted"],
        failed_calls=stats["faults_injected"],
        refreshes=refreshes,
        failed_refreshes=failed_refreshes,
        mean_staleness_h=weighted_sum / (samples * total_weight) / 3600,
        max_staleness_h=worst / 3600,
        recovery_min=recovery / 60 if recovery is not None else None,
        breaker_opened=breaker.times_opened if breaker is not None else 0,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=80_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    events = generate_events(args.seed, BASE, args.events)
    logging.disable(logging.WARNING)  # the worker logs each failed refresh

    print("scenario: outage from 06:20, mean of four runs (end at 08:05, 08:20, 08:35, 08:50)")
    print(
        "| Client | Failed calls | Recovery, mean (min) | Recovery, max (min) "
        "| Mean staleness (h) | Max staleness (h) |"
    )
    print("|---|---|---|---|---|---|")
    for name in CLIENTS:
        runs = [
            run_day(name, {"outages": [[OUTAGE_START, end]]}, events, outage_end=end)
            for end in OUTAGE_ENDS
        ]
        recoveries = [r.recovery_min for r in runs if r.recovery_min is not None]
        assert len(recoveries) == len(runs)
        print(
            f"| {name} | {statistics.mean(r.failed_calls for r in runs):.0f} "
            f"| {statistics.mean(recoveries):.1f} | {max(recoveries):.1f} "
            f"| {statistics.mean(r.mean_staleness_h for r in runs):.2f} "
            f"| {max(r.max_staleness_h for r in runs):.2f} |"
        )

    print("\nscenario: one call in five fails for the full day")
    print(
        "| Client | Calls | Failed calls | Refreshes | Failed refreshes "
        "| Mean staleness (h) | Max staleness (h) | Breaker opened |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for name in CLIENTS:
        r = run_day(name, {"error_rate": 0.2}, events)
        print(
            f"| {r.client} | {r.calls} | {r.failed_calls} | {r.refreshes} | {r.failed_refreshes} "
            f"| {r.mean_staleness_h:.2f} | {r.max_staleness_h:.2f} | {r.breaker_opened} |"
        )


if __name__ == "__main__":
    main()
