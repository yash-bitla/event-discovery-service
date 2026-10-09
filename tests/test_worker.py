from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from conftest import BASE, make_sim
from event_discovery.clock import ManualClock
from event_discovery.geo import haversine_km
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import Region, RegionState, plan_all
from event_discovery.ingest.resilience import CircuitBreaker, Retrier
from event_discovery.ingest.worker import IngestWorker, InMemorySink, seconds_to_next_cycle
from event_discovery.upstream_sim.dataset import METROS, SimEvent, generate_events

REGIONS = [Region(m.name, m.lat, m.lon, 50, m.weight) for m in METROS]


def in_region(events: list[SimEvent], region: Region) -> set[str]:
    return {e.id for e in events if haversine_km(region.lat, region.lon, e.lat, e.lon) <= 50}


def test_one_cycle_stores_each_event_of_each_region(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    sink = InMemorySink()
    client = DiscoveryClient(sim, "demo", RatePacer(clock, 4))
    worker = IngestWorker(client, sink, REGIONS, clock, planner=plan_all)

    report = worker.run_cycle(3600)

    expected = set().union(*(in_region(events, region) for region in REGIONS))
    assert set(sink.events) == expected
    assert report.refreshed == len(REGIONS)
    assert report.events_new == len(expected)
    assert report.stopped is None
    assert sim.get("/_sim/stats").json()["rejected_spike"] == 0


def test_a_region_with_more_events_than_the_paging_limit_is_divided(
    clock: ManualClock,
) -> None:
    dense = generate_events(seed=11, base=BASE, count=30_000)
    new_york = REGIONS[0]
    expected = in_region(dense, new_york)
    assert len(expected) > 1000

    sink = InMemorySink()
    client = DiscoveryClient(make_sim(dense, clock), "demo", RatePacer(clock, 4))
    worker = IngestWorker(client, sink, [new_york], clock)

    report = worker.run_cycle(3600)

    assert set(sink.events) == expected
    assert worker.states["New York"].expected_cost == report.calls


def test_the_worker_stops_at_the_reserve_and_the_upstream_rejects_no_call(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock, daily_quota=40)
    budget = QuotaBudget(clock, daily_limit=40, reserve=10)
    client = DiscoveryClient(sim, "demo", RatePacer(clock, 4), budget)
    worker = IngestWorker(client, InMemorySink(), REGIONS, clock, budget=budget)

    reports = []
    for hour in range(24):
        clock.advance_to(BASE.timestamp() + hour * 3600)
        reports.append(worker.run_cycle(3600))

    stats = sim.get("/_sim/stats").json()
    assert (stats["accepted"], stats["rejected_quota"], stats["rejected_spike"]) == (30, 0, 0)
    assert sum(report.calls for report in reports) == 30
    assert budget.spendable == 0


def test_without_a_budget_the_worker_stops_when_the_upstream_rejects_a_call(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock, daily_quota=20)
    client = DiscoveryClient(sim, "demo", RatePacer(clock, 4))
    worker = IngestWorker(client, InMemorySink(), REGIONS, clock, planner=plan_all)

    report = worker.run_cycle(3600)

    assert report.stopped == "quota exceeded"
    assert report.refreshed < len(REGIONS)
    assert sim.get("/_sim/stats").json()["rejected_quota"] == 1


def test_a_failed_region_does_not_stop_the_cycle(
    events: list[SimEvent], clock: ManualClock
) -> None:
    bad = Region("Bad", 40.0, -100.0, -1, 1.0)  # the upstream rejects a negative radius
    sim = make_sim(events, clock)
    client = DiscoveryClient(sim, "demo", RatePacer(clock, 4))
    worker = IngestWorker(client, InMemorySink(), [bad, REGIONS[-1]], clock, planner=plan_all)

    report = worker.run_cycle(3600)

    assert (report.failed, report.refreshed) == (1, 1)
    assert worker.states["Bad"].last_refreshed_at is None


def resilient_worker(
    sim: TestClient, clock: ManualClock, regions: list[Region], **kwargs: Any
) -> tuple[IngestWorker, CircuitBreaker]:
    breaker = CircuitBreaker(clock)
    client = DiscoveryClient(
        sim, "demo", RatePacer(clock, 4), retrier=Retrier(clock, seed=1), breaker=breaker
    )
    worker = IngestWorker(client, InMemorySink(), regions, clock, planner=plan_all, **kwargs)
    return worker, breaker


def test_retries_complete_a_cycle_when_one_call_in_five_fails(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    sim.put("/_sim/faults", json={"error_rate": 0.2})
    worker, _ = resilient_worker(sim, clock, REGIONS)

    report = worker.run_cycle(3600)

    assert report.refreshed == len(REGIONS)
    assert sim.get("/_sim/stats").json()["faults_injected"] > 0


def test_an_outage_opens_the_breaker_and_stops_the_cycle_after_five_calls(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    sim.put("/_sim/faults", json={"outages": [[clock.now(), clock.now() + 600]]})
    worker, breaker = resilient_worker(sim, clock, REGIONS)

    report = worker.run_cycle(3600)

    assert (report.refreshed, report.stopped, report.calls) == (0, "circuit open", 5)
    assert breaker.state == "open"
    # The next cycle starts at the time of the probe, not after one hour.
    wait = seconds_to_next_cycle(report, 3600, breaker, clock.now())
    assert 0 < wait <= 30

    assert worker.run_cycle(3600).calls == 0  # the breaker is open: no call goes out


def test_ingestion_starts_again_after_the_outage(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    outage_end = clock.now() + 600
    sim.put("/_sim/faults", json={"outages": [[clock.now(), outage_end]]})
    worker, breaker = resilient_worker(sim, clock, REGIONS)

    report = worker.run_cycle(3600)
    while report.refreshed == 0:
        clock.sleep(seconds_to_next_cycle(report, 3600, breaker, clock.now()))
        report = worker.run_cycle(3600)

    assert report.refreshed == len(REGIONS)
    assert breaker.state == "closed"
    # Open for 30, 60, 120, 240 and 300 s: the probe after 300 s is the first after the end.
    assert breaker.times_opened == 5
    assert clock.now() - outage_end < 300


class MemoryStateStore:
    def __init__(self) -> None:
        self.states: dict[str, RegionState] = {}

    def load(self) -> dict[str, RegionState]:
        return dict(self.states)

    def save(self, region: Region, state: RegionState) -> None:
        self.states[region.name] = RegionState(state.last_refreshed_at, state.expected_cost)


def test_a_new_worker_continues_from_the_saved_state(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    store = MemoryStateStore()
    first, _ = resilient_worker(sim, clock, REGIONS[:3], state_store=store)
    first.run_cycle(3600)

    second, _ = resilient_worker(sim, clock, REGIONS[:3], state_store=store)

    assert second.states == first.states
    assert all(state.last_refreshed_at is not None for state in second.states.values())
