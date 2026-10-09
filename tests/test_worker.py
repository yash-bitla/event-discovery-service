from __future__ import annotations

from conftest import BASE, make_sim
from event_discovery.clock import ManualClock
from event_discovery.geo import haversine_km
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import Region, plan_all
from event_discovery.ingest.worker import IngestWorker, InMemorySink
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
    assert stats == {"accepted": 30, "rejected_quota": 0, "rejected_spike": 0}
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
