from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from conftest import BASE, make_sim
from event_discovery.api.app import create_api
from event_discovery.clock import ManualClock
from event_discovery.ingest.budget import RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import Region, RegionState
from event_discovery.ingest.resilience import CircuitBreaker, Retrier
from event_discovery.ingest.worker import IngestWorker
from event_discovery.storage.events import PostgresSink
from event_discovery.storage.regions import PostgresStateStore
from event_discovery.upstream_sim.dataset import SimEvent
from test_storage import event

NEW_YORK = {"lat": 40.7128, "lon": -74.0060}


@pytest.fixture
def api(pool: ConnectionPool) -> TestClient:
    return TestClient(create_api(pool, now=lambda: BASE))


def test_events_returns_the_events_near_the_point(api: TestClient, pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert([event("near", lat=40.75), event("far", lat=42.36, lon=-71.06)])

    body = api.get("/events", params=NEW_YORK).json()

    assert [e["id"] for e in body["events"]] == ["near"]
    assert body["next_cursor"] is None
    assert body["refreshed_at"] is None  # no region has a refresh yet
    assert body["events"][0] == {
        "id": "near",
        "name": "Event near",
        "url": "https://events.example/near",
        "starts_at": "2026-01-02T00:00:00Z",
        "segment": "Music",
        "venue": "Hall",
        "city": "New York",
        "lat": 40.75,
        "lon": -74.006,
        "distance_km": pytest.approx(4.13, abs=0.02),
    }


def test_the_default_range_is_30_days_from_now(api: TestClient, pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert(
        [event("past", days=-1), event("soon", days=29), event("later", days=31)]
    )
    body = api.get("/events", params=NEW_YORK).json()
    assert [e["id"] for e in body["events"]] == ["soon"]


def test_start_and_end_set_the_range(api: TestClient, pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert([event("a", days=40), event("b", days=50)])
    params = {**NEW_YORK, "start": "2026-02-15T00:00:00Z", "end": "2026-02-25T00:00:00Z"}
    body = api.get("/events", params=params).json()
    assert [e["id"] for e in body["events"]] == ["b"]


def test_the_cursor_gets_the_next_page(api: TestClient, pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert([event(f"e{n}", days=1 + n) for n in range(5)])

    seen: list[str] = []
    params: dict[str, object] = {**NEW_YORK, "limit": 2}
    for _ in range(3):
        body = api.get("/events", params=params).json()
        seen += [e["id"] for e in body["events"]]
        params["cursor"] = body["next_cursor"]

    assert seen == ["e0", "e1", "e2", "e3", "e4"]
    assert body["next_cursor"] is None


@pytest.mark.parametrize(
    "params",
    [
        {"lon": -74.0},  # no lat
        {"lat": 91, "lon": -74.0},
        {"lat": 40.7, "lon": -74.0, "radius_km": 0},
        {"lat": 40.7, "lon": -74.0, "radius_km": 201},
        {"lat": 40.7, "lon": -74.0, "limit": 201},
        {"lat": 40.7, "lon": -74.0, "start": "not a time"},
        {"lat": 40.7, "lon": -74.0, "start": "2026-02-02T00:00:00Z", "end": "2026-02-01T00:00:00Z"},
    ],
)
def test_invalid_parameters_get_422(api: TestClient, params: dict[str, object]) -> None:
    assert api.get("/events", params=params).status_code == 422


def test_an_invalid_cursor_gets_400(api: TestClient) -> None:
    assert api.get("/events", params={**NEW_YORK, "cursor": "abc"}).status_code == 400


def test_healthz(api: TestClient) -> None:
    assert api.get("/healthz").json() == {"status": "ok"}


def test_ingested_events_are_in_the_api(
    api: TestClient, pool: ConnectionPool, events: list[SimEvent], clock: ManualClock
) -> None:
    boston = Region("Boston", 42.3601, -71.0589, 50, 1.0)
    client = DiscoveryClient(make_sim(events, clock), "demo", RatePacer(clock, 4))
    report = IngestWorker(client, PostgresSink(pool), [boston], clock).run_cycle(3600)

    params = {
        "lat": 42.3601,
        "lon": -71.0589,
        "radius_km": 60,
        "limit": 200,
        "end": "2026-04-02T00:00:00Z",
    }
    body = api.get("/events", params=params).json()

    assert report.events_new > 0
    assert len(body["events"]) == min(report.events_new, 200)
    assert {e["city"] for e in body["events"]} == {"Boston"}


def test_the_api_answers_from_stored_data_while_the_upstream_is_not_available(
    api: TestClient, pool: ConnectionPool, events: list[SimEvent], clock: ManualClock
) -> None:
    boston = Region("Boston", 42.3601, -71.0589, 50, 1.0)
    sim = make_sim(events, clock)
    breaker = CircuitBreaker(clock)
    client = DiscoveryClient(
        sim, "demo", RatePacer(clock, 4), retrier=Retrier(clock, seed=1), breaker=breaker
    )
    worker = IngestWorker(
        client, PostgresSink(pool), [boston], clock, state_store=PostgresStateStore(pool)
    )
    params = {"lat": 42.3601, "lon": -71.0589, "radius_km": 30, "end": "2026-04-02T00:00:00Z"}

    assert worker.run_cycle(3600).refreshed == 1
    before = api.get("/events", params=params).json()
    assert before["refreshed_at"] == "2026-01-01T00:00:00Z"
    assert len(before["events"]) == 50

    clock.advance_to(BASE.timestamp() + 3600)
    sim.put("/_sim/faults", json={"outages": [[clock.now(), clock.now() + 7200]]})
    report = worker.run_cycle(3600)
    assert (report.refreshed, report.failed) == (0, 1)

    during = api.get("/events", params=params)
    assert during.status_code == 200
    assert during.json() == before  # the same events, and the same refresh time


def test_refreshed_at_is_null_for_a_point_outside_all_regions(
    api: TestClient, pool: ConnectionPool
) -> None:
    boston = Region("Boston", 42.3601, -71.0589, 50, 1.0)
    PostgresStateStore(pool).save(boston, RegionState(last_refreshed_at=BASE.timestamp()))
    inside = api.get("/events", params={"lat": 42.4, "lon": -71.1}).json()
    outside = api.get("/events", params=NEW_YORK).json()
    assert inside["refreshed_at"] == "2026-01-01T00:00:00Z"
    assert outside["refreshed_at"] is None
