from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest
from psycopg_pool import ConnectionPool

from conftest import BASE
from event_discovery.geo import haversine_km
from event_discovery.ingest.planner import Region, RegionState
from event_discovery.models import Event
from event_discovery.storage.db import migrate
from event_discovery.storage.events import PostgresSink, SearchQuery, search_events
from event_discovery.storage.regions import PostgresStateStore, last_refresh
from event_discovery.upstream_sim.dataset import SimEvent

NEW_YORK = (40.7128, -74.0060)
END = BASE + timedelta(days=90)


def to_event(sim: SimEvent) -> Event:
    return Event(
        id=sim.id,
        name=sim.name,
        url=f"https://events.example/{sim.id}",
        starts_at=sim.starts_at,
        segment=sim.segment,
        venue=sim.venue,
        city=sim.city,
        lat=sim.lat,
        lon=sim.lon,
    )


def event(event_id: str, *, days: float = 1, lat: float = 40.7128, lon: float = -74.0060) -> Event:
    return Event(
        id=event_id,
        name=f"Event {event_id}",
        url=f"https://events.example/{event_id}",
        starts_at=BASE + timedelta(days=days),
        segment="Music",
        venue="Hall",
        city="New York",
        lat=lat,
        lon=lon,
    )


def ny_query(**changes: object) -> SearchQuery:
    query = SearchQuery(lat=NEW_YORK[0], lon=NEW_YORK[1], radius_km=10, start=BASE, end=END)
    return replace(query, **changes)  # type: ignore[arg-type]


def updated_at(pool: ConnectionPool, event_id: str) -> object:
    with pool.connection() as conn:
        row = conn.execute("SELECT updated_at FROM events WHERE id = %s", (event_id,)).fetchone()
    assert row is not None
    return row[0]


def test_migrate_can_run_again(pool: ConnectionPool) -> None:
    migrate(pool, schema="eds_test")


def test_upsert_returns_the_number_of_new_events(pool: ConnectionPool) -> None:
    sink = PostgresSink(pool)
    assert sink.upsert([event("a"), event("b")]) == 2
    assert sink.upsert([event("b"), event("c")]) == 1
    assert sink.upsert([]) == 0


def test_upsert_accepts_one_event_two_times_in_one_batch(pool: ConnectionPool) -> None:
    assert PostgresSink(pool).upsert([event("a"), event("a")]) == 1


def test_upsert_does_not_write_an_unchanged_event(pool: ConnectionPool) -> None:
    sink = PostgresSink(pool)
    sink.upsert([event("a")])
    before = updated_at(pool, "a")
    sink.upsert([event("a")])
    assert updated_at(pool, "a") == before


def test_upsert_updates_a_changed_event(pool: ConnectionPool) -> None:
    sink = PostgresSink(pool)
    sink.upsert([event("a")])
    before = updated_at(pool, "a")

    moved = replace(event("a"), name="New name", lat=40.75)
    assert sink.upsert([moved]) == 0

    assert updated_at(pool, "a") != before
    (hit,) = search_events(pool, ny_query())
    assert hit.event == moved


def test_search_returns_the_stored_event_and_its_distance(pool: ConnectionPool) -> None:
    stored = event("a", lat=40.75, lon=-73.99)
    PostgresSink(pool).upsert([stored])

    (hit,) = search_events(pool, ny_query())

    assert hit.event == stored
    # PostGIS measures on the WGS 84 spheroid. The haversine value is for a sphere.
    assert hit.distance_km == pytest.approx(haversine_km(*NEW_YORK, 40.75, -73.99), rel=0.005)


def test_search_agrees_with_a_full_scan(pool: ConnectionPool, events: list[SimEvent]) -> None:
    PostgresSink(pool).upsert([to_event(sim) for sim in events])

    for radius_km in (5, 20, 50):
        found = {
            hit.event.id for hit in search_events(pool, ny_query(radius_km=radius_km, limit=6000))
        }
        distances = {sim.id: haversine_km(*NEW_YORK, sim.lat, sim.lon) for sim in events}
        # The sphere and the spheroid differ by less than 0.5 %, so events in that
        # band at the edge of the radius can be in or out.
        surely_in = {i for i, d in distances.items() if d <= radius_km * 0.995}
        surely_out = {i for i, d in distances.items() if d > radius_km * 1.005}
        assert surely_in <= found
        assert not found & surely_out
        assert len(surely_in) > 20


def test_search_includes_the_start_and_excludes_the_end(pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert(
        [event("at-start", days=1), event("middle", days=1.5), event("at-end", days=2)]
    )
    hits = search_events(
        pool, ny_query(start=BASE + timedelta(days=1), end=BASE + timedelta(days=2))
    )
    assert [hit.event.id for hit in hits] == ["at-start", "middle"]


def test_search_filters_by_segment(pool: ConnectionPool) -> None:
    PostgresSink(pool).upsert([event("music"), replace(event("sports"), segment="Sports")])
    hits = search_events(pool, ny_query(segment="Sports"))
    assert [hit.event.id for hit in hits] == ["sports"]


def test_pages_cover_each_event_one_time_in_order(pool: ConnectionPool) -> None:
    # Three events have the same start time, so the order must also use the id.
    stored = [event(f"e{n:02d}", days=1 + n // 3) for n in range(10)]
    PostgresSink(pool).upsert(stored)

    seen: list[str] = []
    after = None
    while True:
        hits = search_events(pool, ny_query(limit=4, after=after))
        seen += [hit.event.id for hit in hits]
        if len(hits) < 4:
            break
        after = (hits[-1].event.starts_at, hits[-1].event.id)

    assert seen == [e.id for e in stored]


def test_the_search_uses_the_index(pool: ConnectionPool, events: list[SimEvent]) -> None:
    PostgresSink(pool).upsert([to_event(sim) for sim in events])
    with pool.connection() as conn:
        conn.execute("ANALYZE events")
        plan = conn.execute(
            """
            EXPLAIN SELECT id FROM events
            WHERE ST_DWithin(
                location, ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326)::geography, 5000
            )
            AND starts_at >= '2026-01-10' AND starts_at < '2026-01-12'
            """
        ).fetchall()
    assert "events_location_time_idx" in "\n".join(row[0] for row in plan)


def test_the_state_store_returns_what_it_saved(pool: ConnectionPool) -> None:
    store = PostgresStateStore(pool)
    boston = Region("Boston", 42.3601, -71.0589, 50, 1.0)
    denver = Region("Denver", 39.7392, -104.9903, 50, 1.0)
    store.save(boston, RegionState(last_refreshed_at=BASE.timestamp(), expected_cost=7))
    store.save(denver, RegionState())
    store.save(boston, RegionState(last_refreshed_at=BASE.timestamp() + 60, expected_cost=9))

    assert store.load() == {
        "Boston": RegionState(last_refreshed_at=BASE.timestamp() + 60, expected_cost=9),
        "Denver": RegionState(),
    }
    assert last_refresh(pool, 42.4, -71.1) == BASE + timedelta(seconds=60)
    assert last_refresh(pool, 39.7, -105.0) is None  # Denver has no refresh yet
