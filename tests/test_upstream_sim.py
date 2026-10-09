from __future__ import annotations

from datetime import timedelta

from conftest import BASE, make_sim
from event_discovery.clock import ManualClock
from event_discovery.geo import geohash_encode, haversine_km
from event_discovery.upstream_sim.dataset import SimEvent, format_datetime, generate_events

NEW_YORK = (40.7128, -74.0060)


def ny_params(**extra: object) -> dict[str, object]:
    return {
        "apikey": "demo",
        "geoPoint": geohash_encode(*NEW_YORK),
        "radius": 50,
        "unit": "km",
        "size": 200,
        **extra,
    }


def test_the_same_seed_gives_the_same_events() -> None:
    assert generate_events(3, BASE, 50) == generate_events(3, BASE, 50)


def test_search_returns_the_events_in_the_radius(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    expected = {e.id for e in events if haversine_km(*NEW_YORK, e.lat, e.lon) <= 50}

    found: set[str] = set()
    for page in range(5):
        clock.sleep(1)
        body = sim.get("/discovery/v2/events.json", params=ny_params(page=page)).json()
        found |= {event["id"] for event in body.get("_embedded", {}).get("events", [])}

    assert body["page"]["totalElements"] == len(expected)
    assert 0 < len(expected) <= 1000
    assert found == expected


def test_search_filters_by_start_time(events: list[SimEvent], clock: ManualClock) -> None:
    sim = make_sim(events, clock)
    start, end = BASE + timedelta(days=10), BASE + timedelta(days=11)
    params = ny_params(startDateTime=format_datetime(start), endDateTime=format_datetime(end))
    body = sim.get("/discovery/v2/events.json", params=params).json()

    times = [event["dates"]["start"]["dateTime"] for event in body["_embedded"]["events"]]
    assert times == sorted(times)
    assert format_datetime(start) <= times[0] and times[-1] <= format_datetime(end)


def test_an_unknown_api_key_gets_401(events: list[SimEvent], clock: ManualClock) -> None:
    sim = make_sim(events, clock)
    response = sim.get("/discovery/v2/events.json", params={"apikey": "wrong"})
    assert response.status_code == 401


def test_deep_paging_gets_400(events: list[SimEvent], clock: ManualClock) -> None:
    sim = make_sim(events, clock)
    assert sim.get("/discovery/v2/events.json", params=ny_params(page=4)).status_code == 200
    assert sim.get("/discovery/v2/events.json", params=ny_params(page=5)).status_code == 400


def test_rate_limit_headers_count_down(events: list[SimEvent], clock: ManualClock) -> None:
    sim = make_sim(events, clock, daily_quota=10)
    response = sim.get("/discovery/v2/events.json", params=ny_params())
    assert response.headers["Rate-Limit"] == "10"
    assert response.headers["Rate-Limit-Available"] == "9"
    assert response.headers["Rate-Limit-Over"] == "0"
    next_midnight = (BASE + timedelta(days=1)).timestamp()
    assert int(response.headers["Rate-Limit-Reset"]) == next_midnight * 1000


def test_quota_rejects_calls_until_the_reset(events: list[SimEvent], clock: ManualClock) -> None:
    sim = make_sim(events, clock, daily_quota=3)
    for _ in range(3):
        clock.sleep(1)
        assert sim.get("/discovery/v2/events.json", params=ny_params()).status_code == 200

    clock.sleep(1)
    rejected = sim.get("/discovery/v2/events.json", params=ny_params())
    assert rejected.status_code == 429
    assert rejected.json()["fault"]["detail"]["errorcode"] == "policies.ratelimit.QuotaViolation"
    assert rejected.headers["Rate-Limit-Over"] == "1"

    clock.advance_to((BASE + timedelta(days=1)).timestamp())
    assert sim.get("/discovery/v2/events.json", params=ny_params()).status_code == 200
    assert sim.get("/_sim/stats").json() == {
        "accepted": 4,
        "rejected_quota": 1,
        "rejected_spike": 0,
    }


def test_the_sixth_call_in_one_second_is_rejected_and_uses_no_quota(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock, daily_quota=100)
    for _ in range(5):
        assert sim.get("/discovery/v2/events.json", params=ny_params()).status_code == 200

    rejected = sim.get("/discovery/v2/events.json", params=ny_params())
    assert rejected.status_code == 429
    code = rejected.json()["fault"]["detail"]["errorcode"]
    assert code == "policies.ratelimit.SpikeArrestViolation"
    assert rejected.headers["Rate-Limit-Available"] == "95"

    clock.sleep(1)
    assert sim.get("/discovery/v2/events.json", params=ny_params()).status_code == 200
