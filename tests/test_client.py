from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from conftest import BASE, make_sim
from event_discovery.clock import ManualClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import (
    BudgetExhausted,
    DiscoveryClient,
    QuotaExceeded,
    RateLimited,
    UpstreamError,
    parse_event,
    parse_page,
)
from event_discovery.upstream_sim.dataset import SimEvent

END = BASE + timedelta(days=90)


def raw_event() -> dict[str, Any]:
    return {
        "id": "E1",
        "name": "Concert",
        "url": "https://events.example/E1",
        "dates": {"start": {"dateTime": "2026-03-04T19:30:00Z"}},
        "classifications": [{"segment": {"name": "Music"}}],
        "_embedded": {
            "venues": [
                {
                    "name": "Hall",
                    "city": {"name": "Boston"},
                    "location": {"latitude": "42.36", "longitude": "-71.06"},
                }
            ]
        },
    }


def test_parse_event_reads_the_fields() -> None:
    event = parse_event(raw_event())
    assert event is not None
    assert event.starts_at == datetime(2026, 3, 4, 19, 30, tzinfo=UTC)
    assert (event.segment, event.venue, event.city) == ("Music", "Hall", "Boston")
    assert (event.lat, event.lon) == (42.36, -71.06)


def test_parse_page_skips_events_without_a_start_time_or_a_location() -> None:
    no_time = raw_event()
    no_time["dates"] = {"start": {"dateTBD": True}}
    no_location = raw_event()
    del no_location["_embedded"]["venues"][0]["location"]
    no_venue = raw_event()
    del no_venue["_embedded"]

    page = parse_page(
        {
            "_embedded": {"events": [raw_event(), no_time, no_location, no_venue]},
            "page": {"size": 20, "totalElements": 4, "totalPages": 1, "number": 0},
        }
    )
    assert len(page.events) == 1
    assert page.skipped == 3


def test_parse_page_accepts_a_response_with_no_events() -> None:
    page = parse_page({"page": {"size": 20, "totalElements": 0, "totalPages": 0, "number": 0}})
    assert page.events == []
    assert page.total_elements == 0


def test_search_updates_the_budget_from_the_headers(
    events: list[SimEvent], clock: ManualClock
) -> None:
    budget = QuotaBudget(clock, daily_limit=5000, reserve=0)
    client = DiscoveryClient(make_sim(events, clock, daily_quota=50), "demo", pacer(clock), budget)

    page = client.search(40.7128, -74.0060, 50, BASE, END)

    assert len(page.events) == 200
    assert budget.spendable == 49  # the upstream value replaces the local 4999


def test_search_does_not_send_a_call_when_the_budget_is_empty(
    events: list[SimEvent], clock: ManualClock
) -> None:
    sim = make_sim(events, clock)
    budget = QuotaBudget(clock, daily_limit=10, reserve=10)
    client = DiscoveryClient(sim, "demo", pacer(clock), budget)

    with pytest.raises(BudgetExhausted):
        client.search(40.7128, -74.0060, 50, BASE, END)
    assert sim.get("/_sim/stats").json()["accepted"] == 0


def test_search_raises_quota_exceeded(events: list[SimEvent], clock: ManualClock) -> None:
    client = DiscoveryClient(make_sim(events, clock, daily_quota=1), "demo", pacer(clock))
    client.search(40.7128, -74.0060, 50, BASE, END)
    with pytest.raises(QuotaExceeded):
        client.search(40.7128, -74.0060, 50, BASE, END)


def test_search_raises_rate_limited(events: list[SimEvent], clock: ManualClock) -> None:
    client = DiscoveryClient(make_sim(events, clock, per_second=1), "demo", pacer(clock))
    client.search(40.7128, -74.0060, 50, BASE, END)
    with pytest.raises(RateLimited):
        client.search(40.7128, -74.0060, 50, BASE, END)


def test_search_raises_upstream_error_for_other_statuses(clock: ManualClock) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(503))
    http = httpx.Client(transport=transport, base_url="http://upstream.test")
    client = DiscoveryClient(http, "demo", pacer(clock))
    with pytest.raises(UpstreamError) as error:
        client.search(40.7128, -74.0060, 50, BASE, END)
    assert error.value.status_code == 503


def pacer(clock: ManualClock) -> RatePacer:
    return RatePacer(clock, per_second=4)
