from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from event_discovery.clock import ManualClock
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import SimEvent, generate_events

BASE = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(BASE.timestamp())


@pytest.fixture(scope="session")
def events() -> list[SimEvent]:
    return generate_events(seed=7, base=BASE, count=6000)


def make_sim(
    events: list[SimEvent], clock: ManualClock, *, daily_quota: int = 5000, per_second: int = 5
) -> TestClient:
    app = create_app(events, clock, daily_quota=daily_quota, per_second=per_second)
    return TestClient(app)
