from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime

import psycopg
import pytest
import redis
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from event_discovery.clock import ManualClock
from event_discovery.storage.db import migrate, open_pool
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import SimEvent, generate_events

BASE = datetime(2026, 1, 1, tzinfo=UTC)
DATABASE_URL = os.environ.get("EDS_TEST_DATABASE_URL", "postgresql://eds:eds@127.0.0.1:54329/eds")
TEST_SCHEMA = "eds_test"
REDIS_URL = os.environ.get("EDS_TEST_REDIS_URL", "redis://127.0.0.1:63799/15")


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


@pytest.fixture(scope="session")
def _pool() -> Iterator[ConnectionPool]:
    """A pool on the test schema. Start the database with `docker compose up -d db`."""
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=3) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    except psycopg.OperationalError:
        if os.environ.get("CI"):
            raise
        pytest.skip("the test database is not available")
    pool = open_pool(DATABASE_URL, schema=TEST_SCHEMA)
    migrate(pool, schema=TEST_SCHEMA)
    yield pool
    pool.close()


@pytest.fixture
def pool(_pool: ConnectionPool) -> ConnectionPool:
    with _pool.connection() as conn:
        conn.execute("TRUNCATE events, region_state")
    return _pool


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    """An empty Redis database. Start it with `docker compose up -d cache`."""
    client = redis.Redis.from_url(REDIS_URL, socket_timeout=1, socket_connect_timeout=1)
    try:
        client.flushdb()
    except redis.RedisError:
        if os.environ.get("CI"):
            raise
        pytest.skip("the test Redis is not available")
    yield client
    client.close()
