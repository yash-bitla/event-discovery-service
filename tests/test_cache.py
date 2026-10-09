from __future__ import annotations

import threading
import time
from datetime import timedelta

import pytest
import redis
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from conftest import BASE, DATABASE_URL, REDIS_URL, TEST_SCHEMA
from event_discovery.api.app import create_api
from event_discovery.api.asgi import create
from event_discovery.api.cache import RedisCache, SingleFlight, cache_key
from event_discovery.storage.events import PostgresSink
from test_storage import event

NEW_YORK = {"lat": 40.7128, "lon": -74.0060}


class FakeTime:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def unreachable_cache(fake_time: FakeTime) -> RedisCache:
    cache = RedisCache.from_url("redis://127.0.0.1:1/0")  # no server listens on port 1
    cache._monotonic = fake_time
    return cache


def test_the_cache_returns_what_it_stored_and_sets_the_ttl(redis_client: redis.Redis) -> None:
    cache = RedisCache(redis_client, ttl_s=60)
    assert cache.get("k") is None
    cache.set("k", b"value")
    assert cache.get("k") == b"value"
    assert 0 < redis_client.ttl("k") <= 60


def test_a_redis_failure_is_a_miss_and_starts_a_pause() -> None:
    fake_time = FakeTime()
    cache = unreachable_cache(fake_time)

    assert cache.get("k") is None
    assert cache.failures == 1
    cache.set("k", b"value")
    assert cache.get("k") is None
    assert cache.failures == 1  # in the pause, the cache does not try Redis

    fake_time.now += 5.0
    assert cache.get("k") is None
    assert cache.failures == 2


def test_cache_keys_differ_when_a_part_differs() -> None:
    assert cache_key(40.7128, -74.006, 10) == cache_key(40.7128, -74.006, 10)
    assert cache_key(40.7128, -74.006, 10) != cache_key(40.7128, -74.006, 25)


def test_single_flight_runs_one_computation_for_callers_at_the_same_time() -> None:
    flights = SingleFlight()
    computations = 0
    started = threading.Event()

    def compute() -> bytes:
        nonlocal computations
        computations += 1
        started.set()
        time.sleep(0.2)
        return b"result"

    results: list[bytes] = []
    threads = [
        threading.Thread(target=lambda: results.append(flights.do("k", compute))) for _ in range(8)
    ]
    threads[0].start()
    started.wait()
    for thread in threads[1:]:
        thread.start()
    for thread in threads:
        thread.join()

    assert computations == 1
    assert results == [b"result"] * 8
    assert flights.do("k", lambda: b"next") == b"next"  # a later call computes again


def test_single_flight_gives_the_error_to_the_caller() -> None:
    def fail() -> bytes:
        raise RuntimeError("database error")

    with pytest.raises(RuntimeError):
        SingleFlight().do("k", fail)


def test_the_second_request_is_a_hit(pool: ConnectionPool, redis_client: redis.Redis) -> None:
    api = TestClient(create_api(pool, now=lambda: BASE, cache=RedisCache(redis_client)))
    PostgresSink(pool).upsert([event("a")])

    first = api.get("/events", params=NEW_YORK)
    PostgresSink(pool).upsert([event("b")])  # the cached answer does not have this event
    second = api.get("/events", params=NEW_YORK)

    assert (first.headers["X-Cache"], second.headers["X-Cache"]) == ("miss", "hit")
    assert second.json() == first.json()
    assert [e["id"] for e in second.json()["events"]] == ["a"]


def test_points_less_than_the_rounding_step_apart_use_one_entry(
    pool: ConnectionPool, redis_client: redis.Redis
) -> None:
    api = TestClient(create_api(pool, now=lambda: BASE, cache=RedisCache(redis_client)))
    api.get("/events", params={"lat": 40.71281, "lon": -74.00601})
    near = api.get("/events", params={"lat": 40.71284, "lon": -74.00604})
    far = api.get("/events", params={"lat": 40.7138, "lon": -74.0060})
    assert (near.headers["X-Cache"], far.headers["X-Cache"]) == ("hit", "miss")


def test_requests_in_the_same_minute_use_one_entry(
    pool: ConnectionPool, redis_client: redis.Redis
) -> None:
    now = BASE
    api = TestClient(create_api(pool, now=lambda: now, cache=RedisCache(redis_client)))
    api.get("/events", params=NEW_YORK)
    now = BASE + timedelta(seconds=59)
    same_minute = api.get("/events", params=NEW_YORK)
    now = BASE + timedelta(seconds=60)
    next_minute = api.get("/events", params=NEW_YORK)
    assert (same_minute.headers["X-Cache"], next_minute.headers["X-Cache"]) == ("hit", "miss")


def test_different_parameters_do_not_share_an_entry(
    pool: ConnectionPool, redis_client: redis.Redis
) -> None:
    api = TestClient(create_api(pool, now=lambda: BASE, cache=RedisCache(redis_client)))
    api.get("/events", params=NEW_YORK)
    for extra in ({"radius_km": 11}, {"limit": 10}, {"segment": "Music"}):
        assert api.get("/events", params={**NEW_YORK, **extra}).headers["X-Cache"] == "miss"


def test_the_api_answers_when_redis_is_not_available(pool: ConnectionPool) -> None:
    cache = unreachable_cache(FakeTime())
    api = TestClient(create_api(pool, now=lambda: BASE, cache=cache))
    PostgresSink(pool).upsert([event("a")])

    response = api.get("/events", params=NEW_YORK)

    assert response.status_code == 200
    assert response.headers["X-Cache"] == "miss"
    assert [e["id"] for e in response.json()["events"]] == ["a"]
    assert cache.failures == 1


def test_without_a_cache_the_header_is_off(pool: ConnectionPool) -> None:
    api = TestClient(create_api(pool, now=lambda: BASE))
    assert api.get("/events", params=NEW_YORK).headers["X-Cache"] == "off"


def test_a_redis_failure_returns_fast() -> None:
    cache = RedisCache.from_url("redis://127.0.0.1:1/0", timeout_s=0.1)
    started = time.perf_counter()
    assert cache.get("k") is None
    assert time.perf_counter() - started < 0.5


def test_the_worker_factory_makes_an_app_from_the_environment(
    pool: ConnectionPool, redis_client: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EDS_DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("EDS_SCHEMA", TEST_SCHEMA)
    monkeypatch.setenv("EDS_POOL_SIZE", "2")
    monkeypatch.setenv("EDS_REDIS_URL", REDIS_URL)
    PostgresSink(pool).upsert([event("a", days=0.5)])

    api = TestClient(create())
    params = {**NEW_YORK, "start": "2026-01-01T00:00:00Z", "end": "2026-01-03T00:00:00Z"}
    first = api.get("/events", params=params)
    second = api.get("/events", params=params)

    assert [e["id"] for e in first.json()["events"]] == ["a"]
    assert (first.headers["X-Cache"], second.headers["X-Cache"]) == ("miss", "hit")
