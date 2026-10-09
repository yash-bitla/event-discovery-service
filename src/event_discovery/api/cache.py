"""Response cache for the query API."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections.abc import Callable
from typing import Protocol

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

logger = logging.getLogger(__name__)


class ResponseCache(Protocol):
    def get(self, key: str) -> bytes | None: ...

    def set(self, key: str, value: bytes) -> None: ...


class RedisCache:
    """Cache in Redis. A Redis failure is a cache miss, not an error.

    After a failure the cache does not use Redis for `cooldown_s` seconds. Without
    that pause, each request would wait for the timeout while Redis is not available.
    """

    def __init__(
        self,
        client: redis.Redis,
        *,
        ttl_s: int = 60,
        cooldown_s: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._cooldown_s = cooldown_s
        self._monotonic = monotonic
        self._skip_until = 0.0
        self.failures = 0

    @classmethod
    def from_url(cls, url: str, *, ttl_s: int = 60, timeout_s: float = 0.1) -> RedisCache:
        # The client must not retry. Its default is 3 retries with backoff, which
        # holds a request for seconds when Redis is not available.
        client = redis.Redis.from_url(
            url,
            socket_timeout=timeout_s,
            socket_connect_timeout=timeout_s,
            retry=Retry(NoBackoff(), 0),
        )
        return cls(client, ttl_s=ttl_s)

    def get(self, key: str) -> bytes | None:
        if self._monotonic() < self._skip_until:
            return None
        try:
            value = self._client.get(key)
        except redis.RedisError as error:
            self._failed(error)
            return None
        return value if isinstance(value, bytes) else None

    def set(self, key: str, value: bytes) -> None:
        if self._monotonic() < self._skip_until:
            return
        try:
            self._client.set(key, value, ex=self._ttl_s)
        except redis.RedisError as error:
            self._failed(error)

    def _failed(self, error: Exception) -> None:
        self.failures += 1
        self._skip_until = self._monotonic() + self._cooldown_s
        logger.warning("cache not available, the API reads the database: %s", error)


class _Call:
    def __init__(self) -> None:
        self.done = threading.Event()
        self.value: bytes = b""
        self.error: BaseException | None = None


class SingleFlight:
    """Run one computation for each key at a time. Other callers get its result.

    When a popular cache entry expires, many requests miss at the same moment.
    This lets one of them read the database, and the others wait for that result.
    """

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._calls: dict[str, _Call] = {}

    def do(self, key: str, compute: Callable[[], bytes]) -> bytes:
        with self._mutex:
            call = self._calls.get(key)
            leader = call is None
            if call is None:
                call = _Call()
                self._calls[key] = call
        if leader:
            try:
                call.value = compute()
            except BaseException as error:
                call.error = error
            finally:
                with self._mutex:
                    del self._calls[key]
                call.done.set()
        else:
            call.done.wait()
        if call.error is not None:
            raise call.error
        return call.value


def cache_key(*parts: object) -> str:
    digest = hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()
    return f"events:v1:{digest}"
