"""Retries with backoff, and a circuit breaker."""

from __future__ import annotations

import random

from event_discovery.clock import Clock


class CircuitOpen(Exception):
    """The breaker is open. The client did not send the request."""

    def __init__(self, retry_at: float) -> None:
        super().__init__("circuit open")
        self.retry_at = retry_at


class Retrier:
    """Exponential backoff with full jitter.

    Before attempt `n + 1` the wait is a random time between 0 and
    `min(cap_s, base_s * 2**n)`. The random part stops many clients from a retry
    at the same moment.
    """

    def __init__(
        self,
        clock: Clock,
        *,
        max_attempts: int = 4,
        base_s: float = 0.5,
        cap_s: float = 30.0,
        seed: int | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be 1 or more")
        self.max_attempts = max_attempts
        self._clock = clock
        self._base_s = base_s
        self._cap_s = cap_s
        self._rng = random.Random(seed)

    def wait(self, failed_attempts: int) -> None:
        limit = min(self._cap_s, self._base_s * 2**failed_attempts)
        self._clock.sleep(self._rng.uniform(0, limit))


class CircuitBreaker:
    """Stops the calls to an upstream that continues to fail.

    closed:    calls go through. `failure_threshold` failures in sequence open it.
    open:      no call goes through until `retry_at`.
    half-open: one call goes through as a probe. If the probe is successful, the
               breaker closes. If it fails, the breaker opens again for two times
               as long, to a maximum of `max_open_s`. Each probe uses one call of
               the quota, so a long outage must not get a probe each few seconds.
    """

    def __init__(
        self,
        clock: Clock,
        *,
        failure_threshold: int = 5,
        open_s: float = 30.0,
        max_open_s: float = 300.0,
    ) -> None:
        self._clock = clock
        self._threshold = failure_threshold
        self._first_open_s = open_s
        self._max_open_s = max_open_s
        self._next_open_s = open_s
        self._failures = 0
        self._half_open = False
        self.retry_at: float | None = None  # set while the breaker is open
        self.times_opened = 0

    @property
    def state(self) -> str:
        if self._half_open:
            return "half-open"
        return "closed" if self.retry_at is None else "open"

    def before_call(self) -> None:
        """Raise CircuitOpen if the call must not go through."""
        if self.retry_at is None:
            return
        if self._clock.now() < self.retry_at:
            raise CircuitOpen(self.retry_at)
        self.retry_at = None
        self._half_open = True

    def record_success(self) -> None:
        self._failures = 0
        self._half_open = False
        self._next_open_s = self._first_open_s

    def record_failure(self) -> None:
        self._failures += 1
        if self._half_open or self._failures >= self._threshold:
            self._half_open = False
            self._failures = 0
            self.retry_at = self._clock.now() + self._next_open_s
            self._next_open_s = min(self._max_open_s, self._next_open_s * 2)
            self.times_opened += 1
