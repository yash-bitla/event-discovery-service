from __future__ import annotations

import pytest

from event_discovery.clock import ManualClock
from event_discovery.ingest.resilience import CircuitBreaker, CircuitOpen, Retrier


def test_the_wait_grows_with_each_failed_attempt_and_stops_at_the_cap(clock: ManualClock) -> None:
    retrier = Retrier(clock, base_s=1.0, cap_s=8.0, seed=3)
    for failed_attempts, limit in [(1, 2.0), (2, 4.0), (3, 8.0), (6, 8.0)]:
        waits = []
        for _ in range(200):
            before = clock.now()
            retrier.wait(failed_attempts)
            waits.append(clock.now() - before)
        assert max(waits) <= limit
        assert max(waits) > limit * 0.9  # the full range is used
        assert min(waits) < limit * 0.1


def test_the_breaker_opens_after_the_threshold_of_failures_in_sequence(clock: ManualClock) -> None:
    breaker = CircuitBreaker(clock, failure_threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()  # a success starts the count again
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == "closed"
    breaker.before_call()

    breaker.record_failure()

    assert breaker.state == "open"
    with pytest.raises(CircuitOpen) as error:
        breaker.before_call()
    assert error.value.retry_at == clock.now() + 30


def test_a_successful_probe_closes_the_breaker(clock: ManualClock) -> None:
    breaker = CircuitBreaker(clock, failure_threshold=1, open_s=30)
    breaker.record_failure()
    clock.sleep(30)

    breaker.before_call()
    assert breaker.state == "half-open"
    breaker.record_success()

    assert breaker.state == "closed"


def test_each_failed_probe_doubles_the_open_time_to_the_maximum(clock: ManualClock) -> None:
    breaker = CircuitBreaker(clock, failure_threshold=1, open_s=30, max_open_s=100)
    open_times = []
    breaker.record_failure()
    for _ in range(4):
        assert breaker.retry_at is not None
        open_times.append(breaker.retry_at - clock.now())
        clock.advance_to(breaker.retry_at)
        breaker.before_call()
        breaker.record_failure()

    assert open_times == [30, 60, 100, 100]
    assert breaker.times_opened == 5


def test_the_open_time_starts_again_after_a_successful_probe(clock: ManualClock) -> None:
    breaker = CircuitBreaker(clock, failure_threshold=1, open_s=30)
    breaker.record_failure()
    clock.sleep(30)
    breaker.before_call()
    breaker.record_failure()  # open for 60 s
    clock.sleep(60)
    breaker.before_call()
    breaker.record_success()

    breaker.record_failure()

    assert breaker.retry_at == clock.now() + 30
