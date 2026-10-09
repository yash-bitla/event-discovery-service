"""Local view of the upstream quota, and pacing for the per-second limit."""

from __future__ import annotations

from event_discovery.clock import Clock

_DAY_SECONDS = 86_400.0


class QuotaBudget:
    """Counts the calls that ingestion can still make before the quota resets.

    `reserve` calls stay unused, so that work outside the schedule has quota.
    The count starts from `daily_limit`. After each response, `observe` replaces
    the count with the value that the upstream reports.
    """

    def __init__(self, clock: Clock, daily_limit: int = 5000, reserve: int = 500) -> None:
        if not 0 <= reserve <= daily_limit:
            raise ValueError("reserve must be between 0 and daily_limit")
        self._clock = clock
        self._daily_limit = daily_limit
        self._reserve = reserve
        self._available = daily_limit
        self._reset_at: float | None = None

    @property
    def spendable(self) -> int:
        self._roll()
        return max(0, self._available - self._reserve)

    def try_spend(self) -> bool:
        """Take one call from the budget. Return False if no call is spendable."""
        if self.spendable <= 0:
            return False
        self._available -= 1
        return True

    def observe(self, available: int, reset_at: float) -> None:
        self._available = available
        self._reset_at = reset_at

    def allowance(self, interval_s: float) -> int:
        """Calls to use in the next `interval_s` seconds.

        This spreads the spendable calls evenly across the time until the reset.
        """
        remaining = _DAY_SECONDS
        if self._reset_at is not None:
            remaining = self._reset_at - self._clock.now()
        spendable = self.spendable
        if remaining <= interval_s:
            return spendable
        return int(spendable * interval_s / remaining)

    def _roll(self) -> None:
        if self._reset_at is not None and self._clock.now() >= self._reset_at:
            self._available = self._daily_limit
            self._reset_at = None


class RatePacer:
    """Spaces the calls so that no more than `per_second` start in one second."""

    def __init__(self, clock: Clock, per_second: float) -> None:
        if per_second <= 0:
            raise ValueError("per_second must be positive")
        self._clock = clock
        self._interval = 1.0 / per_second
        self._next_at = 0.0

    def wait(self) -> None:
        now = self._clock.now()
        if now < self._next_at:
            self._clock.sleep(self._next_at - now)
            now = self._clock.now()
        self._next_at = now + self._interval
