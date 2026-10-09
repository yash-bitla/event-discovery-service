"""Daily quota and per-second rate limit, counted for each API key."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum

from event_discovery.clock import Clock

_DAY_SECONDS = 86_400


class Verdict(Enum):
    OK = "ok"
    QUOTA = "quota"
    SPIKE = "spike"


@dataclass(frozen=True)
class Decision:
    verdict: Verdict
    limit: int
    available: int
    over: int
    reset_ms: int


@dataclass
class _KeyUsage:
    day: int
    used: int = 0
    over: int = 0
    recent: deque[float] = field(default_factory=deque)


@dataclass
class LimiterStats:
    accepted: int = 0
    rejected_quota: int = 0
    rejected_spike: int = 0


class RateLimiter:
    """The quota resets at 00:00 UTC. The rate limit is a sliding window of one second.

    The per-second check runs first. A call that fails it does not use the quota.
    """

    def __init__(self, clock: Clock, daily_quota: int = 5000, per_second: int = 5) -> None:
        self._clock = clock
        self._daily_quota = daily_quota
        self._per_second = per_second
        self._usage: dict[str, _KeyUsage] = {}
        self.stats = LimiterStats()

    def check(self, api_key: str) -> Decision:
        now = self._clock.now()
        day = int(now // _DAY_SECONDS)
        usage = self._usage.get(api_key)
        if usage is None or usage.day != day:
            usage = _KeyUsage(day=day)
            self._usage[api_key] = usage

        while usage.recent and usage.recent[0] <= now - 1.0:
            usage.recent.popleft()
        if len(usage.recent) >= self._per_second:
            self.stats.rejected_spike += 1
            return self._decision(Verdict.SPIKE, usage)

        if usage.used >= self._daily_quota:
            usage.over += 1
            self.stats.rejected_quota += 1
            return self._decision(Verdict.QUOTA, usage)

        usage.used += 1
        usage.recent.append(now)
        self.stats.accepted += 1
        return self._decision(Verdict.OK, usage)

    def _decision(self, verdict: Verdict, usage: _KeyUsage) -> Decision:
        return Decision(
            verdict=verdict,
            limit=self._daily_quota,
            available=self._daily_quota - usage.used,
            over=usage.over,
            reset_ms=(usage.day + 1) * _DAY_SECONDS * 1000,
        )
