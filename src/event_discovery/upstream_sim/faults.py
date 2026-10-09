"""Faults that a test or a benchmark can switch on in the simulated upstream."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from event_discovery.clock import Clock


@dataclass
class FaultPlan:
    """`error_rate` is the share of calls that get HTTP 503.

    In an outage, each call gets HTTP 503. An outage is a pair of Unix times
    (start, end). `latency_s` is a delay before each response.
    """

    error_rate: float = 0.0
    latency_s: float = 0.0
    outages: list[tuple[float, float]] = field(default_factory=list)

    @classmethod
    def from_json(cls, body: dict[str, Any]) -> FaultPlan:
        plan = cls(
            error_rate=float(body.get("error_rate", 0.0)),
            latency_s=float(body.get("latency_s", 0.0)),
            outages=[(float(start), float(end)) for start, end in body.get("outages", [])],
        )
        if not 0.0 <= plan.error_rate <= 1.0 or plan.latency_s < 0:
            raise ValueError("error_rate must be in [0, 1] and latency_s must not be negative")
        return plan


class FaultInjector:
    def __init__(self, clock: Clock, seed: int = 0) -> None:
        self._clock = clock
        self._rng = random.Random(seed)
        self.plan = FaultPlan()
        self.injected = 0

    def should_fail(self) -> bool:
        """Apply the delay, then report if this call must fail."""
        if self.plan.latency_s:
            self._clock.sleep(self.plan.latency_s)
        now = self._clock.now()
        fail = any(start <= now < end for start, end in self.plan.outages) or (
            self.plan.error_rate > 0 and self._rng.random() < self.plan.error_rate
        )
        if fail:
            self.injected += 1
        return fail
