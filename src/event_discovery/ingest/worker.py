"""Ingestion cycle: plan, get the events of each planned region, store them."""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from event_discovery.clock import Clock
from event_discovery.ingest.budget import QuotaBudget
from event_discovery.ingest.client import (
    BudgetExhausted,
    DiscoveryClient,
    Page,
    QuotaExceeded,
    RateLimited,
    UpstreamError,
    UpstreamUnavailable,
)
from event_discovery.ingest.planner import Planner, Region, RegionState, plan_refresh
from event_discovery.ingest.resilience import CircuitBreaker, CircuitOpen
from event_discovery.models import Event

logger = logging.getLogger(__name__)

MAX_PAGING_DEPTH = 1000  # the upstream rejects a call if size * page is not less than this
_MIN_SPLIT = timedelta(hours=1)
CIRCUIT_OPEN = "circuit open"


class EventSink(Protocol):
    def upsert(self, events: Sequence[Event]) -> int:
        """Store the events. Return the number of events that were not stored before."""
        ...


class StateStore(Protocol):
    """Keeps the refresh state of each region, so that a restart does not lose it."""

    def load(self) -> dict[str, RegionState]: ...

    def save(self, region: Region, state: RegionState) -> None: ...


class InMemorySink:
    def __init__(self) -> None:
        self.events: dict[str, Event] = {}

    def upsert(self, events: Sequence[Event]) -> int:
        before = len(self.events)
        for event in events:
            self.events[event.id] = event
        return len(self.events) - before


@dataclass(frozen=True)
class CycleReport:
    planned: int
    refreshed: int
    failed: int
    calls: int
    events_received: int
    events_new: int
    stopped: str | None  # the reason, if the cycle stopped before the end of the plan


class IngestWorker:
    def __init__(
        self,
        client: DiscoveryClient,
        sink: EventSink,
        regions: Sequence[Region],
        clock: Clock,
        *,
        budget: QuotaBudget | None = None,
        planner: Planner = plan_refresh,
        horizon_days: int = 90,
        page_size: int = 200,
        state_store: StateStore | None = None,
    ) -> None:
        self._client = client
        self._sink = sink
        self._regions = list(regions)
        self._clock = clock
        self._budget = budget
        self._planner = planner
        self._horizon = timedelta(days=horizon_days)
        self._page_size = page_size
        self._reachable = math.ceil(MAX_PAGING_DEPTH / page_size) * page_size
        self._state_store = state_store
        stored = state_store.load() if state_store is not None else {}
        self.states = {
            region.name: stored.get(region.name, RegionState()) for region in self._regions
        }

    def run_cycle(self, interval_s: float) -> CycleReport:
        """Run one cycle. `interval_s` is the time until the next cycle."""
        allowance = len(self._regions) * MAX_PAGING_DEPTH
        if self._budget is not None:
            allowance = self._budget.allowance(interval_s)
        plan = self._planner(self._regions, self.states, self._clock.now(), allowance)

        calls_before = self._client.calls
        refreshed = failed = received = new = 0
        stopped: str | None = None
        for region in plan:
            region_calls_before = self._client.calls
            start = datetime.fromtimestamp(self._clock.now(), UTC).replace(microsecond=0)
            try:
                region_received, region_new = self._fetch(region, start, start + self._horizon)
            except BudgetExhausted:
                stopped = "budget exhausted"
            except QuotaExceeded:
                stopped = "quota exceeded"
            except RateLimited:
                stopped = "rate limited"
            except CircuitOpen:
                stopped = CIRCUIT_OPEN
            except (UpstreamError, UpstreamUnavailable) as error:
                logger.warning("refresh of %s failed: %s", region.name, error)
                failed += 1
                continue
            if stopped is not None:
                break
            received += region_received
            new += region_new
            refreshed += 1
            state = self.states[region.name]
            state.last_refreshed_at = self._clock.now()
            state.expected_cost = max(1, self._client.calls - region_calls_before)
            if self._state_store is not None:
                self._state_store.save(region, state)
        return CycleReport(
            planned=len(plan),
            refreshed=refreshed,
            failed=failed,
            calls=self._client.calls - calls_before,
            events_received=received,
            events_new=new,
            stopped=stopped,
        )

    def _fetch(self, region: Region, start: datetime, end: datetime) -> tuple[int, int]:
        """Get all events of the region that start in [start, end].

        If the range has more events than the paging limit permits, divide the range
        into two halves and get each half.
        """
        first = self._search(region, start, end, page=0)
        if first.total_elements > self._reachable:
            if end - start >= 2 * _MIN_SPLIT:
                middle = start + (end - start) / 2
                middle = middle.replace(microsecond=0)
                left = self._fetch(region, start, middle)
                right = self._fetch(region, middle, end)
                return left[0] + right[0], left[1] + right[1]
            logger.warning(
                "%s has %d events between %s and %s; only %d are reachable",
                region.name, first.total_elements, start, end, self._reachable,
            )  # fmt: skip
        received = len(first.events)
        new = self._sink.upsert(first.events)
        last_page = min(first.total_pages, self._reachable // self._page_size)
        for number in range(1, last_page):
            page = self._search(region, start, end, page=number)
            received += len(page.events)
            new += self._sink.upsert(page.events)
        return received, new

    def _search(self, region: Region, start: datetime, end: datetime, *, page: int) -> Page:
        return self._client.search(
            region.lat, region.lon, region.radius_km, start, end, page=page, size=self._page_size
        )


def seconds_to_next_cycle(
    report: CycleReport, interval_s: float, breaker: CircuitBreaker | None, now: float
) -> float:
    """Time to wait after a cycle.

    If the breaker stopped the cycle, the next cycle starts when the breaker permits
    a probe. Ingestion then starts again soon after the upstream is back, and does
    not wait for the remainder of the interval.
    """
    if report.stopped == CIRCUIT_OPEN and breaker is not None and breaker.retry_at is not None:
        return min(interval_s, max(0.0, breaker.retry_at - now))
    return interval_s
