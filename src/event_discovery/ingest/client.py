"""Client for the Discovery API event search."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from event_discovery.geo import geohash_encode
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.resilience import CircuitBreaker, Retrier
from event_discovery.models import Event

_QUOTA_VIOLATION = "policies.ratelimit.QuotaViolation"


class BudgetExhausted(Exception):
    """The local budget has no spendable call. The client did not send the request."""


class QuotaExceeded(Exception):
    """The upstream rejected the call because the daily quota is used."""


class RateLimited(Exception):
    """The upstream rejected the call because of the per-second limit."""


class UpstreamError(Exception):
    """The upstream rejected the request (HTTP 4xx). A retry does not help."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"upstream returned HTTP {status_code}")
        self.status_code = status_code


class UpstreamUnavailable(Exception):
    """The upstream returned HTTP 5xx or did not answer. A retry can help."""

    def __init__(self, status_code: int | None, reason: str) -> None:
        super().__init__(reason)
        self.status_code = status_code


@dataclass(frozen=True)
class Page:
    events: list[Event]
    skipped: int  # events without a start time or a venue location
    total_elements: int
    total_pages: int
    number: int


class DiscoveryClient:
    """Each attempt goes through the breaker, the budget and the pacer, in that order.

    A retry is an attempt, so a retry also uses the budget. With `budget=None` the
    client does not check a budget. With `retrier=None` it does not retry.
    """

    def __init__(
        self,
        http: httpx.Client,
        api_key: str,
        pacer: RatePacer,
        budget: QuotaBudget | None = None,
        *,
        retrier: Retrier | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self._http = http
        self._api_key = api_key
        self._pacer = pacer
        self._budget = budget
        self._retrier = retrier
        self._breaker = breaker
        self.calls = 0

    def search(
        self,
        lat: float,
        lon: float,
        radius_km: int,
        start: datetime,
        end: datetime,
        *,
        page: int = 0,
        size: int = 200,
    ) -> Page:
        params: dict[str, str | int] = {
            "apikey": self._api_key,
            "geoPoint": geohash_encode(lat, lon),
            "radius": radius_km,
            "unit": "km",
            "startDateTime": _format_datetime(start),
            "endDateTime": _format_datetime(end),
            "sort": "date,asc",
            "size": size,
            "page": page,
        }
        failed_attempts = 0
        while True:
            try:
                return self._attempt(params)
            except (UpstreamUnavailable, RateLimited):
                failed_attempts += 1
                if self._retrier is None or failed_attempts >= self._retrier.max_attempts:
                    raise
                self._retrier.wait(failed_attempts)

    def _attempt(self, params: dict[str, str | int]) -> Page:
        if self._breaker is not None:
            self._breaker.before_call()
        if self._budget is not None and not self._budget.try_spend():
            raise BudgetExhausted
        self._pacer.wait()
        self.calls += 1
        try:
            response = self._http.get("/discovery/v2/events.json", params=params)
        except httpx.TransportError as error:
            self._record(healthy=False)
            raise UpstreamUnavailable(None, type(error).__name__) from error
        self._observe(response)
        if response.status_code >= 500:
            self._record(healthy=False)
            raise UpstreamUnavailable(
                response.status_code, f"upstream returned HTTP {response.status_code}"
            )
        # All other responses show that the upstream is in operation.
        self._record(healthy=True)
        if response.status_code == 429:
            if _error_code(response) == _QUOTA_VIOLATION:
                raise QuotaExceeded
            raise RateLimited
        if response.status_code != 200:
            raise UpstreamError(response.status_code)
        return parse_page(response.json())

    def _record(self, *, healthy: bool) -> None:
        if self._breaker is None:
            return
        if healthy:
            self._breaker.record_success()
        else:
            self._breaker.record_failure()

    def _observe(self, response: httpx.Response) -> None:
        if self._budget is None:
            return
        try:
            available = int(response.headers["Rate-Limit-Available"])
            reset_at = int(response.headers["Rate-Limit-Reset"]) / 1000
        except (KeyError, ValueError):
            return
        self._budget.observe(available, reset_at)


def parse_page(body: dict[str, Any]) -> Page:
    raw_events = body.get("_embedded", {}).get("events", [])
    events = [event for raw in raw_events if (event := parse_event(raw)) is not None]
    page = body.get("page", {})
    return Page(
        events=events,
        skipped=len(raw_events) - len(events),
        total_elements=int(page.get("totalElements", len(raw_events))),
        total_pages=int(page.get("totalPages", 1)),
        number=int(page.get("number", 0)),
    )


def parse_event(raw: dict[str, Any]) -> Event | None:
    """Return None if the event has no start time or no venue location."""
    try:
        venue = raw["_embedded"]["venues"][0]
        classifications = raw.get("classifications") or [{}]
        return Event(
            id=str(raw["id"]),
            name=str(raw["name"]),
            url=str(raw.get("url", "")),
            starts_at=_parse_datetime(raw["dates"]["start"]["dateTime"]),
            segment=classifications[0].get("segment", {}).get("name"),
            venue=str(venue.get("name", "")),
            city=str(venue.get("city", {}).get("name", "")),
            lat=float(venue["location"]["latitude"]),
            lon=float(venue["location"]["longitude"]),
        )
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _error_code(response: httpx.Response) -> str | None:
    try:
        code = response.json()["fault"]["detail"]["errorcode"]
    except (ValueError, KeyError, TypeError):
        return None
    return str(code)


def _format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_datetime(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
