"""A local copy of the Ticketmaster Discovery API event search, with its limits.

It copies the limits that Ticketmaster documents: a daily quota, a per-second rate
limit, the `Rate-Limit-*` response headers, the 429 fault body, and the deep paging
limit (`size * page < 1000`). The events are synthetic.

`PUT /_sim/faults` makes the upstream fail on command. A call that fails in this
way uses the quota, because the limits run before the fault.
"""

from __future__ import annotations

import math
from collections import OrderedDict, defaultdict
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from event_discovery.clock import Clock
from event_discovery.geo import geohash_decode, haversine_km
from event_discovery.upstream_sim.dataset import SimEvent
from event_discovery.upstream_sim.faults import FaultInjector, FaultPlan
from event_discovery.upstream_sim.limits import Decision, RateLimiter, Verdict

MAX_PAGE_SIZE = 200
MAX_PAGING_DEPTH = 1000
_KM_PER_MILE = 1.609344
_QUERY_CACHE_SIZE = 64


class _BadRequest(Exception):
    pass


class _EventIndex:
    """Events in one-degree cells, so that a search reads only the cells near the point."""

    def __init__(self, events: Sequence[SimEvent]) -> None:
        self._cells: dict[tuple[int, int], list[SimEvent]] = defaultdict(list)
        for event in events:
            self._cells[(math.floor(event.lat), math.floor(event.lon))].append(event)

    def search(
        self,
        point: tuple[float, float] | None,
        radius_km: float,
        start: datetime | None,
        end: datetime | None,
    ) -> list[SimEvent]:
        if point is None:
            candidates = [event for cell in self._cells.values() for event in cell]
        else:
            lat, lon = point
            dlat = radius_km / 110.57
            dlon = radius_km / (111.32 * max(0.01, math.cos(math.radians(lat))))
            candidates = [
                event
                for cell_lat in range(math.floor(lat - dlat), math.floor(lat + dlat) + 1)
                for cell_lon in range(math.floor(lon - dlon), math.floor(lon + dlon) + 1)
                for event in self._cells.get((cell_lat, cell_lon), ())
                if haversine_km(lat, lon, event.lat, event.lon) <= radius_km
            ]
        matches = [
            event
            for event in candidates
            if (start is None or event.starts_at >= start)
            and (end is None or event.starts_at <= end)
        ]
        matches.sort(key=lambda event: (event.starts_at, event.id))
        return matches


def create_app(
    events: Sequence[SimEvent],
    clock: Clock,
    *,
    api_keys: Sequence[str] = ("demo",),
    daily_quota: int = 5000,
    per_second: int = 5,
    fault_seed: int = 0,
) -> FastAPI:
    app = FastAPI(title="Simulated Discovery API", docs_url=None, redoc_url=None)
    limiter = RateLimiter(clock, daily_quota=daily_quota, per_second=per_second)
    faults = FaultInjector(clock, seed=fault_seed)
    index = _EventIndex(events)
    valid_keys = frozenset(api_keys)
    cache: OrderedDict[tuple[str, ...], list[SimEvent]] = OrderedDict()

    def search(params: dict[str, str]) -> list[SimEvent]:
        key = tuple(
            params.get(name, "")
            for name in ("geoPoint", "radius", "unit", "startDateTime", "endDateTime")
        )
        cached = cache.get(key)
        if cached is not None:
            cache.move_to_end(key)
            return cached
        point = None
        if params.get("geoPoint"):
            try:
                point = geohash_decode(params["geoPoint"])
            except ValueError as error:
                raise _BadRequest(str(error)) from error
        radius = _parse_int(params, "radius", default=25, minimum=0)
        radius_km = radius if params.get("unit", "miles") == "km" else radius * _KM_PER_MILE
        matches = index.search(
            point,
            radius_km,
            _parse_datetime(params, "startDateTime"),
            _parse_datetime(params, "endDateTime"),
        )
        cache[key] = matches
        if len(cache) > _QUERY_CACHE_SIZE:
            cache.popitem(last=False)
        return matches

    @app.get("/discovery/v2/events")
    @app.get("/discovery/v2/events.json")
    def search_events(request: Request) -> JSONResponse:
        params = dict(request.query_params)
        api_key = params.get("apikey", "")
        if api_key not in valid_keys:
            return _fault(401, "Invalid ApiKey", "oauth.v2.InvalidApiKey")

        decision = limiter.check(api_key)
        headers = _rate_limit_headers(decision)
        if decision.verdict is Verdict.SPIKE:
            return _fault(
                429,
                f"Spike arrest violation. Allowed rate : {per_second}ps",
                "policies.ratelimit.SpikeArrestViolation",
                headers,
            )
        if decision.verdict is Verdict.QUOTA:
            return _fault(
                429,
                f"Rate limit quota violation. Quota limit exceeded. Identifier : {api_key}",
                "policies.ratelimit.QuotaViolation",
                headers,
            )

        if faults.should_fail():
            return _fault(
                503,
                "The Service is temporarily unavailable",
                "messaging.adaptors.http.flow.ServiceUnavailable",
                headers,
            )

        try:
            size = _parse_int(params, "size", default=20, minimum=1)
            page = _parse_int(params, "page", default=0, minimum=0)
            if size > MAX_PAGE_SIZE:
                raise _BadRequest(f"size must not be more than {MAX_PAGE_SIZE}")
            if size * page >= MAX_PAGING_DEPTH:
                raise _BadRequest(
                    f"Max paging depth exceeded. (page * size) must be less than {MAX_PAGING_DEPTH}"
                )
            matches = search(params)
        except _BadRequest as error:
            errors = {"errors": [{"code": "DIS1035", "detail": str(error), "status": "400"}]}
            return JSONResponse(errors, status_code=400, headers=headers)

        body: dict[str, Any] = {
            "page": {
                "size": size,
                "totalElements": len(matches),
                "totalPages": math.ceil(len(matches) / size),
                "number": page,
            }
        }
        page_events = matches[page * size : (page + 1) * size]
        if page_events:
            body["_embedded"] = {"events": [event.to_api() for event in page_events]}
        return JSONResponse(body, headers=headers)

    @app.get("/_sim/stats")
    def stats() -> dict[str, int]:
        """Counts of the calls that the limits accepted and rejected. Not a Discovery route."""
        return {**asdict(limiter.stats), "faults_injected": faults.injected}

    @app.put("/_sim/faults")
    def set_faults(body: Annotated[dict[str, Any], Body()]) -> dict[str, Any]:
        """Replace the fault plan. An empty object removes all faults."""
        try:
            faults.plan = FaultPlan.from_json(body)
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error
        return asdict(faults.plan)

    return app


def _rate_limit_headers(decision: Decision) -> dict[str, str]:
    return {
        "Rate-Limit": str(decision.limit),
        "Rate-Limit-Available": str(decision.available),
        "Rate-Limit-Over": str(decision.over),
        "Rate-Limit-Reset": str(decision.reset_ms),
    }


def _fault(
    status: int, message: str, code: str, headers: dict[str, str] | None = None
) -> JSONResponse:
    body = {"fault": {"faultstring": message, "detail": {"errorcode": code}}}
    return JSONResponse(body, status_code=status, headers=headers)


def _parse_int(params: dict[str, str], name: str, *, default: int, minimum: int) -> int:
    raw = params.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise _BadRequest(f"{name} must be an integer") from error
    if value < minimum:
        raise _BadRequest(f"{name} must not be less than {minimum}")
    return value


def _parse_datetime(params: dict[str, str], name: str) -> datetime | None:
    raw = params.get(name)
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise _BadRequest(f"{name} must have the format YYYY-MM-DDTHH:mm:ssZ") from error
