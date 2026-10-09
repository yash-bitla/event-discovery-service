"""Query API: events near a point in a time range."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from event_discovery.storage.events import EventHit, SearchQuery, search_events
from event_discovery.storage.regions import last_refresh

MAX_RADIUS_KM = 200
MAX_LIMIT = 200
DEFAULT_RANGE = timedelta(days=30)


def create_api(
    pool: ConnectionPool, now: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> FastAPI:
    app = FastAPI(title="Event discovery", docs_url=None, redoc_url=None)

    @app.get("/events")
    def get_events(
        lat: Annotated[float, Query(ge=-90, le=90)],
        lon: Annotated[float, Query(ge=-180, le=180)],
        radius_km: Annotated[float, Query(gt=0, le=MAX_RADIUS_KM)] = 10,
        start: Annotated[datetime | None, Query(description="default: now")] = None,
        end: Annotated[datetime | None, Query(description="default: start + 30 days")] = None,
        segment: str | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        range_start = _as_utc(start) if start else now()
        range_end = _as_utc(end) if end else range_start + DEFAULT_RANGE
        if range_end <= range_start:
            raise HTTPException(422, "end must be after start")
        # One more row than the limit shows if a next page exists.
        hits = search_events(
            pool,
            SearchQuery(
                lat=lat,
                lon=lon,
                radius_km=radius_km,
                start=range_start,
                end=range_end,
                segment=segment,
                limit=limit + 1,
                after=_decode_cursor(cursor) if cursor else None,
            ),
        )
        page = hits[:limit]
        next_cursor = None
        if len(hits) > limit:
            last = page[-1].event
            next_cursor = _encode_cursor(last.starts_at, last.id)
        # The API reads only the database, so it answers also when the upstream is
        # not available. `refreshed_at` tells the client the age of the data.
        refreshed_at = last_refresh(pool, lat, lon)
        return {
            "events": [_to_json(hit) for hit in page],
            "next_cursor": next_cursor,
            "refreshed_at": _format(refreshed_at) if refreshed_at else None,
        }

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        try:
            with pool.connection(timeout=2.0) as conn:
                conn.execute("SELECT 1")
        except Exception:
            return JSONResponse({"status": "database unavailable"}, status_code=503)
        return JSONResponse({"status": "ok"})

    return app


def _to_json(hit: EventHit) -> dict[str, Any]:
    body = asdict(hit.event)
    body["starts_at"] = _format(hit.event.starts_at)
    body["distance_km"] = round(hit.distance_km, 3)
    return body


def _format(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_utc(value: datetime) -> datetime:
    """A time with no zone is UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _encode_cursor(starts_at: datetime, event_id: str) -> str:
    raw = json.dumps([starts_at.astimezone(UTC).isoformat(), event_id])
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        starts_at, event_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(starts_at), str(event_id)
    except (ValueError, TypeError) as error:
        raise HTTPException(400, "invalid cursor") from error
