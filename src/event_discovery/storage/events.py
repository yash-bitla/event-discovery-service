"""Store events in PostgreSQL and search them by location and start time."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from event_discovery.models import Event

_UPSERT = """
INSERT INTO events (id, name, url, starts_at, segment, venue, city, location)
SELECT id, name, url, starts_at, segment, venue, city,
       ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
FROM unnest(
    %(id)s::text[], %(name)s::text[], %(url)s::text[], %(starts_at)s::timestamptz[],
    %(segment)s::text[], %(venue)s::text[], %(city)s::text[],
    %(lat)s::float8[], %(lon)s::float8[]
) AS t(id, name, url, starts_at, segment, venue, city, lat, lon)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    url = EXCLUDED.url,
    starts_at = EXCLUDED.starts_at,
    segment = EXCLUDED.segment,
    venue = EXCLUDED.venue,
    city = EXCLUDED.city,
    location = EXCLUDED.location,
    updated_at = now()
WHERE (events.name, events.url, events.starts_at, events.segment, events.venue, events.city)
      IS DISTINCT FROM
      (EXCLUDED.name, EXCLUDED.url, EXCLUDED.starts_at, EXCLUDED.segment, EXCLUDED.venue,
       EXCLUDED.city)
   OR NOT ST_Equals(events.location::geometry, EXCLUDED.location::geometry)
RETURNING (xmax = 0) AS inserted
"""

_SEARCH = """
SELECT id, name, url, starts_at, segment, venue, city,
       ST_Y(location::geometry) AS lat,
       ST_X(location::geometry) AS lon,
       ST_Distance(location, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography)
           / 1000 AS distance_km
FROM events
WHERE ST_DWithin(
          location, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography, %(radius_m)s
      )
  AND starts_at >= %(start)s
  AND starts_at < %(end)s
  AND (%(segment)s::text IS NULL OR segment = %(segment)s)
  AND (
      %(after_id)s::text IS NULL
      OR (starts_at, id) > (%(after_start)s::timestamptz, %(after_id)s::text)
  )
ORDER BY starts_at, id
LIMIT %(limit)s
"""


@dataclass(frozen=True)
class SearchQuery:
    lat: float
    lon: float
    radius_km: float
    start: datetime  # inclusive
    end: datetime  # exclusive
    segment: str | None = None
    limit: int = 50
    after: tuple[datetime, str] | None = None  # (starts_at, id) of the last event seen


@dataclass(frozen=True)
class EventHit:
    event: Event
    distance_km: float


class PostgresSink:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def upsert(self, events: Sequence[Event]) -> int:
        """Insert new events and update changed events. An unchanged row is not written."""
        unique = list({event.id: event for event in events}.values())
        if not unique:
            return 0
        params = {
            "id": [e.id for e in unique],
            "name": [e.name for e in unique],
            "url": [e.url for e in unique],
            "starts_at": [e.starts_at for e in unique],
            "segment": [e.segment for e in unique],
            "venue": [e.venue for e in unique],
            "city": [e.city for e in unique],
            "lat": [e.lat for e in unique],
            "lon": [e.lon for e in unique],
        }
        with self._pool.connection() as conn:
            rows = conn.execute(_UPSERT, params).fetchall()
        return sum(1 for (inserted,) in rows if inserted)


def search_events(pool: ConnectionPool, query: SearchQuery) -> list[EventHit]:
    """Return the events in the radius and the time range, in order of (starts_at, id)."""
    after_start, after_id = query.after if query.after else (None, None)
    params = {
        "lat": query.lat,
        "lon": query.lon,
        "radius_m": query.radius_km * 1000,
        "start": query.start,
        "end": query.end,
        "segment": query.segment,
        "after_start": after_start,
        "after_id": after_id,
        "limit": query.limit,
    }
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        rows = cursor.execute(_SEARCH, params).fetchall()
    return [
        EventHit(
            event=Event(
                id=row["id"],
                name=row["name"],
                url=row["url"],
                starts_at=row["starts_at"],
                segment=row["segment"],
                venue=row["venue"],
                city=row["city"],
                lat=row["lat"],
                lon=row["lon"],
            ),
            distance_km=row["distance_km"],
        )
        for row in rows
    ]
