"""Refresh state of the regions in PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime

from psycopg_pool import ConnectionPool

from event_discovery.ingest.planner import Region, RegionState

_SAVE = """
INSERT INTO region_state (name, center, radius_m, last_refreshed_at, expected_cost)
VALUES (
    %(name)s, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography, %(radius_m)s,
    %(last_refreshed_at)s, %(expected_cost)s
)
ON CONFLICT (name) DO UPDATE SET
    center = EXCLUDED.center,
    radius_m = EXCLUDED.radius_m,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    expected_cost = EXCLUDED.expected_cost
"""

_LAST_REFRESH = """
SELECT max(last_refreshed_at)
FROM region_state
WHERE ST_DWithin(center, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography, radius_m)
"""


class PostgresStateStore:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def load(self) -> dict[str, RegionState]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT name, last_refreshed_at, expected_cost FROM region_state"
            ).fetchall()
        return {
            name: RegionState(
                last_refreshed_at=refreshed.timestamp() if refreshed else None,
                expected_cost=cost,
            )
            for name, refreshed, cost in rows
        }

    def save(self, region: Region, state: RegionState) -> None:
        refreshed = state.last_refreshed_at
        params = {
            "name": region.name,
            "lat": region.lat,
            "lon": region.lon,
            "radius_m": region.radius_km * 1000,
            "last_refreshed_at": datetime.fromtimestamp(refreshed, UTC) if refreshed else None,
            "expected_cost": state.expected_cost,
        }
        with self._pool.connection() as conn:
            conn.execute(_SAVE, params)


def last_refresh(pool: ConnectionPool, lat: float, lon: float) -> datetime | None:
    """Time of the last refresh of a region that contains the point.

    None if no region contains the point, or if none of them has a refresh yet.
    """
    with pool.connection() as conn:
        row = conn.execute(_LAST_REFRESH, {"lat": lat, "lon": lon}).fetchone()
    return row[0] if row else None
