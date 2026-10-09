"""Synthetic events for the simulated upstream. The same seed gives the same events."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class Metro:
    name: str
    lat: float
    lon: float
    weight: float  # relative share of events, roughly the metro population in millions


METROS: tuple[Metro, ...] = (
    Metro("New York", 40.7128, -74.0060, 19.5),
    Metro("Los Angeles", 34.0522, -118.2437, 12.9),
    Metro("Chicago", 41.8781, -87.6298, 9.3),
    Metro("Dallas", 32.7767, -96.7970, 8.1),
    Metro("Houston", 29.7604, -95.3698, 7.5),
    Metro("Washington", 38.9072, -77.0369, 6.3),
    Metro("Atlanta", 33.7490, -84.3880, 6.3),
    Metro("Philadelphia", 39.9526, -75.1652, 6.2),
    Metro("Miami", 25.7617, -80.1918, 6.2),
    Metro("Phoenix", 33.4484, -112.0740, 5.1),
    Metro("Boston", 42.3601, -71.0589, 4.9),
    Metro("San Francisco", 37.7749, -122.4194, 4.6),
    Metro("Detroit", 42.3314, -83.0458, 4.3),
    Metro("Seattle", 47.6062, -122.3321, 4.0),
    Metro("Minneapolis", 44.9778, -93.2650, 3.7),
    Metro("San Diego", 32.7157, -117.1611, 3.3),
    Metro("Tampa", 27.9506, -82.4572, 3.3),
    Metro("Denver", 39.7392, -104.9903, 3.0),
    Metro("St. Louis", 38.6270, -90.1994, 2.8),
    Metro("Charlotte", 35.2271, -80.8431, 2.8),
    Metro("Orlando", 28.5383, -81.3792, 2.8),
    Metro("San Antonio", 29.4241, -98.4936, 2.7),
    Metro("Portland", 45.5152, -122.6784, 2.5),
    Metro("Pittsburgh", 40.4406, -79.9959, 2.4),
    Metro("Austin", 30.2672, -97.7431, 2.4),
    Metro("Las Vegas", 36.1699, -115.1398, 2.3),
    Metro("Kansas City", 39.0997, -94.5786, 2.2),
    Metro("Nashville", 36.1627, -86.7816, 2.1),
    Metro("Salt Lake City", 40.7608, -111.8910, 1.3),
    Metro("New Orleans", 29.9511, -90.0715, 1.2),
)

SEGMENTS = ("Music", "Sports", "Arts & Theatre", "Film", "Miscellaneous")
_VENUE_SPREAD_KM = 12.0
_VENUES_PER_METRO = 40


@dataclass(frozen=True)
class SimEvent:
    id: str
    name: str
    starts_at: datetime
    segment: str
    venue: str
    city: str
    lat: float
    lon: float

    def to_api(self) -> dict[str, Any]:
        """The subset of the Discovery API event object that this project reads."""
        return {
            "id": self.id,
            "name": self.name,
            "type": "event",
            "url": f"https://events.example/{self.id}",
            "dates": {"start": {"dateTime": format_datetime(self.starts_at)}},
            "classifications": [{"primary": True, "segment": {"name": self.segment}}],
            "_embedded": {
                "venues": [
                    {
                        "name": self.venue,
                        "city": {"name": self.city},
                        "location": {"latitude": f"{self.lat:.6f}", "longitude": f"{self.lon:.6f}"},
                    }
                ]
            },
        }


def format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_events(
    seed: int, base: datetime, count: int, horizon_days: int = 90
) -> list[SimEvent]:
    """Generate `count` events that start in the `horizon_days` after `base`."""
    rng = random.Random(seed)
    venues: dict[str, list[tuple[str, float, float]]] = {}
    for metro in METROS:
        km_per_deg_lon = 111.32 * math.cos(math.radians(metro.lat))
        venues[metro.name] = [
            (
                f"{metro.name} Venue {n + 1}",
                metro.lat + rng.gauss(0, _VENUE_SPREAD_KM) / 110.57,
                metro.lon + rng.gauss(0, _VENUE_SPREAD_KM) / km_per_deg_lon,
            )
            for n in range(_VENUES_PER_METRO)
        ]
    weights = [metro.weight for metro in METROS]
    horizon_minutes = horizon_days * 24 * 60
    events: list[SimEvent] = []
    for n in range(count):
        metro = rng.choices(METROS, weights)[0]
        venue, lat, lon = rng.choice(venues[metro.name])
        segment = rng.choice(SEGMENTS)
        events.append(
            SimEvent(
                id=f"SIM{n:07d}",
                name=f"{segment} event {n}",
                starts_at=base + timedelta(minutes=rng.randrange(horizon_minutes)),
                segment=segment,
                venue=venue,
                city=metro.name,
                lat=lat,
                lon=lon,
            )
        )
    return events
