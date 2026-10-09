from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Event:
    id: str
    name: str
    url: str
    starts_at: datetime
    segment: str | None
    venue: str
    city: str
    lat: float
    lon: float
