"""Geohash and great-circle distance."""

from __future__ import annotations

import math

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_EARTH_RADIUS_KM = 6371.0088


def geohash_encode(lat: float, lon: float, precision: int = 9) -> str:
    lat_lo, lat_hi, lon_lo, lon_hi = -90.0, 90.0, -180.0, 180.0
    chars: list[str] = []
    bits = 0
    value = 0
    even = True  # even bits split longitude
    while len(chars) < precision:
        if even:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                value = (value << 1) | 1
                lon_lo = mid
            else:
                value <<= 1
                lon_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                value = (value << 1) | 1
                lat_lo = mid
            else:
                value <<= 1
                lat_hi = mid
        even = not even
        bits += 1
        if bits == 5:
            chars.append(_BASE32[value])
            bits = 0
            value = 0
    return "".join(chars)


def geohash_decode(geohash: str) -> tuple[float, float]:
    """Return the centre of the geohash cell as (lat, lon). Raise ValueError if invalid."""
    if not geohash:
        raise ValueError("empty geohash")
    lat_lo, lat_hi, lon_lo, lon_hi = -90.0, 90.0, -180.0, 180.0
    even = True
    for char in geohash.lower():
        index = _BASE32.find(char)
        if index < 0:
            raise ValueError(f"invalid geohash character: {char!r}")
        for shift in range(4, -1, -1):
            bit = (index >> shift) & 1
            if even:
                mid = (lon_lo + lon_hi) / 2
                if bit:
                    lon_lo = mid
                else:
                    lon_hi = mid
            else:
                mid = (lat_lo + lat_hi) / 2
                if bit:
                    lat_lo = mid
                else:
                    lat_hi = mid
            even = not even
    return (lat_lo + lat_hi) / 2, (lon_lo + lon_hi) / 2


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))
