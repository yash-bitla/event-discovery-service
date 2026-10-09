from __future__ import annotations

import pytest

from event_discovery.geo import geohash_decode, geohash_encode, haversine_km


def test_geohash_encode_matches_the_reference_value() -> None:
    assert geohash_encode(57.64911, 10.40744, precision=11) == "u4pruydqqvj"


def test_geohash_decode_returns_a_point_near_the_input() -> None:
    lat, lon = geohash_decode(geohash_encode(40.7128, -74.0060))
    assert lat == pytest.approx(40.7128, abs=1e-4)
    assert lon == pytest.approx(-74.0060, abs=1e-4)


def test_geohash_decode_rejects_an_invalid_character() -> None:
    with pytest.raises(ValueError):
        geohash_decode("abc")  # "a" is not in the geohash alphabet


def test_haversine_new_york_to_los_angeles() -> None:
    assert haversine_km(40.7128, -74.0060, 34.0522, -118.2437) == pytest.approx(3936, abs=5)
