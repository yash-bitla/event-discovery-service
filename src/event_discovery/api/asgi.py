"""App factory for uvicorn worker processes. `eds serve` sets the environment."""

from __future__ import annotations

import os

from fastapi import FastAPI

from event_discovery.api.app import create_api
from event_discovery.api.cache import RedisCache
from event_discovery.storage.db import open_pool


def create() -> FastAPI:
    pool = open_pool(
        os.environ["EDS_DATABASE_URL"],
        schema=os.environ.get("EDS_SCHEMA") or None,
        max_size=int(os.environ.get("EDS_POOL_SIZE", "20")),
    )
    redis_url = os.environ.get("EDS_REDIS_URL")
    ttl_s = int(os.environ.get("EDS_CACHE_TTL", "60"))
    cache = RedisCache.from_url(redis_url, ttl_s=ttl_s) if redis_url else None
    return create_api(pool, cache=cache)
