"""Measure the event search in PostgreSQL with and without indexes.

The script loads synthetic events into its own schema, runs the same queries for
each index configuration, and prints the latency. It needs the database:

    docker compose up -d db
    python benchmarks/geo_query.py
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from datetime import UTC, datetime, timedelta

from psycopg_pool import ConnectionPool

from event_discovery.storage.db import migrate, open_pool
from event_discovery.storage.events import SearchQuery, search_events
from event_discovery.upstream_sim.dataset import METROS, generate_events

BASE = datetime(2026, 1, 1, tzinfo=UTC)
SCHEMA = "eds_bench"
DEFAULT_URL = "postgresql://eds:eds@127.0.0.1:54329/eds"

# name -> statements that create the indexes of the configuration
CONFIGURATIONS: dict[str, list[str]] = {
    "No index": [],
    "B-tree on starts_at": ["CREATE INDEX bench_time ON events (starts_at)"],
    "GiST on location": ["CREATE INDEX bench_location ON events USING gist (location)"],
    "GiST on location + B-tree on starts_at": [
        "CREATE INDEX bench_location ON events USING gist (location)",
        "CREATE INDEX bench_time ON events (starts_at)",
    ],
    "GiST on (location, starts_at), the index of the service": [
        "CREATE INDEX bench_location ON events USING gist (location, starts_at)",
    ],
}


def load(pool: ConnectionPool, count: int, seed: int) -> None:
    events = generate_events(seed, BASE, count)
    with pool.connection() as conn, conn.cursor() as cursor:
        with cursor.copy(
            "COPY events (id, name, url, starts_at, segment, venue, city, location) FROM STDIN"
        ) as copy:
            for e in events:
                copy.write_row(
                    (
                        e.id,
                        e.name,
                        f"https://events.example/{e.id}",
                        e.starts_at,
                        e.segment,
                        e.venue,
                        e.city,
                        f"SRID=4326;POINT({e.lon} {e.lat})",
                    )
                )


def make_queries(count: int, seed: int) -> list[SearchQuery]:
    """Searches near a metro centre: radius 5, 10 or 25 km, range 1, 3 or 7 days."""
    rng = random.Random(seed)
    weights = [metro.weight for metro in METROS]
    queries = []
    for _ in range(count):
        metro = rng.choices(METROS, weights)[0]
        start = BASE + timedelta(hours=rng.randrange(80 * 24))
        queries.append(
            SearchQuery(
                lat=metro.lat + rng.gauss(0, 0.05),
                lon=metro.lon + rng.gauss(0, 0.05),
                radius_km=rng.choice((5, 10, 25)),
                start=start,
                end=start + timedelta(days=rng.choice((1, 3, 7))),
                limit=50,
            )
        )
    return queries


def measure(pool: ConnectionPool, queries: list[SearchQuery]) -> tuple[list[float], float]:
    """Return the latency of each query in ms and the mean number of rows."""
    for query in queries[:20]:
        search_events(pool, query)
    latencies = []
    rows = 0
    for query in queries:
        started = time.perf_counter()
        rows += len(search_events(pool, query))
        latencies.append((time.perf_counter() - started) * 1000)
    return latencies, rows / len(queries)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=DEFAULT_URL)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--queries", type=int, default=500)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    pool = open_pool(args.database_url, schema=SCHEMA, max_size=1)
    try:
        with pool.connection() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        migrate(pool, schema=SCHEMA)
        with pool.connection() as conn:
            conn.execute("DROP INDEX events_location_time_idx")
        load(pool, args.events, args.seed)
        queries = make_queries(args.queries, args.seed)

        print(f"events: {args.events}, queries: {args.queries}, limit: 50")
        print("| Indexes | P50 (ms) | P95 (ms) | P99 (ms) | Mean rows |")
        print("|---|---|---|---|---|")
        for name, statements in CONFIGURATIONS.items():
            with pool.connection() as conn:
                conn.execute("DROP INDEX IF EXISTS bench_location")
                conn.execute("DROP INDEX IF EXISTS bench_time")
                for statement in statements:
                    conn.execute(statement)
                conn.execute("ANALYZE events")
            latencies, mean_rows = measure(pool, queries)
            cuts = statistics.quantiles(latencies, n=100)
            print(
                f"| {name} | {statistics.median(latencies):.2f} | {cuts[94]:.2f} "
                f"| {cuts[98]:.2f} | {mean_rows:.1f} |"
            )
    finally:
        with pool.connection() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        pool.close()


if __name__ == "__main__":
    main()
