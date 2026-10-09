"""Load test of the query API, with and without the cache, and during an outage.

The script loads synthetic events into its own schema, starts the API as a
process on this machine, and sends requests at a fixed rate. It needs the
database and Redis:

    docker compose up -d db cache
    python benchmarks/load_test.py

The load is open-loop: request `i` has the scheduled time `i / rate`, and the
latency is the time from the scheduled time to the end of the response. A slow
response thus does not delay the requests after it. If 800 requests are in
progress at the scheduled time of a request, the generator does not send that
request and counts it as an error. A response that takes more than 5 seconds is
also an error. Four generator processes send the requests. The last column of
each table is the P99 of the time by which the generator started a request late.
If that time is not small, the row measures the generator and not the API.

Workload: 90 % of the requests come from 2000 popular searches with a Zipf
distribution (search number `k` has the weight `1 / k`). The other 10 % are
searches that occur one time. Each search is near one of 30 cities, has a radius
of 5, 10 or 25 km, starts at 00:00 of one of the next 14 days, and has a range
of 1, 3 or 7 days.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import subprocess
import sys
import time
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import accumulate

import httpx
import redis

from event_discovery.ingest.planner import Region, RegionState
from event_discovery.storage.db import migrate, open_pool
from event_discovery.storage.regions import PostgresStateStore
from event_discovery.upstream_sim.dataset import METROS, generate_events

SCHEMA = "eds_load"
POPULAR = 2000
UNIQUE_SHARE = 0.1
MAX_IN_PROGRESS = 800
GENERATORS = 4
RUN = [sys.executable, "-c", "from event_discovery.cli import main; main()"]


@dataclass(frozen=True)
class Sample:
    scheduled_s: float
    latency_ms: float
    generator_delay_ms: float  # how late the generator started the request
    ok: bool
    cache: str | None


def seed(database_url: str, count: int, base: datetime) -> None:
    pool = open_pool(database_url, schema=SCHEMA, max_size=1)
    try:
        with pool.connection() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        migrate(pool, schema=SCHEMA)
        with pool.connection() as conn, conn.cursor() as cursor:
            with cursor.copy(
                "COPY events (id, name, url, starts_at, segment, venue, city, location) FROM STDIN"
            ) as copy:
                for e in generate_events(7, base, count):
                    copy.write_row(
                        (
                            f"SEED{e.id[3:]}",
                            e.name,
                            f"https://events.example/{e.id}",
                            e.starts_at,
                            e.segment,
                            e.venue,
                            e.city,
                            f"SRID=4326;POINT({e.lon} {e.lat})",
                        )
                    )
        with pool.connection() as conn:
            conn.execute("ANALYZE events")
        store = PostgresStateStore(pool)
        for m in METROS:
            store.save(Region(m.name, m.lat, m.lon, 50, m.weight), RegionState(time.time(), 5))
    finally:
        pool.close()


def make_requests(count: int, base: datetime, seed_value: int) -> list[dict[str, str]]:
    rng = random.Random(seed_value)
    metro_weights = [m.weight for m in METROS]

    def search() -> dict[str, str]:
        metro = rng.choices(METROS, metro_weights)[0]
        start = base + timedelta(days=rng.randrange(14))
        end = start + timedelta(days=rng.choice((1, 3, 7)))
        return {
            "lat": f"{metro.lat + rng.gauss(0, 0.05):.4f}",
            "lon": f"{metro.lon + rng.gauss(0, 0.05):.4f}",
            "radius_km": str(rng.choice((5, 10, 25))),
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    popular = [search() for _ in range(POPULAR)]
    cumulative = list(accumulate(1 / rank for rank in range(1, POPULAR + 1)))
    return [
        search() if rng.random() < UNIQUE_SHARE else rng.choices(popular, cum_weights=cumulative)[0]
        for _ in range(count)
    ]


async def _send_slice(
    base_url: str, requests: list[tuple[int, dict[str, str]]], rate: float, started: float
) -> list[Sample]:
    samples: list[Sample] = []
    in_progress = 0
    limit = MAX_IN_PROGRESS // GENERATORS
    # No connection limit: httpx is slow when many requests wait for a connection,
    # and that would add time that is not from the API. `limit` is the limit.
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=limit)
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0, limits=limits) as client:

        async def one(index: int, params: dict[str, str]) -> None:
            nonlocal in_progress
            scheduled = started + index / rate
            await asyncio.sleep(max(0.0, scheduled - time.time()))
            delay_ms = (time.time() - scheduled) * 1000
            ok, cache = False, None
            if in_progress < limit:
                in_progress += 1
                try:
                    response = await client.get("/events", params=params)
                    ok = response.status_code == 200
                    cache = response.headers.get("X-Cache")
                except httpx.HTTPError:
                    pass
                finally:
                    in_progress -= 1
            latency_ms = (time.time() - scheduled) * 1000
            samples.append(Sample(index / rate, latency_ms, delay_ms, ok, cache))

        await asyncio.gather(*(one(index, params) for index, params in requests))
    return samples


def _generator(
    base_url: str, requests: list[tuple[int, dict[str, str]]], rate: float, started: float
) -> list[Sample]:
    return asyncio.run(_send_slice(base_url, requests, rate, started))


def send(base_url: str, requests: list[dict[str, str]], rate: float) -> list[Sample]:
    """Send request `i` at `i / rate` seconds. GENERATORS processes share the requests,

    because one Python process cannot send more than a few hundred requests a second.
    """
    indexed = list(enumerate(requests))
    started = time.time() + 2.0
    with ProcessPoolExecutor(GENERATORS) as pool:
        futures = [
            pool.submit(_generator, base_url, indexed[n::GENERATORS], rate, started)
            for n in range(GENERATORS)
        ]
        return [sample for future in futures for sample in future.result()]


@contextmanager
def process(*args: str) -> Iterator[None]:
    proc = subprocess.Popen([*RUN, *args], stderr=subprocess.DEVNULL)
    try:
        yield
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def wait_for(url: str) -> None:
    for _ in range(100):
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"{url} did not answer")


def row(label: str, samples: list[Sample], seconds: float) -> str:
    """One table row for the samples of a time range of `seconds` seconds."""
    good = [s.latency_ms for s in samples if s.ok]
    errors = len(samples) - len(good)
    if len(good) < 100:
        return f"| {label} | {len(good) / seconds:.0f} | - | - | - | {errors} | - | - |"
    cuts = statistics.quantiles(good, n=100)
    hits = sum(1 for s in samples if s.cache == "hit")
    cached = sum(1 for s in samples if s.cache in ("hit", "miss"))
    hit_rate = f"{100 * hits / cached:.1f} %" if cached else "-"
    delay = statistics.quantiles([s.generator_delay_ms for s in samples], n=100)[98]
    return (
        f"| {label} | {len(good) / seconds:.0f} | {statistics.median(good):.1f} "
        f"| {cuts[94]:.1f} | {cuts[98]:.1f} | {errors} | {hit_rate} | {delay:.1f} |"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default="postgresql://eds:eds@127.0.0.1:54329/eds")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:63799/1")
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--rates", default="100,200,400", help="requests a second")
    parser.add_argument("--warmup", type=int, default=60, help="seconds")
    parser.add_argument("--duration", type=int, default=60, help="seconds")
    parser.add_argument("--outage-rate", type=int, default=200)
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--workers", type=int, default=4, help="API processes")
    parser.add_argument("--skip-seed", action="store_true", help="use the events of the last run")
    args = parser.parse_args()

    base = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    if not args.skip_seed:
        seed(args.database_url, args.events, base)
    cache = redis.Redis.from_url(args.redis_url)
    api_url = f"http://127.0.0.1:{args.port}"
    serve = ["serve", "--database-url", args.database_url, "--schema", SCHEMA]
    serve += ["--port", str(args.port), "--workers", str(args.workers)]
    header = "| Successful/s | P50 (ms) | P95 (ms) | P99 (ms) | Errors | Cache hit rate "
    header += "| Generator delay P99 (ms) |"

    print(
        f"events: {args.events}, API processes: {args.workers}, "
        f"warm-up: {args.warmup} s, measured: {args.duration} s"
    )
    for name, extra in (("no cache", []), ("cache", ["--redis-url", args.redis_url])):
        print(f"\n{name}\n| Target rate {header}\n|---|---|---|---|---|---|---|---|")
        for rate in (int(r) for r in args.rates.split(",")):
            cache.flushdb()
            with process(*serve, *extra):
                wait_for(f"{api_url}/healthz")
                requests = make_requests(rate * (args.warmup + args.duration), base, 7)
                samples = send(api_url, requests, rate)
            measured = [s for s in samples if s.scheduled_s >= args.warmup]
            print(row(str(rate), measured, args.duration), flush=True)

    # Outage: ingestion runs against the simulated upstream, which fails each call
    # in the middle third of the run. The quota is large here, so that ingestion
    # sends calls for the full run and the outage has calls to fail.
    rate, third = args.outage_rate, 30
    sim_url = f"http://127.0.0.1:{args.port + 1}"
    ingest = ["ingest", "--base-url", sim_url, "--database-url", args.database_url]
    ingest += ["--schema", SCHEMA, "--cycles", "0", "--interval", "10"]
    ingest += ["--quota", "500000", "--reserve", "0"]
    cache.flushdb()
    with process("sim", "--port", str(args.port + 1), "--events", "20000", "--quota", "500000"):
        wait_for(f"{sim_url}/_sim/stats")
        with process(*serve, "--redis-url", args.redis_url), process(*ingest):
            wait_for(f"{api_url}/healthz")
            outage_start = time.time() + 2.0 + third
            httpx.put(
                f"{sim_url}/_sim/faults", json={"outages": [[outage_start, outage_start + third]]}
            )
            samples = send(api_url, make_requests(rate * 3 * third, base, 8), rate)
            stats = httpx.get(f"{sim_url}/_sim/stats").json()
    print(f"\ncache, {rate} requests/s, upstream outage in the middle 30 s")
    print(f"| Phase {header}\n|---|---|---|---|---|---|---|---|")
    for index, phase in enumerate(("before the outage", "during the outage", "after the outage")):
        in_phase = [s for s in samples if index * third <= s.scheduled_s < (index + 1) * third]
        print(row(phase, in_phase, third))
    print(
        f"upstream calls of ingestion in the run: {stats['accepted']} accepted, "
        f"{stats['faults_injected']} failed in the outage"
    )
    cache.close()


if __name__ == "__main__":
    main()
