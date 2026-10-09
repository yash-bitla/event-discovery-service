"""Command line: `eds sim`, `eds ingest` and `eds serve`."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import uvicorn

from event_discovery.clock import SystemClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import load_regions
from event_discovery.ingest.resilience import CircuitBreaker, Retrier
from event_discovery.ingest.worker import (
    EventSink,
    IngestWorker,
    InMemorySink,
    StateStore,
    seconds_to_next_cycle,
)
from event_discovery.storage.db import migrate, open_pool
from event_discovery.storage.events import PostgresSink
from event_discovery.storage.regions import PostgresStateStore
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import generate_events


def main() -> None:
    parser = argparse.ArgumentParser(prog="eds")
    commands = parser.add_subparsers(dest="command", required=True)

    sim = commands.add_parser("sim", help="start the simulated upstream API")
    sim.add_argument("--host", default="127.0.0.1")
    sim.add_argument("--port", type=int, default=8081)
    sim.add_argument("--seed", type=int, default=7)
    sim.add_argument("--events", type=int, default=80_000)
    sim.add_argument("--quota", type=int, default=5000, help="calls for each day")
    sim.add_argument("--rate", type=int, default=5, help="calls for each second")
    sim.add_argument("--api-key", default="demo")

    ingest = commands.add_parser("ingest", help="run ingestion cycles")
    ingest.add_argument("--base-url", default="http://127.0.0.1:8081")
    ingest.add_argument("--api-key", default="demo")
    ingest.add_argument("--regions", type=Path, default=Path("config/regions.toml"))
    ingest.add_argument("--database-url", help="store the events here; default: in memory")
    ingest.add_argument("--schema", help="database schema of the tables; default: public")
    ingest.add_argument("--cycles", type=int, default=1, help="0: run until stopped")
    ingest.add_argument("--interval", type=float, default=3600, help="seconds between cycles")
    ingest.add_argument("--quota", type=int, default=5000)
    ingest.add_argument("--reserve", type=int, default=500)
    ingest.add_argument("--rate", type=float, default=4, help="largest number of calls a second")

    serve = commands.add_parser("serve", help="start the query API")
    serve.add_argument("--database-url", required=True)
    serve.add_argument("--schema", help="database schema of the tables; default: public")
    serve.add_argument("--redis-url", help="cache the responses here; default: no cache")
    serve.add_argument("--cache-ttl", type=int, default=60, help="seconds")
    serve.add_argument("--workers", type=int, default=1, help="API processes")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    # httpx logs each URL at INFO, and the URL contains the API key.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "sim":
        _run_sim(args)
    elif args.command == "ingest":
        _run_ingest(args)
    else:
        _run_serve(args)


def _run_sim(args: argparse.Namespace) -> None:
    base = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    app = create_app(
        generate_events(args.seed, base, args.events),
        SystemClock(),
        api_keys=(args.api_key,),
        daily_quota=args.quota,
        per_second=args.rate,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


def _run_serve(args: argparse.Namespace) -> None:
    pool = open_pool(args.database_url, schema=args.schema, max_size=1)
    migrate(pool, schema=args.schema)
    pool.close()
    # Each worker process makes its own app from this environment.
    os.environ["EDS_DATABASE_URL"] = args.database_url
    os.environ["EDS_SCHEMA"] = args.schema or ""
    os.environ["EDS_POOL_SIZE"] = str(max(4, 40 // args.workers))
    os.environ["EDS_CACHE_TTL"] = str(args.cache_ttl)
    if args.redis_url:
        os.environ["EDS_REDIS_URL"] = args.redis_url
    uvicorn.run(
        "event_discovery.api.asgi:create",
        factory=True,
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_level="warning",
    )


def _run_ingest(args: argparse.Namespace) -> None:
    clock = SystemClock()
    budget = QuotaBudget(clock, daily_limit=args.quota, reserve=args.reserve)
    pool = open_pool(args.database_url, schema=args.schema) if args.database_url else None
    sink: EventSink = InMemorySink()
    state_store: StateStore | None = None
    if pool is not None:
        migrate(pool, schema=args.schema)
        sink = PostgresSink(pool)
        state_store = PostgresStateStore(pool)
    breaker = CircuitBreaker(clock)
    try:
        with httpx.Client(base_url=args.base_url, timeout=10.0) as http:
            client = DiscoveryClient(
                http,
                args.api_key,
                RatePacer(clock, args.rate),
                budget,
                retrier=Retrier(clock),
                breaker=breaker,
            )
            worker = IngestWorker(
                client,
                sink,
                load_regions(args.regions),
                clock,
                budget=budget,
                state_store=state_store,
            )
            cycle = 0
            while args.cycles == 0 or cycle < args.cycles:
                started = clock.now()
                report = worker.run_cycle(args.interval)
                logging.info(
                    "cycle %d: %s, spendable calls left: %d", cycle, report, budget.spendable
                )
                cycle += 1
                if args.cycles == 0 or cycle < args.cycles:
                    wait = seconds_to_next_cycle(report, args.interval, breaker, clock.now())
                    if wait == args.interval:
                        wait = max(0.0, args.interval - (clock.now() - started))
                    clock.sleep(wait)
    finally:
        if pool is not None:
            pool.close()
