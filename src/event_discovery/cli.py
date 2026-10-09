"""Command line: `eds sim` starts the simulated upstream, `eds ingest` runs ingestion."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

import httpx
import uvicorn

from event_discovery.clock import SystemClock
from event_discovery.ingest.budget import QuotaBudget, RatePacer
from event_discovery.ingest.client import DiscoveryClient
from event_discovery.ingest.planner import load_regions
from event_discovery.ingest.worker import IngestWorker, InMemorySink
from event_discovery.upstream_sim.app import create_app
from event_discovery.upstream_sim.dataset import generate_events


def main() -> None:
    parser = argparse.ArgumentParser(prog="eds")
    commands = parser.add_subparsers(dest="command", required=True)

    sim = commands.add_parser("sim", help="start the simulated upstream API")
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
    ingest.add_argument("--cycles", type=int, default=1)
    ingest.add_argument("--interval", type=float, default=3600, help="seconds between cycles")
    ingest.add_argument("--quota", type=int, default=5000)
    ingest.add_argument("--reserve", type=int, default=500)
    ingest.add_argument("--rate", type=float, default=4, help="largest number of calls a second")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    # httpx logs each URL at INFO, and the URL contains the API key.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "sim":
        _run_sim(args)
    else:
        _run_ingest(args)


def _run_sim(args: argparse.Namespace) -> None:
    base = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    app = create_app(
        generate_events(args.seed, base, args.events),
        SystemClock(),
        api_keys=(args.api_key,),
        daily_quota=args.quota,
        per_second=args.rate,
    )
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


def _run_ingest(args: argparse.Namespace) -> None:
    clock = SystemClock()
    budget = QuotaBudget(clock, daily_limit=args.quota, reserve=args.reserve)
    sink = InMemorySink()
    with httpx.Client(base_url=args.base_url, timeout=10.0) as http:
        client = DiscoveryClient(http, args.api_key, RatePacer(clock, args.rate), budget)
        worker = IngestWorker(client, sink, load_regions(args.regions), clock, budget=budget)
        for cycle in range(args.cycles):
            started = clock.now()
            report = worker.run_cycle(args.interval)
            logging.info("cycle %d: %s, spendable calls left: %d", cycle, report, budget.spendable)
            if cycle + 1 < args.cycles:
                clock.sleep(max(0.0, args.interval - (clock.now() - started)))
    logging.info("events stored: %d", len(sink.events))
