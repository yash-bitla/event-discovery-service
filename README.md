# event-discovery-service

A backend service that answers one question: which events are near a location in a
date range? It gets its events from an API that has a daily quota and a rate limit.

The project is in progress. Milestone 1 of 5 is complete: ingestion that stays in
the quota.

## Result of milestone 1

The upstream permits 5000 calls a day. One refresh of all 30 regions uses
approximately 550 calls, so a refresh of all regions each hour needs 13,200 calls
a day. Ingestion must select what to refresh.

One simulated day, 80,000 events, 30 regions, 24 hourly cycles:

| Policy | Calls accepted | Calls rejected | Region refreshes | Mean staleness (h) | Max staleness (h) |
|---|---|---|---|---|---|
| Refresh all regions each hour, no budget | 5000 | 15 | 271 | 5.44 | 15.99 |
| Refresh all regions each 3 hours, no budget | 4351 | 0 | 240 | 1.52 | 2.99 |
| Quota budget (this project) | 4499 | 0 | 270 | 1.59 | 4.00 |

The same day with half the quota, 2500 calls:

| Policy | Calls accepted | Calls rejected | Region refreshes | Mean staleness (h) | Max staleness (h) |
|---|---|---|---|---|---|
| Refresh all regions each hour, no budget | 2500 | 20 | 128 | 8.84 | 20.98 |
| Refresh all regions each 3 hours, no budget | 2500 | 4 | 129 | 4.43 | 14.98 |
| Quota budget (this project) | 2245 | 0 | 131 | 3.26 | 8.00 |

What the tables show:

- The quota budget sends no call that the upstream rejects, at each quota. It also
  keeps the reserve: 4499 calls used of the 4500 that it can spend.
- The hourly refresh with no budget uses the full quota in the first 10 hours. The
  regions then get no refresh until the next day.
- A fixed interval of 3 hours is equal to the budget at 5000 calls (1.52 h and
  1.59 h). But a person must calculate that interval from the quota and the cost.
  With half the quota, the same interval fails, and the budget adjusts with no change.

Staleness is the time since the last refresh of a region. The mean uses the region
weight and a sample each 5 minutes. A region with no refresh yet counts from the
start of the day.

To get the same tables:

```
python benchmarks/quota_day.py
python benchmarks/quota_day.py --quota 2500 --reserve 250
```

The benchmark uses a manual clock, so each command runs in approximately one minute
and each run gives the same numbers.

## How ingestion stays in the quota

1. **Budget.** `QuotaBudget` counts the calls that remain until the reset. It keeps a
   reserve (500 calls by default) for work outside the schedule. After each response
   it takes the count from the `Rate-Limit-Available` header.
2. **Allowance.** Each cycle gets a share of the calls that remain, in proportion to
   its share of the time until the reset. At the start of the day:
   4500 calls × 3600 s ÷ 86,400 s = 187 calls for the first hour.
3. **Plan.** The planner puts the regions in order of `weight × staleness ÷ cost`,
   where cost is the number of calls that the last refresh of the region used. It
   takes regions in that order until the allowance is used.
4. **Limit.** The client asks the budget before each call. If the budget has no call
   to spend, the client does not send the request.
5. **Pace.** The client sends a maximum of 4 calls a second. The upstream limit is 5.

The upstream returns no item after the first 1000 of a search (`size × page < 1000`).
If a region has more events than that, the worker divides the date range into two
halves and gets each half.

## The simulated upstream

`event_discovery.upstream_sim` is a local copy of the event search of the
[Ticketmaster Discovery API](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/).
It copies the limits that Ticketmaster documents:

- a quota of 5000 calls a day and a rate limit of 5 calls a second
- the `Rate-Limit`, `Rate-Limit-Available`, `Rate-Limit-Over` and `Rate-Limit-Reset`
  response headers
- HTTP 429 with the documented fault body when the quota is used
- the deep paging limit

The events are synthetic, and a seed makes them the same on each run. No API key and
no network are necessary. Three points are my decisions, because the Ticketmaster
documents do not specify them: the quota resets at 00:00 UTC, the rate limit is a
sliding window of one second, and a call that the rate limit rejects does not use
the quota.

## Quick start

Python 3.12 or later is necessary.

```
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest

.venv/bin/eds sim --events 3000        # terminal 1: the simulated upstream on port 8081
.venv/bin/eds ingest                   # terminal 2: one ingestion cycle
```

## Milestones

1. **Done.** Ingestion with a quota budget, and the simulated upstream.
2. Storage, a geospatial index, and the query API.
3. Retries, a circuit breaker, and fault-injection tests.
4. A cache and a load test: P95 latency, cache hit rate, and behavior during an
   upstream failure.
5. Final results and a write-up.

In milestone 1 the events go to memory. Storage is the subject of milestone 2.

## License

MIT
