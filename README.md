# event-discovery-service

A backend service that answers one question: which events are near a location in a
date range? It gets its events from an API that has a daily quota and a rate limit.

The project is in progress. Milestones 1 to 3 of 5 are complete: ingestion that
stays in the quota, storage with a search API, and behavior during upstream
failures.

## Result of milestone 3: upstream failures

The simulated upstream can fail on command. The same day of hourly ingestion ran
with three clients. All three use the quota budget.

**One call in five fails (HTTP 503) for the full day:**

| Client | Calls | Failed calls | Region refreshes | Failed refreshes | Mean staleness (h) | Max staleness (h) |
|---|---|---|---|---|---|---|
| No retry | 3152 | 659 | 59 | 659 | 10.16 | 24.00 |
| Retry | 4482 | 947 | 205 | 5 | 2.07 | 5.99 |
| Retry + circuit breaker | 4482 | 947 | 205 | 5 | 2.07 | 5.99 |

- Without retries, ingestion almost stops. One failed call fails the refresh of
  its region, and a refresh needs 17 calls on average (4499 calls ÷ 270 refreshes
  in milestone 1). For a region that needs 17 calls, the chance that all are
  successful is 0.8^17 = 2.3 %.
- With a maximum of 4 attempts for each call, 205 refreshes are successful and 5
  fail. The retries go through the quota budget, so the day stays in the quota.
- The breaker did not open (0 times). That is correct: it opens after 5 failures
  in sequence, and it must not stop ingestion for errors that are not in sequence.

**A full outage that starts at 06:20 and continues for approximately 2 hours**
(mean of four runs, with the end at 08:05, 08:20, 08:35 and 08:50):

| Client | Failed calls | Recovery, mean (min) | Recovery, max (min) | Mean staleness (h) |
|---|---|---|---|---|
| No retry | 21 | 33.3 | 55.8 | 1.68 |
| Retry | 84 | 33.3 | 55.8 | 1.70 |
| Retry + circuit breaker | 24 | 3.3 | 3.3 | 1.62 |

- Recovery is the time from the end of the outage to the first refresh. Without
  the breaker, ingestion waits for the next hourly cycle. With the breaker, the
  next cycle starts at the next probe, and the breaker sends a probe at least
  each 5 minutes.
- Retries alone make an outage cost more quota: 84 failed calls. The breaker
  decreases that to 24, which includes the probes.
- The staleness for the day is almost the same for the three clients. No client
  can refresh during an outage.

During an outage the query API continues to answer, because it reads only the
database. Each response has a `refreshed_at` field, so a client can see the age
of the data. A test and the CI smoke test check this.

To get the same tables (approximately 4 minutes):

```
python benchmarks/fault_day.py
```

## Result of milestone 2: search in PostgreSQL

The search is "events less than N km from a point, with a start time in a range".
One GiST index on `(location, starts_at)` answers it. I measured that index and four
alternatives on 1,000,000 events with the same 500 searches:

| Indexes | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|
| No index | 178.35 | 209.20 | 229.44 |
| B-tree on `starts_at` | 74.35 | 106.41 | 115.58 |
| GiST on `location` | 26.04 | 73.90 | 82.02 |
| GiST on `location` + B-tree on `starts_at` | 5.91 | 22.66 | 38.89 |
| **GiST on `(location, starts_at)`, the index of the service** | **2.57** | **17.06** | **25.37** |

- The index of the service is 69 times faster than no index at P50
  (178.35 ÷ 2.57) and 12 times faster at P95 (209.20 ÷ 17.06).
- An index on only the location or only the time is not sufficient. Each search
  has the two conditions, and the combined index applies them in one index scan.

Conditions: the time is for one search from a Python client on one connection,
with the network time to a local container. Each search has a radius of 5, 10 or
25 km, a range of 1, 3 or 7 days, and a limit of 50 rows. PostgreSQL 17 with
PostGIS 3.6 and the default settings, in a Docker virtual machine with 2 CPUs and
2 GB of memory, on an Apple M3 Pro. The events have 40 venues for each of 30
cities, so many events have the same location. The benchmark does not measure
the cost of the index for writes.

To get the same table:

```
docker compose up -d db
python benchmarks/geo_query.py
```

## Result of milestone 1: ingestion that stays in the quota

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

## How ingestion handles failures

- **Retry.** After HTTP 5xx, a network error, or a rate-limit rejection, the client
  tries again: a maximum of 4 attempts, with exponential backoff and full jitter.
  It does not retry HTTP 4xx or a used quota. Each attempt uses the quota budget.
- **Circuit breaker.** After 5 failures in sequence, the breaker opens and the
  client sends no calls. After 30 seconds, one probe goes through. A successful
  probe closes the breaker. A failed probe opens it again for two times as long,
  to a maximum of 5 minutes. Each probe uses one call of the quota.
- **Saved state.** The worker saves the refresh time and the cost of each region
  in the database. After a restart it continues from that state.

The upstream returns no item after the first 1000 of a search (`size × page < 1000`).
If a region has more events than that, the worker divides the date range into two
halves and gets each half.

## Storage and the query API

The events are in one PostgreSQL table. The location is a PostGIS `geography`
point, so a distance is in meters on the WGS 84 spheroid. The worker writes a page
of events with one statement. That statement inserts new events, updates changed
events, and does not write a row that is the same.

```
GET /events?lat=40.7128&lon=-74.0060&radius_km=10&start=2026-01-10T00:00:00Z&end=2026-01-12T00:00:00Z
```

| Parameter | Default | Limit |
|---|---|---|
| `lat`, `lon` | necessary | |
| `radius_km` | 10 | 200 |
| `start` | now | |
| `end` | `start` + 30 days | |
| `segment` | all | |
| `limit` | 50 | 200 |
| `cursor` | first page | |

The response has the events in order of start time, the distance of each event in
km, `next_cursor`, and `refreshed_at` (the last refresh of a region that contains
the point). The cursor holds the start time and the id of the last
event, so a page does not change when new events arrive before it.
`GET /healthz` reports if the database answers.

## The simulated upstream

`event_discovery.upstream_sim` is a local copy of the event search of the
[Ticketmaster Discovery API](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/).
It copies the limits that Ticketmaster documents:

- a quota of 5000 calls a day and a rate limit of 5 calls a second
- the `Rate-Limit`, `Rate-Limit-Available`, `Rate-Limit-Over` and `Rate-Limit-Reset`
  response headers
- HTTP 429 with the documented fault body when the quota is used
- the deep paging limit

`PUT /_sim/faults` makes it fail on command: an error rate, an outage between two
times, or a delay before each response.

The events are synthetic, and a seed makes them the same on each run. No API key and
no network are necessary. Four points are my decisions, because the Ticketmaster
documents do not specify them: the quota resets at 00:00 UTC, the rate limit is a
sliding window of one second, a call that the rate limit rejects does not use the
quota, and a call that fails with HTTP 503 does use the quota.

## Quick start

Python 3.12 or later and Docker are necessary.

```
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
docker compose up -d db                # PostgreSQL with PostGIS, for the tests
.venv/bin/pytest
```

To start all the parts (the simulated upstream, ingestion, the database and the
API) and then search:

```
docker compose up -d --build --wait
curl 'http://127.0.0.1:8080/events?lat=40.7128&lon=-74.0060&radius_km=10'
```

The first ingestion cycle needs approximately 30 seconds to get all regions.

## Milestones

1. **Done.** Ingestion with a quota budget, and the simulated upstream.
2. **Done.** Storage in PostgreSQL, a geospatial index, and the query API.
3. **Done.** Retries, a circuit breaker, and fault-injection tests.
4. A cache and a load test: P95 latency, cache hit rate, and behavior during an
   upstream failure.
5. Final results and a write-up.

Known limits at this point: the service does not delete events that are in the
past. The breaker state is in the memory of the worker, so the API cannot report it.

## License

MIT
