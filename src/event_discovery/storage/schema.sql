CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
-- btree_gist lets one GiST index hold the location and the start time.
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

CREATE TABLE IF NOT EXISTS events (
    id         text PRIMARY KEY,
    name       text NOT NULL,
    url        text NOT NULL,
    starts_at  timestamptz NOT NULL,
    segment    text,
    venue      text NOT NULL,
    city       text NOT NULL,
    location   geography(Point, 4326) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- benchmarks/geo_query.py compares this index with the alternatives.
CREATE INDEX IF NOT EXISTS events_location_time_idx
    ON events USING gist (location, starts_at);

-- One row for each region that ingestion refreshes. The API reads it to report
-- the age of its data.
CREATE TABLE IF NOT EXISTS region_state (
    name              text PRIMARY KEY,
    center            geography(Point, 4326) NOT NULL,
    radius_m          double precision NOT NULL,
    last_refreshed_at timestamptz,
    expected_cost     integer NOT NULL
);
