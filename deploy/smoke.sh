#!/bin/sh
# Start all services and wait until the API returns events of a refreshed region.
# Then check that the API answers during an upstream outage.
set -eu
docker compose up -d --build --wait
url='http://127.0.0.1:8080/events?lat=40.7128&lon=-74.0060&radius_km=50&limit=1'
faults='http://127.0.0.1:8081/_sim/faults'

refreshed() {
    curl -fsS "$url" | grep '"id"' | grep -q '"refreshed_at":"2'
}

for _ in $(seq 1 60); do
    if refreshed; then
        echo "ok: the API returned an ingested event"
        # Make each upstream call fail. The API must continue to answer.
        curl -fsS -X PUT -H 'Content-Type: application/json' \
            -d '{"outages": [[0, 9999999999]]}' "$faults" >/dev/null
        # A different limit, so that this answer does not come from the cache.
        curl -fsS "${url}0" | grep '"id"' | grep -q '"refreshed_at":"2'
        echo "ok: the API answers while the upstream is not available"
        curl -fsS -X PUT -H 'Content-Type: application/json' -d '{}' "$faults" >/dev/null
        exit 0
    fi
    sleep 2
done
echo "failed: the API returned no event of a refreshed region in 120 seconds" >&2
docker compose logs --tail 50 >&2
exit 1
