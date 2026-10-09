#!/bin/sh
# Start all services, then wait until the API returns an ingested event.
set -eu
docker compose up -d --build --wait
url='http://127.0.0.1:8080/events?lat=40.7128&lon=-74.0060&radius_km=50&limit=1'
for _ in $(seq 1 60); do
    if curl -fsS "$url" | grep -q '"id"'; then
        echo "ok: the API returned an ingested event"
        exit 0
    fi
    sleep 2
done
echo "failed: the API returned no event in 120 seconds" >&2
docker compose logs --tail 50 >&2
exit 1
