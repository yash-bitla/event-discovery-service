# PostgreSQL with PostGIS. The postgis/postgis image has no arm64 build, so this
# installs PostGIS from the PostgreSQL apt repository, which has amd64 and arm64.
FROM postgres:17-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-postgis-3 \
    && rm -rf /var/lib/apt/lists/*
