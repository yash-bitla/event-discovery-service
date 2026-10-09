"""Connection pool and schema setup."""

from __future__ import annotations

from importlib import resources

from psycopg import sql
from psycopg_pool import ConnectionPool

_MIGRATE_LOCK = 4_815_162_342  # an arbitrary key for the advisory lock


def open_pool(
    database_url: str, *, schema: str | None = None, min_size: int = 1, max_size: int = 10
) -> ConnectionPool:
    """Open a pool. With `schema`, the tables are in that schema and not in `public`."""
    kwargs = {"options": f"-c search_path={schema},public"} if schema else {}
    pool = ConnectionPool(
        database_url, min_size=min_size, max_size=max_size, kwargs=kwargs, open=False
    )
    pool.open(wait=True, timeout=10.0)
    return pool


def migrate(pool: ConnectionPool, *, schema: str | None = None) -> None:
    """Create the tables and indexes if they are not there.

    The advisory lock makes it safe for two processes to start at the same time.
    """
    statements = resources.files("event_discovery.storage").joinpath("schema.sql").read_text()
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATE_LOCK,))
        if schema:
            conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.execute(statements)
