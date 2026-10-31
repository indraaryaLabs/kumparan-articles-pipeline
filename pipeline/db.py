"""Database connection helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg


@contextmanager
def source_snapshot(dsn: str, statement_timeout_ms: int) -> Iterator[psycopg.Connection]:
    """Read-only, REPEATABLE READ transaction on the source.

    Every query inside sees the same snapshot, so the articles we read and the
    hard-delete tombstones we read are consistent with each other.
    """
    with psycopg.connect(dsn, autocommit=False) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        conn.execute(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        try:
            yield conn
        finally:
            conn.rollback()  # read-only: nothing to commit


@contextmanager
def warehouse(dsn: str) -> Iterator[psycopg.Connection]:
    """One warehouse transaction: committed on success, rolled back on any error."""
    with psycopg.connect(dsn, autocommit=False) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        yield conn
        conn.commit()


# Writers to core/mart take this lock so hourly, backfill and reconcile runs
# never interleave their merges, even though they are different DAGs.
CORE_WRITE_LOCK = 821_640_001


def lock_core(conn: psycopg.Connection) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (CORE_WRITE_LOCK,))
