"""Test fixtures.

Tests run against real PostgreSQL (the `kumparan_test` and `warehouse_test`
databases created by the compose stack), not mocks: most of what can go wrong
in this pipeline is SQL behaviour (snapshots, conflicts, partitions, time zones).
Every test starts from empty schemas.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from pipeline.config import Settings

ROOT = Path(__file__).resolve().parents[1]

SOURCE_ADMIN_DSN = os.environ.get(
    "TEST_SOURCE_ADMIN_DSN", "postgresql://postgres:postgres@localhost:5433/kumparan_test"
)
SOURCE_DSN = os.environ.get("TEST_SOURCE_DSN", "postgresql://etl_reader:etl_reader@localhost:5433/kumparan_test")
WAREHOUSE_DSN = os.environ.get("TEST_WAREHOUSE_DSN", "postgresql://postgres:postgres@localhost:5434/warehouse_test")


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


@pytest.fixture
def source():
    with psycopg.connect(SOURCE_ADMIN_DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS articles, articles_deleted_log CASCADE")
        conn.execute((ROOT / "source_db" / "init.sql").read_text())
        yield conn


@pytest.fixture
def warehouse_conn():
    with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw, core, mart, meta CASCADE")
        conn.execute((ROOT / "warehouse_db" / "init.sql").read_text())
        conn.execute("SET TIME ZONE 'UTC'")
        yield conn


@pytest.fixture
def settings(tmp_path, source, warehouse_conn) -> Settings:
    return Settings(
        source_dsn=SOURCE_DSN,
        warehouse_dsn=WAREHOUSE_DSN,
        data_dir=tmp_path,
        lookback=timedelta(minutes=15),
        fetch_size=3,  # tiny on purpose: forces several fetch rounds / row groups
    )


def insert_article(
    conn,
    *,
    created_at: datetime,
    updated_at: datetime | None = None,
    published_at: datetime | None = None,
    title: str = "t",
    content: str = "c",
    author_id: int = 1,
    deleted_at: datetime | None = None,
) -> int:
    return conn.execute(
        "INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at, deleted_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
        (title, content, published_at, author_id, created_at, updated_at or created_at, deleted_at),
    ).fetchone()[0]


def core_row(conn, article_id: int) -> dict | None:
    cur = conn.execute("SELECT * FROM core.articles WHERE id = %s", (article_id,))
    row = cur.fetchone()
    return None if row is None else dict(zip([d.name for d in cur.description], row, strict=True))


def core_checksum(conn) -> str:
    return conn.execute(
        "SELECT md5(coalesce(string_agg(md5(c::text), '' ORDER BY id), '')) FROM core.articles c"
    ).fetchone()[0]


def mart_rows(conn) -> set[tuple]:
    return set(
        conn.execute(
            "SELECT published_date_wib, author_id, articles_published FROM mart.daily_articles_by_author"
        ).fetchall()
    )


def mart_truth(conn) -> set[tuple]:
    """The mart recomputed from scratch; the incremental mart must always equal this."""
    return set(
        conn.execute(
            "SELECT (published_at AT TIME ZONE 'Asia/Jakarta')::date, author_id, count(*)::int "
            "FROM core.articles WHERE NOT is_deleted AND published_at IS NOT NULL GROUP BY 1, 2"
        ).fetchall()
    )
