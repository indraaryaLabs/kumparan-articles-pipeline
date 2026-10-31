"""Extract articles from the source into Parquet files.

Three ways to choose rows, one writer:
  * incremental  rows whose updated_at OR deleted_at falls in [window_start - lookback, window_end)
  * month        rows created in one calendar month (historical backfill)
  * ids          specific ids (reconciliation found them missing in the warehouse)

Why Parquet and not CSV: `content` is free text with commas, quotes, newlines
and emoji. CSV escaping is where those rows get corrupted. Parquet is typed,
columnar and compressed, and it is what a production setup would write to
GCS/S3 before loading BigQuery.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import Settings
from pipeline.db import source_snapshot

TS = pa.timestamp("us", tz="UTC")

ARTICLE_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("title", pa.string()),
        ("content", pa.string()),
        ("published_at", TS),
        ("author_id", pa.int64()),
        ("created_at", TS),
        ("updated_at", TS),
        ("deleted_at", TS),
    ]
)
HARD_DELETE_SCHEMA = pa.schema([("article_id", pa.int64()), ("created_at", TS), ("deleted_at", TS)])

ARTICLE_COLUMNS = ", ".join(ARTICLE_SCHEMA.names)

# Half-open intervals everywhere: >= lower AND < upper. BETWEEN would put a row
# stamped exactly on the hour into two consecutive runs.
INCREMENTAL_SQL = f"""
    SELECT {ARTICLE_COLUMNS}
    FROM articles
    WHERE (updated_at >= %(lo)s AND updated_at < %(hi)s)
       -- The schema says updated_at changes on create/update; a soft delete only
       -- promises deleted_at. Without this branch a soft delete can be missed.
       OR (deleted_at >= %(lo)s AND deleted_at < %(hi)s)
"""
HARD_DELETES_SQL = """
    SELECT article_id, created_at, deleted_at
    FROM articles_deleted_log
    WHERE deleted_at >= %(lo)s AND deleted_at < %(hi)s
"""
MONTH_SQL = f"""
    SELECT {ARTICLE_COLUMNS}
    FROM articles
    WHERE created_at >= %(lo)s AND created_at < %(hi)s
"""
IDS_SQL = f"SELECT {ARTICLE_COLUMNS} FROM articles WHERE id = ANY(%(ids)s)"


@dataclass
class ExtractResult:
    batch_id: str
    articles_path: str
    articles_rows: int
    hard_deletes_path: str | None
    hard_deletes_rows: int
    extract_from: str | None
    extract_to: str | None
    extracted_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def batch_paths(settings: Settings, batch_id: str) -> tuple[Path, Path]:
    return (
        settings.data_dir / "articles" / f"{batch_id}.parquet",
        settings.data_dir / "hard_deletes" / f"{batch_id}.parquet",
    )


def _to_table(rows: Sequence[tuple], schema: pa.Schema) -> pa.Table:
    columns = list(zip(*rows, strict=True)) if rows else [[] for _ in schema]
    return pa.Table.from_arrays(
        [pa.array(col, type=field.type) for col, field in zip(columns, schema, strict=True)], schema=schema
    )


def _query_to_parquet(conn, sql: str, params: dict, schema: pa.Schema, path: Path, fetch_size: int) -> int:
    """Stream a query into a Parquet file without holding the result in memory.

    A named (server-side) cursor makes Postgres send rows in chunks of
    `fetch_size`; each chunk becomes a Parquet row group. The file is written
    under a temporary name and renamed at the end, so a crash never leaves a
    half-written file that a retry could mistake for a finished one.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    rows_written = 0
    with conn.cursor(name=f"extract_{path.stem}"[:63]) as cur:
        cur.itersize = fetch_size
        cur.execute(sql, params)
        with pq.ParquetWriter(tmp, schema, compression="zstd") as writer:
            while True:
                chunk = cur.fetchmany(fetch_size)
                if not chunk:
                    break
                writer.write_table(_to_table(chunk, schema))
                rows_written += len(chunk)
            if rows_written == 0:
                writer.write_table(_to_table([], schema))  # valid empty file with the schema
    os.replace(tmp, path)
    return rows_written


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def extract_incremental(
    settings: Settings, batch_id: str, window_start: datetime, window_end: datetime
) -> ExtractResult:
    if window_end <= window_start:
        # Airflow 3 interprets a cron string as CronTriggerTimetable by default,
        # which gives data_interval_start == data_interval_end. Refuse instead of
        # silently extracting nothing every hour.
        raise ValueError(f"Empty or inverted window: {window_start} .. {window_end}")
    lo = window_start - settings.lookback
    params = {"lo": lo, "hi": window_end}
    articles_path, deletes_path = batch_paths(settings, batch_id)
    with source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as conn:
        extracted_at = _now_iso()
        n_articles = _query_to_parquet(
            conn, INCREMENTAL_SQL, params, ARTICLE_SCHEMA, articles_path, settings.fetch_size
        )
        n_deletes = _query_to_parquet(
            conn, HARD_DELETES_SQL, params, HARD_DELETE_SCHEMA, deletes_path, settings.fetch_size
        )
    return ExtractResult(
        batch_id,
        str(articles_path),
        n_articles,
        str(deletes_path),
        n_deletes,
        lo.isoformat(),
        window_end.isoformat(),
        extracted_at,
    )


def extract_month(settings: Settings, batch_id: str, month_start: datetime, month_end: datetime) -> ExtractResult:
    articles_path, _ = batch_paths(settings, batch_id)
    params = {"lo": month_start, "hi": month_end}
    with source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as conn:
        extracted_at = _now_iso()
        n = _query_to_parquet(conn, MONTH_SQL, params, ARTICLE_SCHEMA, articles_path, settings.fetch_size)
    return ExtractResult(
        batch_id, str(articles_path), n, None, 0, month_start.isoformat(), month_end.isoformat(), extracted_at
    )


def extract_ids(settings: Settings, batch_id: str, ids: list[int]) -> ExtractResult:
    articles_path, _ = batch_paths(settings, batch_id)
    with source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as conn:
        extracted_at = _now_iso()
        n = _query_to_parquet(conn, IDS_SQL, {"ids": ids}, ARTICLE_SCHEMA, articles_path, settings.fetch_size)
    return ExtractResult(batch_id, str(articles_path), n, None, 0, None, None, extracted_at)
