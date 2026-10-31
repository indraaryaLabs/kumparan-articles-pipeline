"""Load extracted Parquet files into the raw layer.

Idempotent by construction: the rows of this batch_id are deleted and
re-inserted in ONE transaction. A retry or a manual re-run of the same
interval replaces the batch instead of appending a second copy.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow.parquet as pq

from pipeline.config import Settings
from pipeline.db import warehouse

ARTICLE_COPY = (
    "COPY raw.articles (_batch_id, _extracted_at, id, title, content, published_at, "
    "author_id, created_at, updated_at, deleted_at) FROM STDIN"
)
HARD_DELETE_COPY = (
    "COPY raw.article_hard_deletes (_batch_id, _extracted_at, article_id, created_at, deleted_at) FROM STDIN"
)


def _copy_parquet(conn, path: str, copy_sql: str, prefix: tuple, batch_size: int) -> int:
    loaded = 0
    with conn.cursor() as cur, cur.copy(copy_sql) as copy:
        for record_batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
            columns = [col.to_pylist() for col in record_batch.columns]
            for row in zip(*columns, strict=True):
                copy.write_row(prefix + row)
                loaded += 1
    return loaded


def load_batch(settings: Settings, extract: dict) -> dict:
    """Replace this batch in raw.articles / raw.article_hard_deletes. Returns row counts."""
    batch_id = extract["batch_id"]
    prefix = (batch_id, datetime.fromisoformat(extract["extracted_at"]))
    with warehouse(settings.warehouse_dsn) as conn:
        conn.execute("DELETE FROM raw.articles WHERE _batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM raw.article_hard_deletes WHERE _batch_id = %s", (batch_id,))
        articles = _copy_parquet(conn, extract["articles_path"], ARTICLE_COPY, prefix, settings.fetch_size)
        deletes = 0
        if extract.get("hard_deletes_path"):
            deletes = _copy_parquet(conn, extract["hard_deletes_path"], HARD_DELETE_COPY, prefix, settings.fetch_size)
        if articles != extract["articles_rows"] or deletes != extract["hard_deletes_rows"]:
            # The file on disk is not the file the extract step wrote. Raising
            # here rolls the whole batch back instead of committing bad data.
            raise RuntimeError(
                f"{batch_id}: loaded {articles}/{deletes} rows but extract reported "
                f"{extract['articles_rows']}/{extract['hard_deletes_rows']}"
            )
    return {"batch_id": batch_id, "raw_articles": articles, "raw_hard_deletes": deletes}
