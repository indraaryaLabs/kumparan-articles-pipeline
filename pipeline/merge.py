"""Merge one raw batch into core.articles.

Steps, all in one warehouse transaction (all or nothing):
  1. take the core write lock (hourly, backfill and reconcile never interleave)
  2. dedupe the batch: one row per id, newest version wins
  3. record which mart days this batch can change (OLD and NEW published dates)
  4. upsert into core.articles, but only when the incoming version is newer
  5. apply hard-delete tombstones (this batch's, and older ones for ids in this batch)

The version guard in step 4 is what makes re-runs and out-of-order runs safe:
re-running last Tuesday's batch today cannot overwrite today's newer title,
because last Tuesday's row carries an older updated_at.
"""

from __future__ import annotations

from pipeline.config import Settings
from pipeline.db import lock_core, warehouse

WIB = "Asia/Jakarta"

DEDUPE_SQL = """
    CREATE TEMP TABLE batch_rows ON COMMIT DROP AS
    SELECT DISTINCT ON (id)
           id, title, content, published_at, author_id, created_at, updated_at, deleted_at
    FROM raw.articles
    WHERE _batch_id = %(batch_id)s
    -- newest version first; with equal updated_at a row that carries a deleted_at is newer
    ORDER BY id, updated_at DESC, deleted_at DESC NULLS LAST, _extracted_at DESC
"""

# Old dates (before this merge) and new dates both need a mart refresh:
# an article rescheduled from Monday to Tuesday changes both days.
AFFECTED_DATES_SQL = f"""
    INSERT INTO meta.batch_affected_dates (batch_id, published_date_wib)
    SELECT %(batch_id)s, (c.published_at AT TIME ZONE '{WIB}')::date
    FROM core.articles c
    JOIN batch_rows b ON b.id = c.id AND b.created_at = c.created_at
    WHERE c.published_at IS NOT NULL
    UNION
    SELECT %(batch_id)s, (published_at AT TIME ZONE '{WIB}')::date
    FROM batch_rows
    WHERE published_at IS NOT NULL
    ON CONFLICT DO NOTHING
"""

# The WHERE on DO UPDATE is the version guard. It compares
# (updated_at, deleted_at) as a tuple, so a soft delete that did not touch
# updated_at still counts as newer (deleted_at goes from NULL/-infinity to a value).
# Strictly newer only: an identical version is left alone, which keeps re-runs
# from rewriting rows (and _batch_id/_loaded_at) for nothing.
UPSERT_SQL = """
    INSERT INTO core.articles AS c
        (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, _batch_id, _loaded_at)
    SELECT id, title, content, published_at, author_id, created_at, updated_at, deleted_at, %(batch_id)s, now()
    FROM batch_rows
    ON CONFLICT (id, created_at) DO UPDATE SET
        title        = EXCLUDED.title,
        content      = EXCLUDED.content,
        published_at = EXCLUDED.published_at,
        author_id    = EXCLUDED.author_id,
        updated_at   = EXCLUDED.updated_at,
        deleted_at   = EXCLUDED.deleted_at,
        _batch_id    = EXCLUDED._batch_id,
        _loaded_at   = EXCLUDED._loaded_at
    WHERE (c.updated_at, COALESCE(c.deleted_at, '-infinity'))
        < (EXCLUDED.updated_at, COALESCE(EXCLUDED.deleted_at, '-infinity'))
"""
# (RETURNING xmax = 0, the usual insert-vs-update trick, is not allowed on a
# partitioned table, so inserts are counted up front instead. That count is
# exact because we hold the core write lock.)
EXISTING_SQL = """
    SELECT count(*) FROM batch_rows b
    WHERE EXISTS (SELECT 1 FROM core.articles c WHERE c.id = b.id AND c.created_at = b.created_at)
"""

# Tombstones to apply: the ones extracted in this batch, plus any stored earlier
# for ids in this batch. The second part closes a race: a backfill (or heal)
# extract taken BEFORE an article was hard-deleted can be merged AFTER the
# hourly run applied the tombstone to a row that was not in core yet.
TOMBSTONES_SQL = """
    CREATE TEMP TABLE batch_tombstones ON COMMIT DROP AS
    SELECT DISTINCT ON (article_id) article_id, created_at, deleted_at
    FROM raw.article_hard_deletes
    WHERE _batch_id = %(batch_id)s
       OR article_id IN (SELECT id FROM batch_rows)
    ORDER BY article_id, deleted_at DESC
"""

HARD_DELETE_DATES_SQL = f"""
    INSERT INTO meta.batch_affected_dates (batch_id, published_date_wib)
    SELECT DISTINCT %(batch_id)s, (c.published_at AT TIME ZONE '{WIB}')::date
    FROM batch_tombstones d
    JOIN core.articles c ON c.id = d.article_id AND c.created_at = d.created_at
    WHERE c.published_at IS NOT NULL
    ON CONFLICT DO NOTHING
"""

# A tombstone from the trigger log carries the exact delete time, so it may
# replace a time that the daily reconciliation only estimated.
HARD_DELETE_SQL = """
    UPDATE core.articles c
    SET hard_deleted_at    = d.deleted_at,
        hard_delete_source = 'trigger_log',
        _batch_id          = %(batch_id)s,
        _loaded_at         = now()
    FROM batch_tombstones d
    WHERE c.id = d.article_id
      AND c.created_at = d.created_at
      AND (c.hard_deleted_at IS NULL OR c.hard_delete_source = 'reconcile')
"""


def merge_batch(settings: Settings, batch_id: str) -> dict:
    params = {"batch_id": batch_id}
    with warehouse(settings.warehouse_dsn) as conn:
        lock_core(conn)
        conn.execute("DELETE FROM meta.batch_affected_dates WHERE batch_id = %(batch_id)s", params)
        conn.execute(DEDUPE_SQL, params)
        distinct_rows = conn.execute("SELECT count(*) FROM batch_rows").fetchone()[0]
        conn.execute(AFFECTED_DATES_SQL, params)

        existing = conn.execute(EXISTING_SQL).fetchone()[0]
        written = conn.execute(UPSERT_SQL, params).rowcount  # inserts + updates that passed the guard
        inserted = distinct_rows - existing
        updated = written - inserted

        conn.execute(TOMBSTONES_SQL, params)
        conn.execute(HARD_DELETE_DATES_SQL, params)
        hard_deleted = conn.execute(HARD_DELETE_SQL, params).rowcount

    return {
        "batch_id": batch_id,
        "rows_in_batch": distinct_rows,
        "rows_inserted": inserted,
        "rows_updated": updated,
        "rows_unchanged": distinct_rows - inserted - updated,
        "hard_deletes_applied": hard_deleted,
    }
