"""Incremental refresh of mart.daily_articles_by_author.

Only the days listed in meta.batch_affected_dates for this batch are
recomputed, from core.articles as it is now. Recomputing a whole day (instead
of adding +1/-1 deltas) means the result is always correct even if batches
run twice or out of order.
"""

from __future__ import annotations

from pipeline.config import Settings
from pipeline.db import lock_core, warehouse

DELETE_DAYS_SQL = """
    DELETE FROM mart.daily_articles_by_author m
    USING meta.batch_affected_dates a
    WHERE a.batch_id = %(batch_id)s AND m.published_date_wib = a.published_date_wib
"""

# Each WIB day is turned into a UTC range so the published_at index can be used,
# instead of computing a date for every row in the table.
RECOMPUTE_DAYS_SQL = """
    INSERT INTO mart.daily_articles_by_author (published_date_wib, author_id, articles_published, refreshed_at)
    SELECT a.published_date_wib, c.author_id, count(*), now()
    FROM meta.batch_affected_dates a
    JOIN core.articles c
      ON c.published_at >= (a.published_date_wib::timestamp AT TIME ZONE 'Asia/Jakarta')
     AND c.published_at <  ((a.published_date_wib + 1)::timestamp AT TIME ZONE 'Asia/Jakarta')
    WHERE a.batch_id = %(batch_id)s
      AND NOT c.is_deleted
    GROUP BY a.published_date_wib, c.author_id
"""


def refresh_mart(settings: Settings, batch_id: str) -> dict:
    params = {"batch_id": batch_id}
    with warehouse(settings.warehouse_dsn) as conn:
        lock_core(conn)  # read core in a state no other merge is halfway through
        days = conn.execute(
            "SELECT count(*) FROM meta.batch_affected_dates WHERE batch_id = %(batch_id)s", params
        ).fetchone()[0]
        conn.execute(DELETE_DAYS_SQL, params)
        rows = conn.execute(RECOMPUTE_DAYS_SQL, params).rowcount
    return {"batch_id": batch_id, "mart_days_refreshed": days, "mart_rows_written": rows}
