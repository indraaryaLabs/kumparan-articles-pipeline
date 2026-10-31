"""meta.etl_run_log: one row per batch, updated as the run progresses.

Answers questions like "which hour was late yesterday?", "how long do runs
take?" and "how many rows did the 03:00 run move?" with a single query.
"""

from __future__ import annotations

from datetime import datetime

from psycopg.types.json import Jsonb

from pipeline.config import Settings
from pipeline.db import warehouse


def start(
    settings: Settings,
    batch_id: str,
    run_type: str,
    window_start: datetime | None,
    window_end: datetime | None,
    extract_from: datetime | None = None,
) -> None:
    with warehouse(settings.warehouse_dsn) as conn:
        conn.execute(
            """
            INSERT INTO meta.etl_run_log (batch_id, run_type, window_start, window_end, extract_from, status)
            VALUES (%s, %s, %s, %s, %s, 'running')
            ON CONFLICT (batch_id) DO UPDATE SET
                status = 'running', attempts = meta.etl_run_log.attempts + 1,
                window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end,
                extract_from = EXCLUDED.extract_from,
                error = NULL, started_at = now(), finished_at = NULL
            """,
            (batch_id, run_type, window_start, window_end, extract_from),
        )


def finish(settings: Settings, batch_id: str, stats: dict, quality: dict | None) -> None:
    with warehouse(settings.warehouse_dsn) as conn:
        conn.execute(
            """
            UPDATE meta.etl_run_log SET
                status = 'success',
                rows_extracted = %(rows_extracted)s,
                deletes_extracted = %(deletes_extracted)s,
                rows_inserted = %(rows_inserted)s,
                rows_updated = %(rows_updated)s,
                rows_unchanged = %(rows_unchanged)s,
                hard_deletes_applied = %(hard_deletes_applied)s,
                mart_days_refreshed = %(mart_days_refreshed)s,
                quality = %(quality)s,
                error = NULL,
                finished_at = now()
            WHERE batch_id = %(batch_id)s
            """,
            {
                "batch_id": batch_id,
                "rows_extracted": stats.get("rows_extracted"),
                "deletes_extracted": stats.get("deletes_extracted"),
                "rows_inserted": stats.get("rows_inserted"),
                "rows_updated": stats.get("rows_updated"),
                "rows_unchanged": stats.get("rows_unchanged"),
                "hard_deletes_applied": stats.get("hard_deletes_applied"),
                "mart_days_refreshed": stats.get("mart_days_refreshed"),
                "quality": Jsonb(quality) if quality is not None else None,
            },
        )


def fail(settings: Settings, batch_id: str, error: str, quality: dict | None = None) -> None:
    with warehouse(settings.warehouse_dsn) as conn:
        conn.execute(
            "UPDATE meta.etl_run_log SET status = 'failed', error = %s, quality = COALESCE(%s, quality), "
            "finished_at = now() WHERE batch_id = %s",
            (error[:4000], Jsonb(quality) if quality is not None else None, batch_id),
        )
