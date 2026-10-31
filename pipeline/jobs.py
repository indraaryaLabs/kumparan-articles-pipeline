"""Batch ids and the end-to-end incremental job.

The Airflow DAGs call the individual steps (extract, load, merge, ...) as
separate tasks; the CLI and the tests call run_incremental(), which chains the
same functions. One implementation, three entry points.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime

from pipeline import runlog
from pipeline.config import Settings
from pipeline.db import warehouse
from pipeline.extract import extract_incremental
from pipeline.load import load_batch
from pipeline.mart import refresh_mart
from pipeline.merge import merge_batch
from pipeline.quality import QualityCheckFailed, run_quality_checks


def as_utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)


# Batch ids are derived from the data interval, never random. Re-running the
# same hour produces the same id, so every write for that hour is replaced
# rather than duplicated.
def hourly_batch_id(window_start: datetime) -> str:
    return f"hourly__{as_utc(window_start):%Y%m%dT%H%MZ}"


def backfill_batch_id(month: date) -> str:
    return f"backfill__{month:%Y-%m}"


def reconcile_batch_id(day: date) -> str:
    return f"reconcile__{day:%Y%m%d}"


def extraction_start(settings: Settings, window_start: datetime) -> datetime:
    """Where this run starts reading: normally window_start, earlier if hours were skipped.

    With catchup=False, Airflow does not create the runs that were missed while
    the scheduler was down; it only schedules the latest interval. If the last
    successful hourly window ended before this window starts, the gap is read
    by this run instead of being lost.
    """
    with warehouse(settings.warehouse_dsn) as conn:
        last_end = conn.execute(
            "SELECT max(window_end) FROM meta.etl_run_log "
            "WHERE run_type = 'hourly' AND status = 'success' AND window_end <= %s",
            (window_start,),
        ).fetchone()[0]
    return last_end if last_end is not None and last_end < window_start else window_start


def ensure_future_partitions(settings: Settings, months_ahead: int = 12) -> int:
    """Keep monthly core partitions ready a year ahead. Cheap, safe to call every run."""
    with warehouse(settings.warehouse_dsn) as conn:
        return conn.execute(
            "SELECT meta.ensure_core_partitions(current_date, (current_date + make_interval(months => %s))::date)",
            (months_ahead,),
        ).fetchone()[0]


def run_incremental(settings: Settings, window_start: datetime, window_end: datetime) -> dict:
    window_start, window_end = as_utc(window_start), as_utc(window_end)
    batch_id = hourly_batch_id(window_start)
    read_from = extraction_start(settings, window_start)
    runlog.start(settings, batch_id, "hourly", window_start, window_end, read_from - settings.lookback)
    quality = None
    try:
        extracted = extract_incremental(settings, batch_id, read_from, window_end)
        load_batch(settings, extracted.to_dict())
        merged = merge_batch(settings, batch_id)
        mart = refresh_mart(settings, batch_id)
        quality = run_quality_checks(settings, batch_id, "hourly", window_end)
    except QualityCheckFailed as exc:
        runlog.fail(settings, batch_id, str(exc), {"checks": [asdict(c) for c in exc.checks]})
        raise
    except Exception as exc:
        runlog.fail(settings, batch_id, f"{type(exc).__name__}: {exc}")
        raise
    stats = {
        "rows_extracted": extracted.articles_rows,
        "deletes_extracted": extracted.hard_deletes_rows,
        **merged,
        **mart,
    }
    runlog.finish(settings, batch_id, stats, quality)
    return {"batch_id": batch_id, **stats, "quality": quality}
