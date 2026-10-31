"""Historical backfill (bonus 1): load everything since 2016, one month at a time.

Design:
  * Separate from the hourly DAG. Using catchup on the hourly DAG from 2016
    would mean ~94,000 tiny runs; ~130 monthly chunks are far easier on the
    scheduler and the source.
  * Chunk = one calendar month of created_at. created_at never changes, so a
    row belongs to exactly one chunk.
  * Resumable: meta.backfill_checkpoint marks finished months; a re-run skips them.
  * Same load -> merge -> mart path as the hourly job, so the version guard
    keeps backfill from overwriting anything the hourly job loaded later.
  * Hand-over: T0 is read from the SOURCE clock when the backfill is planned.
    The hourly job must already be covering changes from T0 onward; the plan
    refuses to start otherwise, because changes made between the snapshot of
    a month and the first hourly window would be lost.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime

from pipeline import runlog
from pipeline.config import Settings
from pipeline.db import source_snapshot, warehouse
from pipeline.extract import extract_month
from pipeline.jobs import backfill_batch_id
from pipeline.load import load_batch
from pipeline.mart import refresh_mart
from pipeline.merge import merge_batch
from pipeline.quality import QualityCheckFailed, run_quality_checks
from pipeline.reconcile import compare_months


class CoverageError(RuntimeError):
    pass


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def next_month(d: date) -> date:
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def as_ts(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def plan_backfill(
    settings: Settings, start_month: date | None = None, end_month: date | None = None, force: bool = False
) -> dict:
    with source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as src:
        source_now, oldest = src.execute("SELECT now(), min(created_at) FROM articles").fetchone()

    with warehouse(settings.warehouse_dsn) as wh:
        wh.execute(
            "INSERT INTO meta.backfill_state (pipeline, t0) VALUES ('articles', %s) ON CONFLICT DO NOTHING",
            (source_now,),
        )
        t0 = wh.execute("SELECT t0 FROM meta.backfill_state WHERE pipeline = 'articles'").fetchone()[0]

        first_hourly = wh.execute(
            "SELECT min(window_start) FROM meta.etl_run_log WHERE run_type = 'hourly' AND status = 'success'"
        ).fetchone()[0]
        covered = first_hourly is not None and first_hourly <= t0
        if not covered and not force:
            hourly_state = (
                "never succeeded"
                if first_hourly is None
                else f"only covered changes since {first_hourly:%Y-%m-%d %H:%M} UTC"
            )
            raise CoverageError(
                f"Backfill T0 is {t0:%Y-%m-%d %H:%M} UTC but the hourly pipeline has {hourly_state}. "
                "Unpause/run articles_hourly first so every change after T0 is captured, "
                "or pass force=True for a local demo."
            )

        if oldest is None:
            return {"t0": t0.isoformat(), "months": [], "hourly_covers_t0": covered}
        first = month_start(start_month or oldest.date())
        last = month_start(end_month or t0.date())
        wh.execute("SELECT meta.ensure_core_partitions(%s, %s)", (first, last))

        months, m = [], first
        while m <= last:
            months.append(m)
            m = next_month(m)
        wh.execute(
            "INSERT INTO meta.backfill_checkpoint (month, status) SELECT unnest(%s::date[]), 'pending' "
            "ON CONFLICT DO NOTHING",
            (months,),
        )
        pending = [
            r[0]
            for r in wh.execute(
                "SELECT month FROM meta.backfill_checkpoint WHERE status <> 'done' AND month = ANY(%s) ORDER BY month",
                (months,),
            ).fetchall()
        ]

    return {
        "t0": t0.isoformat(),
        "hourly_covers_t0": covered,
        "months_total": len(months),
        "months": [m.isoformat() for m in pending],
    }


def backfill_month(settings: Settings, month_iso: str) -> dict:
    month = month_start(date.fromisoformat(month_iso))
    batch_id = backfill_batch_id(month)
    with warehouse(settings.warehouse_dsn) as wh:
        status = wh.execute("SELECT status FROM meta.backfill_checkpoint WHERE month = %s", (month,)).fetchone()
    if status and status[0] == "done":
        return {"batch_id": batch_id, "skipped": True}

    lo, hi = as_ts(month), as_ts(next_month(month))
    runlog.start(settings, batch_id, "backfill", lo, hi, lo)
    try:
        extracted = extract_month(settings, batch_id, lo, hi)
        load_batch(settings, extracted.to_dict())
        merged = merge_batch(settings, batch_id)
        mart = refresh_mart(settings, batch_id)
        quality = run_quality_checks(settings, batch_id, "backfill")
    except QualityCheckFailed as exc:
        runlog.fail(settings, batch_id, str(exc), {"checks": [asdict(c) for c in exc.checks]})
        raise
    except Exception as exc:
        runlog.fail(settings, batch_id, f"{type(exc).__name__}: {exc}")
        raise

    stats = {"rows_extracted": extracted.articles_rows, "deletes_extracted": 0, **merged, **mart}
    with warehouse(settings.warehouse_dsn) as wh:
        wh.execute(
            "UPDATE meta.backfill_checkpoint SET status = 'done', rows_extracted = %s, finished_at = now() "
            "WHERE month = %s",
            (extracted.articles_rows, month),
        )
    runlog.finish(settings, batch_id, stats, quality)
    return {"batch_id": batch_id, **stats}


def validate_backfill(settings: Settings) -> dict:
    """Row count and id checksum per month, source vs warehouse, for months before T0."""
    with warehouse(settings.warehouse_dsn) as wh:
        row = wh.execute("SELECT t0 FROM meta.backfill_state WHERE pipeline = 'articles'").fetchone()
    if row is None:
        raise RuntimeError("No backfill has been planned")
    comparison = compare_months(settings, cutoff=row[0])
    mismatched = [m for m in comparison if not m["match"]]
    return {
        "cutoff": row[0].isoformat(),
        "months_checked": len(comparison),
        "months_mismatched": len(mismatched),
        "mismatches": mismatched[:20],
    }
