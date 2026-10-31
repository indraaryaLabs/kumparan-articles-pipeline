"""Daily reconciliation: the safety net for hard deletes (bonus 2).

The trigger log catches hard deletes precisely, but it can be bypassed
(TRUNCATE, a DBA running SQL with triggers disabled, the log being added after
some rows were already deleted). Once a day we therefore compare source and
warehouse directly.

Cheap first, precise only where needed:
  1. per month of created_at, compare (row count, sum of ids) on both sides.
     Two aggregate queries, no ids leave either database.
  2. only for months that differ, stream the source ids (one narrow column)
     into a temp table in the warehouse and anti-join both ways:
       in warehouse, not in source -> hard-deleted, mark it
       in source, not in warehouse -> missed, re-extract it (if the count is small)

Rows created after the cutoff are ignored: they may simply not have reached
the warehouse yet. The cutoff is the end of the last successful hourly window
minus the lookback.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from pipeline import runlog
from pipeline.config import Settings
from pipeline.db import lock_core, source_snapshot, warehouse
from pipeline.extract import extract_ids
from pipeline.jobs import reconcile_batch_id
from pipeline.load import load_batch
from pipeline.mart import refresh_mart
from pipeline.merge import merge_batch

SOURCE_SUMMARY_SQL = """
    SELECT date_trunc('month', created_at)::date AS month, count(*), COALESCE(sum(id), 0)
    FROM articles WHERE created_at < %(cutoff)s
    GROUP BY 1
"""
WAREHOUSE_SUMMARY_SQL = """
    SELECT date_trunc('month', created_at)::date AS month, count(*), COALESCE(sum(id), 0)
    FROM core.articles WHERE hard_deleted_at IS NULL AND created_at < %(cutoff)s
    GROUP BY 1
"""


def compare_months(settings: Settings, cutoff: datetime) -> list[dict]:
    params = {"cutoff": cutoff}
    with source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as src:
        source = {m: (n, s) for m, n, s in src.execute(SOURCE_SUMMARY_SQL, params).fetchall()}
    with warehouse(settings.warehouse_dsn) as wh:
        wh_rows = {m: (n, s) for m, n, s in wh.execute(WAREHOUSE_SUMMARY_SQL, params).fetchall()}
    result = []
    for month in sorted(set(source) | set(wh_rows)):
        s_n, s_sum = source.get(month, (0, 0))
        w_n, w_sum = wh_rows.get(month, (0, 0))
        result.append(
            {
                "month": month.isoformat(),
                "source_rows": s_n,
                "warehouse_rows": w_n,
                "match": (s_n, s_sum) == (w_n, w_sum),
            }
        )
    return result


def reconcile_cutoff(settings: Settings) -> datetime | None:
    with warehouse(settings.warehouse_dsn) as wh:
        last_window_end = wh.execute(
            "SELECT max(window_end) FROM meta.etl_run_log WHERE run_type = 'hourly' AND status = 'success'"
        ).fetchone()[0]
        t0 = wh.execute("SELECT t0 FROM meta.backfill_state WHERE pipeline = 'articles'").fetchone()
    if last_window_end is not None:
        return last_window_end - settings.lookback
    return t0[0] if t0 else None


def _month_bounds(month: date, cutoff: datetime) -> tuple[datetime, datetime]:
    lo = datetime(month.year, month.month, 1, tzinfo=UTC)
    hi = datetime(month.year + (month.month == 12), month.month % 12 + 1, 1, tzinfo=UTC)
    return lo, min(hi, cutoff)


def _stream_source_ids(src, wh, lo: datetime, hi: datetime) -> None:
    """COPY ids straight from the source into a warehouse temp table, never through Python lists."""
    with (
        src.cursor() as src_cur,
        wh.cursor() as wh_cur,
        src_cur.copy(
            "COPY (SELECT id FROM articles WHERE created_at >= %s AND created_at < %s) TO STDOUT", (lo, hi)
        ) as out,
        wh_cur.copy("COPY src_ids (id) FROM STDIN") as inp,
    ):
        for chunk in out:
            inp.write(chunk)


def run_reconcile(settings: Settings, day: date) -> dict:
    batch_id = reconcile_batch_id(day)
    heal_batch_id = f"{batch_id}__heal"
    cutoff = reconcile_cutoff(settings)
    if cutoff is None:
        return {"batch_id": batch_id, "skipped": "nothing has been loaded yet"}

    runlog.start(settings, batch_id, "reconcile", None, cutoff)
    try:
        months = [date.fromisoformat(m["month"]) for m in compare_months(settings, cutoff) if not m["match"]]
        gone_total, missing_ids, per_month = 0, [], []

        with (
            source_snapshot(settings.source_dsn, settings.source_statement_timeout_ms) as src,
            warehouse(settings.warehouse_dsn) as wh,
        ):
            lock_core(wh)
            wh.execute("DELETE FROM meta.batch_affected_dates WHERE batch_id = %s", (batch_id,))
            wh.execute("DELETE FROM meta.reconcile_result WHERE batch_id = %s", (batch_id,))
            wh.execute("CREATE TEMP TABLE src_ids (id BIGINT PRIMARY KEY) ON COMMIT DROP")
            for month in months:
                lo, hi = _month_bounds(month, cutoff)
                wh.execute("TRUNCATE src_ids")
                _stream_source_ids(src, wh, lo, hi)
                bounds = {"lo": lo, "hi": hi, "batch_id": batch_id}

                wh.execute(
                    """
                    INSERT INTO meta.batch_affected_dates (batch_id, published_date_wib)
                    SELECT DISTINCT %(batch_id)s, (c.published_at AT TIME ZONE 'Asia/Jakarta')::date
                    FROM core.articles c
                    WHERE c.created_at >= %(lo)s AND c.created_at < %(hi)s
                      AND c.hard_deleted_at IS NULL AND c.published_at IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM src_ids s WHERE s.id = c.id)
                    ON CONFLICT DO NOTHING
                    """,
                    bounds,
                )
                gone = wh.execute(
                    """
                    UPDATE core.articles c
                    SET hard_deleted_at = now(), hard_delete_source = 'reconcile',
                        _batch_id = %(batch_id)s, _loaded_at = now()
                    WHERE c.created_at >= %(lo)s AND c.created_at < %(hi)s
                      AND c.hard_deleted_at IS NULL
                      AND NOT EXISTS (SELECT 1 FROM src_ids s WHERE s.id = c.id)
                    """,
                    bounds,
                ).rowcount
                missing = [
                    r[0]
                    for r in wh.execute(
                        """
                    SELECT s.id FROM src_ids s
                    WHERE NOT EXISTS (SELECT 1 FROM core.articles c
                                      WHERE c.id = s.id AND c.created_at >= %(lo)s AND c.created_at < %(hi)s)
                    """,
                        bounds,
                    ).fetchall()
                ]
                source_rows = wh.execute("SELECT count(*) FROM src_ids").fetchone()[0]
                wh_rows = wh.execute(
                    "SELECT count(*) FROM core.articles WHERE created_at >= %(lo)s AND created_at < %(hi)s "
                    "AND hard_deleted_at IS NULL",
                    bounds,
                ).fetchone()[0]
                wh.execute(
                    "INSERT INTO meta.reconcile_result VALUES (%s, %s, %s, %s, %s, %s)",
                    (batch_id, month, source_rows, wh_rows, len(missing), gone),
                )
                gone_total += gone
                missing_ids.extend(missing)
                per_month.append({"month": month.isoformat(), "gone_from_source": gone, "missing_in_wh": len(missing)})

        healed = 0
        heal_note = None
        if missing_ids and len(missing_ids) <= settings.reconcile_max_heal:
            extracted = extract_ids(settings, heal_batch_id, missing_ids)
            load_batch(settings, extracted.to_dict())
            healed = merge_batch(settings, heal_batch_id)["rows_inserted"]
            refresh_mart(settings, heal_batch_id)
        elif missing_ids:
            heal_note = (
                f"{len(missing_ids)} rows missing, above RECONCILE_MAX_HEAL="
                f"{settings.reconcile_max_heal}; run the backfill for these months"
            )
        mart = refresh_mart(settings, batch_id)
    except Exception as exc:
        runlog.fail(settings, batch_id, f"{type(exc).__name__}: {exc}")
        raise

    stats = {
        "rows_extracted": len(missing_ids),
        "rows_inserted": healed,
        "hard_deletes_applied": gone_total,
        "mart_days_refreshed": mart["mart_days_refreshed"],
    }
    quality = {"months_compared_mismatched": per_month, "note": heal_note}
    runlog.finish(settings, batch_id, stats, quality)
    return {
        "batch_id": batch_id,
        "cutoff": cutoff.isoformat(),
        "months_mismatched": len(months),
        **stats,
        "per_month": per_month,
        "note": heal_note,
    }


def yesterday_utc() -> date:
    return (datetime.now(UTC) - timedelta(days=1)).date()
