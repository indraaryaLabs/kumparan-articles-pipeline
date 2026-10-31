"""Command line entry point: run any job without Airflow.

python -m pipeline incremental --start 2026-10-30T09:00Z --end 2026-10-30T10:00Z
python -m pipeline backfill [--start-month 2016-01] [--end-month 2026-10] [--force]
python -m pipeline reconcile [--day 2026-10-29]
python -m pipeline report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime

from pipeline.backfill import backfill_month, plan_backfill, validate_backfill
from pipeline.config import Settings
from pipeline.db import warehouse
from pipeline.jobs import run_incremental
from pipeline.reconcile import run_reconcile, yesterday_utc


def _ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def _month(value: str) -> date:
    return date.fromisoformat(f"{value}-01") if len(value) == 7 else date.fromisoformat(value)


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def report(settings: Settings) -> None:
    with warehouse(settings.warehouse_dsn) as wh:
        print("Last runs:")
        for row in wh.execute(
            "SELECT batch_id, status, attempts, rows_extracted, rows_inserted, rows_updated, rows_unchanged, "
            "hard_deletes_applied, round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS secs "
            "FROM meta.etl_run_log ORDER BY started_at DESC LIMIT 10"
        ).fetchall():
            print("  ", row)
        total, live, soft, hard = wh.execute(
            "SELECT count(*), count(*) FILTER (WHERE NOT is_deleted), "
            "count(*) FILTER (WHERE deleted_at IS NOT NULL), count(*) FILTER (WHERE hard_deleted_at IS NOT NULL) "
            "FROM core.articles"
        ).fetchone()
        print(f"core.articles: {total:,} rows, {live:,} live, {soft:,} soft-deleted, {hard:,} hard-deleted")
        print("Busiest days in mart.daily_articles_by_author:")
        for row in wh.execute(
            "SELECT published_date_wib, sum(articles_published) FROM mart.daily_articles_by_author "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
        ).fetchall():
            print("  ", row)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    inc = sub.add_parser("incremental", help="run one incremental window")
    inc.add_argument("--start", type=_ts, required=True)
    inc.add_argument("--end", type=_ts, required=True)

    bf = sub.add_parser("backfill", help="load history month by month")
    bf.add_argument("--start-month", type=_month)
    bf.add_argument("--end-month", type=_month)
    bf.add_argument("--force", action="store_true", help="skip the hourly-coverage guard (local demo only)")

    rc = sub.add_parser("reconcile", help="compare source and warehouse, mark hard deletes")
    rc.add_argument("--day", type=date.fromisoformat, default=None)

    sub.add_parser("report", help="print run log and table counts")

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.command == "incremental":
        _print(run_incremental(settings, args.start, args.end))
    elif args.command == "backfill":
        plan = plan_backfill(settings, args.start_month, args.end_month, force=args.force)
        print(f"T0={plan['t0']}  months pending={len(plan['months'])}")
        for month in plan["months"]:
            result = backfill_month(settings, month)
            print(
                f"  {month}: {result.get('rows_extracted', 0):>7} rows, "
                f"inserted {result.get('rows_inserted', 0)}, updated {result.get('rows_updated', 0)}"
            )
        _print(validate_backfill(settings))
    elif args.command == "reconcile":
        _print(run_reconcile(settings, args.day or yesterday_utc()))
    elif args.command == "report":
        report(settings)
