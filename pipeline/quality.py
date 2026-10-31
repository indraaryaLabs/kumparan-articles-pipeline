"""Data quality checks that run after every batch.

Two levels:
  fail  the data in core is wrong (duplicates, missing rows, a newer version
        lost). The task fails so the problem cannot spread into reports.
  warn  suspicious but possibly legitimate (odd timestamps, a quiet hour).
        Recorded in meta.etl_run_log.quality for a human to look at.

Checks are scoped to the ids in the batch. Scanning the whole multi-billion
row table every hour would cost more than the load itself.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from pipeline.config import Settings
from pipeline.db import warehouse


@dataclass
class Check:
    name: str
    level: str  # "fail" | "warn"
    passed: bool
    value: float | int | None
    detail: str


class QualityCheckFailed(RuntimeError):
    def __init__(self, batch_id: str, checks: list[Check]):
        self.checks = checks
        failed = [c.name for c in checks if c.level == "fail" and not c.passed]
        super().__init__(f"{batch_id}: quality checks failed: {', '.join(failed)}")


BATCH_LATEST = """
    SELECT DISTINCT ON (id) id, created_at, updated_at, deleted_at, published_at
    FROM raw.articles WHERE _batch_id = %(batch_id)s
    ORDER BY id, updated_at DESC, deleted_at DESC NULLS LAST
"""


def _count(conn, sql: str, params: dict) -> int:
    return conn.execute(sql, params).fetchone()[0]


def run_quality_checks(
    settings: Settings,
    batch_id: str,
    run_type: str,
    window_end: datetime | None = None,
) -> dict:
    p = {"batch_id": batch_id}
    checks: list[Check] = []
    with warehouse(settings.warehouse_dsn) as conn:
        dupes = _count(
            conn,
            """
            SELECT count(*) FROM (
                SELECT c.id FROM core.articles c
                WHERE c.id IN (SELECT id FROM raw.articles WHERE _batch_id = %(batch_id)s)
                GROUP BY c.id HAVING count(*) > 1) x""",
            p,
        )
        checks.append(
            Check(
                "no_duplicate_ids",
                "fail",
                dupes == 0,
                dupes,
                "same id stored twice; would mean created_at changed in the source",
            )
        )

        missing = _count(
            conn,
            f"""
            SELECT count(*) FROM ({BATCH_LATEST}) b
            WHERE NOT EXISTS (SELECT 1 FROM core.articles c WHERE c.id = b.id AND c.created_at = b.created_at)""",
            p,
        )
        checks.append(
            Check(
                "batch_rows_present_in_core",
                "fail",
                missing == 0,
                missing,
                "rows extracted in this batch but absent from core",
            )
        )

        stale = _count(
            conn,
            f"""
            SELECT count(*) FROM ({BATCH_LATEST}) b
            JOIN core.articles c ON c.id = b.id AND c.created_at = b.created_at
            WHERE (c.updated_at, COALESCE(c.deleted_at, '-infinity'))
                < (b.updated_at, COALESCE(b.deleted_at, '-infinity'))""",
            p,
        )
        checks.append(
            Check(
                "core_not_older_than_batch",
                "fail",
                stale == 0,
                stale,
                "core holds an older version than this batch delivered",
            )
        )

        bad_ts = _count(conn, f"SELECT count(*) FROM ({BATCH_LATEST}) b WHERE b.updated_at < b.created_at", p)
        checks.append(
            Check("updated_at_not_before_created_at", "warn", bad_ts == 0, bad_ts, "updated_at earlier than created_at")
        )

        early_pub = _count(conn, f"SELECT count(*) FROM ({BATCH_LATEST}) b WHERE b.published_at < b.created_at", p)
        checks.append(
            Check(
                "published_at_not_before_created_at",
                "warn",
                early_pub == 0,
                early_pub,
                "published before created; seen in migrated legacy rows",
            )
        )

        in_default = _count(conn, "SELECT count(*) FROM core.articles_default", p)
        checks.append(
            Check(
                "default_partition_empty",
                "warn",
                in_default == 0,
                in_default,
                "rows whose created_at has no monthly partition",
            )
        )

        if run_type == "hourly" and window_end is not None:
            current = _count(conn, "SELECT count(*) FROM raw.articles WHERE _batch_id = %(batch_id)s", p)
            last_week = conn.execute(
                "SELECT rows_extracted FROM meta.etl_run_log "
                "WHERE run_type = 'hourly' AND status = 'success' AND window_end = %s",
                (window_end - timedelta(days=7),),
            ).fetchone()
            if last_week and last_week[0] and last_week[0] >= 100:
                ratio = current / last_week[0]
                checks.append(
                    Check(
                        "volume_vs_same_hour_last_week",
                        "warn",
                        ratio >= 0.1,
                        round(ratio, 3),
                        f"{current} rows now vs {last_week[0]} a week ago",
                    )
                )
            else:
                checks.append(
                    Check("volume_vs_same_hour_last_week", "warn", True, None, "no comparable run a week ago")
                )

            newest = conn.execute("SELECT max(updated_at) FROM core.articles").fetchone()[0]
            lag_hours = None if newest is None else (window_end - newest).total_seconds() / 3600
            checks.append(
                Check(
                    "freshness",
                    "warn",
                    lag_hours is not None and lag_hours <= 3,
                    None if lag_hours is None else round(lag_hours, 2),
                    "hours between window end and newest updated_at in core",
                )
            )

    result = {"batch_id": batch_id, "checks": [asdict(c) for c in checks]}
    if any(c.level == "fail" and not c.passed for c in checks):
        raise QualityCheckFailed(batch_id, checks)
    return result
