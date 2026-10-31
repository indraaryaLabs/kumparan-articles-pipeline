"""Backfill hand-over rules and the Airflow schedule."""

from __future__ import annotations

from datetime import timedelta

import pytest

from pipeline.backfill import CoverageError, backfill_month, plan_backfill, validate_backfill
from pipeline.jobs import run_incremental
from tests.conftest import insert_article, utc


def test_backfill_refuses_to_start_before_the_hourly_job_covers_t0(settings, source):
    insert_article(source, created_at=utc(2016, 1, 15))
    with pytest.raises(CoverageError):
        plan_backfill(settings)


def test_backfill_loads_history_is_resumable_and_validates(settings, source, warehouse_conn):
    for month in (1, 2, 3):
        insert_article(source, created_at=utc(2016, month, 10))
    insert_article(source, created_at=utc(2016, 3, 20), deleted_at=utc(2016, 4, 1))
    now = source.execute("SELECT date_trunc('hour', now())").fetchone()[0]
    run_incremental(settings, now - timedelta(hours=1), now)  # hourly coverage starts before T0

    plan = plan_backfill(settings, end_month=utc(2016, 3, 1).date())
    assert plan["months"] == ["2016-01-01", "2016-02-01", "2016-03-01"]

    backfill_month(settings, "2016-01-01")
    # Simulate a crash after month 1: a new plan only returns what is left.
    assert plan_backfill(settings, end_month=utc(2016, 3, 1).date())["months"] == ["2016-02-01", "2016-03-01"]
    assert backfill_month(settings, "2016-01-01") == {"batch_id": "backfill__2016-01", "skipped": True}
    for month in ("2016-02-01", "2016-03-01"):
        backfill_month(settings, month)

    assert warehouse_conn.execute("SELECT count(*) FROM core.articles").fetchone()[0] == 4
    assert validate_backfill(settings)["months_mismatched"] == 0


def test_hourly_dag_uses_a_data_interval_timetable():
    pytest.importorskip("airflow")
    from airflow.sdk import DAG, CronDataIntervalTimetable, CronTriggerTimetable

    from dags.articles_hourly import articles_hourly

    dag = articles_hourly()
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.catchup is False and dag.max_active_runs == 1

    # The trap this guards against: a plain cron string gives a trigger timetable
    # with no data interval under Airflow 3 defaults.
    plain = DAG("plain_cron_string", schedule="0 * * * *", start_date=utc(2026, 1, 1))
    assert isinstance(plain.timetable, CronTriggerTimetable)
