"""
### articles_reconcile

Daily safety net (bonus 2). Compares source and warehouse per month of
created_at and fixes what the hourly pipeline could not see:

* rows in the warehouse that are gone from the source -> marked hard-deleted
  (`hard_delete_source = 'reconcile'`, time = when detected)
* rows in the source that never reached the warehouse -> re-extracted, if there
  are at most `RECONCILE_MAX_HEAL` of them; otherwise reported

Runs at 02:30 WIB, when traffic on the source is lowest.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task

from pipeline.alerts import on_task_failure
from pipeline.config import Settings
from pipeline.reconcile import run_reconcile


@dag(
    dag_id="articles_reconcile",
    schedule=CronDataIntervalTimetable("30 2 * * *", timezone="Asia/Jakarta"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": on_task_failure,
    },
    tags=["articles", "reconcile"],
    doc_md=__doc__,
)
def articles_reconcile():
    @task
    def reconcile() -> dict:
        day = get_current_context()["data_interval_start"].date()
        return run_reconcile(Settings.from_env(), day)

    reconcile()


articles_reconcile()
