"""
### articles_backfill

One-off (manually triggered) load of the article history since 2016.

`plan -> backfill_month (one mapped task per month, 2 at a time) -> validate`

* **plan** takes T0 from the source clock, refuses to start unless
  `articles_hourly` already covers changes from T0 onward, creates the
  monthly partitions and the checkpoint rows, and returns the months not done yet.
* **backfill_month** extracts one month of created_at, then the same
  load -> merge -> mart path as the hourly DAG. Finished months are skipped
  on a re-run, so a failure at month 47 resumes at month 47.
* **validate** compares row count and id checksum per month, source vs warehouse.

Params: `start_month`, `end_month` (YYYY-MM, empty = everything up to T0),
`force` (skip the hourly-coverage guard; local demo only).
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from pipeline.alerts import on_task_failure
from pipeline.backfill import CoverageError, backfill_month, plan_backfill, validate_backfill
from pipeline.config import Settings


def _month(value: str | None) -> date | None:
    return date.fromisoformat(f"{value}-01") if value else None


@dag(
    dag_id="articles_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "start_month": Param("", type="string", description="YYYY-MM, empty = oldest month in the source"),
        "end_month": Param("", type="string", description="YYYY-MM, empty = month of T0"),
        "force": Param(False, type="boolean", description="skip the hourly-coverage guard (local demo only)"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": on_task_failure,
    },
    tags=["articles", "backfill"],
    doc_md=__doc__,
)
def articles_backfill():
    @task
    def plan() -> list[str]:
        params = get_current_context()["params"]
        try:
            result = plan_backfill(
                Settings.from_env(), _month(params["start_month"]), _month(params["end_month"]), force=params["force"]
            )
        except CoverageError as exc:
            raise AirflowFailException(str(exc)) from exc
        print(f"T0={result['t0']} hourly_covers_t0={result['hourly_covers_t0']} months pending={len(result['months'])}")
        return result["months"]  # a plain list: Airflow maps one task instance per month

    # Two months at a time: enough to finish in reasonable time, gentle on the source replica.
    @task(max_active_tis_per_dagrun=2)
    def load_month(month: str) -> dict:
        return backfill_month(Settings.from_env(), month)

    @task
    def validate(_: list) -> dict:
        result = validate_backfill(Settings.from_env())
        if result["months_mismatched"]:
            # Not fatal: hard deletes that happened after T0 show up here until
            # the hourly run applies them. The details are in the task log.
            print(f"WARNING: {result['months_mismatched']} month(s) differ: {result['mismatches']}")
        return result

    loaded = load_month.expand(month=plan())
    validate(loaded)


articles_backfill()
