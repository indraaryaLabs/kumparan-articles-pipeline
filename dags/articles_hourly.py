"""
### articles_hourly

Every hour, moves the articles that changed in the previous hour from the
source database into the warehouse.

`plan -> extract -> load -> merge -> refresh_mart -> quality -> finish`

* **Window**: the run's data interval `[start, end)`, widened by `LOOKBACK_MINUTES`
  (default 15) at the start to catch transactions that committed late.
* **Safe to re-run**: the batch id comes from the interval, raw rows for the
  batch are replaced, and the merge only applies versions newer than what
  core already has. Clear any task and run it again.
* **Skipped hours heal themselves**: with catchup=False Airflow does not create
  runs missed during an outage; `plan` notices the gap since the last
  successful window and this run reads from there.
* **Bad data does not retry**: a failed quality check stops the run without
  retries; retries are for transient errors (network, locks), not wrong data.

The schedule is a `CronDataIntervalTimetable` on purpose. In Airflow 3 a plain
cron string becomes a `CronTriggerTimetable` (setting
`[scheduler] create_cron_data_intervals` defaults to False), whose
data_interval_start equals data_interval_end: an incremental query on that
window returns nothing, every hour, while the run shows green.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from pipeline import runlog
from pipeline.alerts import on_task_failure
from pipeline.config import Settings
from pipeline.extract import extract_incremental
from pipeline.jobs import ensure_future_partitions, extraction_start, hourly_batch_id
from pipeline.load import load_batch
from pipeline.mart import refresh_mart
from pipeline.merge import merge_batch
from pipeline.quality import QualityCheckFailed, run_quality_checks


@dag(
    dag_id="articles_hourly",
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # history is the backfill DAG's job, not 90k catch-up runs
    max_active_runs=1,  # hours are applied in order, one at a time
    dagrun_timeout=timedelta(minutes=55),
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": on_task_failure,
    },
    tags=["articles", "incremental"],
    doc_md=__doc__,
)
def articles_hourly():
    @task
    def plan() -> dict:
        ctx = get_current_context()
        start, end = ctx["data_interval_start"], ctx["data_interval_end"]
        if start is None or end is None or end <= start:
            raise AirflowFailException(
                f"Data interval is empty ({start} .. {end}). Check that the DAG uses CronDataIntervalTimetable."
            )
        settings = Settings.from_env()
        batch_id = hourly_batch_id(start)
        ensure_future_partitions(settings)
        read_from = extraction_start(settings, start)  # earlier than start only if hours were skipped
        if read_from < start:
            print(f"Gap since last successful run: reading from {read_from} instead of {start}")
        runlog.start(settings, batch_id, "hourly", start, end, read_from - settings.lookback)
        return {
            "batch_id": batch_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "read_from": read_from.isoformat(),
        }

    @task
    def extract(p: dict) -> dict:
        return extract_incremental(
            Settings.from_env(),
            p["batch_id"],
            datetime.fromisoformat(p["read_from"]),
            datetime.fromisoformat(p["window_end"]),
        ).to_dict()

    @task
    def load(extracted: dict) -> dict:
        return load_batch(Settings.from_env(), extracted)

    @task
    def merge(p: dict, loaded: dict) -> dict:
        return merge_batch(Settings.from_env(), p["batch_id"])

    @task
    def refresh(p: dict, merged: dict) -> dict:
        return refresh_mart(Settings.from_env(), p["batch_id"])

    @task(retries=0)
    def quality(p: dict, mart: dict) -> dict:
        try:
            return run_quality_checks(
                Settings.from_env(), p["batch_id"], "hourly", datetime.fromisoformat(p["window_end"])
            )
        except QualityCheckFailed as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def finish(p: dict, extracted: dict, merged: dict, mart: dict, checks: dict) -> None:
        stats = {
            "rows_extracted": extracted["articles_rows"],
            "deletes_extracted": extracted["hard_deletes_rows"],
            **merged,
            **mart,
        }
        runlog.finish(Settings.from_env(), p["batch_id"], stats, checks)

    p = plan()
    extracted = extract(p)
    loaded = load(extracted)
    merged = merge(p, loaded)
    mart = refresh(p, merged)
    checks = quality(p, mart)
    finish(p, extracted, merged, mart, checks)


articles_hourly()
