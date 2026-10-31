# Decisions

Short records: what was decided, why, what else was considered, and what it
costs. The last section lists things that turned up while building and testing.

## D1. ELT with a raw layer, not ETL

**Decision.** Extract rows as they are, load them into `raw`, transform with SQL in the warehouse.
**Why.** If the merge or mart logic has a bug, it can be fixed and replayed
from `raw` without touching the production database again. `raw` also keeps
every version of every article, which is the history SCD-style analysis needs later.
**Cost.** Storage: `raw` grows with every edit. Needs a retention policy in production.

## D2. Watermark = Airflow data interval, not `max(updated_at)` from the warehouse

**Decision.** Each run extracts its own `[start - lookback, end)`.
**Why.** Deterministic: the same run always reads the same window, so retries,
manual re-runs and per-hour backfills are trivial. A stored high-water mark
depends on when the run happens and is hard to re-run for one specific hour.
**Cost.** Depends on the orchestrator providing correct intervals (see F1),
and a pure interval design loses hours that never get a run: with
`catchup=False` an outage of the scheduler skips those intervals. The `plan`
task therefore extends the read window back to the end of the last successful
hourly window when there is a gap (F8). `catchup=True` with a recent
start_date was the alternative; it replays every missed hour one by one,
which is slower to recover and floods the scheduler after a long outage.

## D3. Filter `deleted_at` as well as `updated_at`

**Why.** The schema promises `updated_at` changes on create and update only.
A soft delete that sets just `deleted_at` would be missed by an
`updated_at`-only query. The partial index `WHERE deleted_at IS NOT NULL`
keeps the extra branch cheap.

## D4. 15-minute lookback

**Why.** `now()` in PostgreSQL is the transaction start time, so a row can
become visible after the run that "owns" its timestamp has finished. Replica
lag has the same effect.
**Alternatives.** Delaying each run by N minutes (same effect, later data);
CDC (no timestamp dependency at all).
**Cost.** Each run re-reads ~15 minutes of rows; the merge ignores those that
are already current.

## D5. Idempotency: deterministic batch id + replace-batch + version-guarded upsert

- Batch id from the interval (`hourly__20261030T0900Z`), never a UUID.
- `raw` rows for the batch are deleted and inserted in one transaction.
- Upsert only when `(updated_at, deleted_at)` is **strictly** newer.
  Strictly: a re-run writes nothing (the whole-table checksum is identical,
  not just "the same data").
  Tuple with `deleted_at`: a soft delete with an unchanged `updated_at` still counts as newer.
**Cost.** An un-delete that does not bump `updated_at` would be ignored (assumption 3 in the README).

## D6. Parquet as the hand-off between extract and load

**Why.** Typed, compressed, no escaping problems with article text, and the
same format a production setup would write to GCS before a BigQuery load
job. Written to a temp name and renamed, so a crash never leaves a
half-written file that a retry could mistake for a complete one.
**Alternative.** Stream rows source → warehouse directly. Simpler, but loses the
replayable file and couples the two databases' transactions.

## D7. Partition `core.articles` by month of `created_at`

**Why.** Never NULL, never changes, so rows never move partitions and the
merge can target one partition per row. Monthly keeps the partition count
at ~130 for ten years.
**Alternative.** `published_at`: what analysts filter on, but NULL for drafts and
mutable. It is indexed instead (a clustering column in BigQuery).

## D8. Hard deletes: trigger log + daily reconciliation

See README, Bonus 2. Two layers because the trigger is exact but can be
bypassed, and reconciliation is complete but only daily and only knows when
it *noticed* the delete. Log-based CDC is the production next step;
it was not used here because it adds Kafka/Debezium (or a paid service) and a
replication slot whose failure mode is filling the primary's disk.

## D9. Tombstones, not physical deletes, in the warehouse

**Why.** Keeps "how many articles were removed, and when" answerable; marts
exclude deleted rows, so analysts see the same live set as the source.
**Cost.** Legal takedowns need a separate purge across `raw` and `core`.

## D10. Incremental mart by recomputing affected days

**Why.** Recomputing whole days from core is always correct, however batches
are ordered or repeated. +1/-1 deltas are faster but drift for ever after one
double-applied batch. Old publish dates are recorded before the merge so a
rescheduled article fixes both days.

## D11. Quality checks scoped to the batch, two severities

Whole-table checks every hour would cost more than the load. Data errors fail
without retry; anomalies only warn, because a quiet hour at 03:00 WIB is not an
incident.

## D12. One code path, three entry points

DAG tasks, the CLI and the tests call the same functions in `pipeline/`. DAG
files only wire tasks together. Logic can be tested without Airflow, and the
CLI makes the pipeline runnable for a reviewer in one command.

## D13. PostgreSQL as the warehouse for this submission

**Why.** The whole thing runs with `docker compose up`, with no cloud account or
credentials needed by the reviewer. The SQL is written so the BigQuery
translation is mechanical (docs/bigquery.md).
**Cost.** Row storage, not columnar; no scan-based cost model to demonstrate.

## D14. Airflow, not Mage

Airflow is the more common choice for data platform work and its
data-interval model fits incremental loads directly. LocalExecutor is enough on
one machine; the official compose file was trimmed (no Celery/Redis/triggerer).

---

## Found while building

**F1. Airflow 3 turns a cron string into a timetable without a data interval.**
`create_cron_data_intervals` defaults to `False` in 3.3.2, so
`schedule="0 * * * *"` gives `CronTriggerTimetable` with
`data_interval_start == data_interval_end`. The incremental query would
return nothing every hour, with green runs. Fixed by an explicit
`CronDataIntervalTimetable`, a guard in `plan`, and a test.

**F2. Manual runs process the interval that ends at the given time.**
`airflow dags test articles_hourly 2026-10-30T01:00:00+00:00` loaded
00:00-01:00, not 01:00-02:00. Documented in the README.

**F3. `RETURNING (xmax = 0)` does not work on a partitioned table.**
The common trick to tell inserts from updates in an upsert fails with
"cannot retrieve a system column in this context". Inserts are counted
before the upsert instead (exact, because the core write lock is held).

**F4. Server time zone leaks into results.** The local PostgreSQL clusters
defaulted to the machine's time zone (UTC+8), so `date_trunc('month', created_at)`
would have grouped by local months. Every pipeline connection sets UTC
explicitly; WIB is applied only in the mart, by name.

**F5. Database encoding.** A database created as `SQL_ASCII` returns text as
bytes to the driver; the Docker images create UTF-8 databases, and the test
setup relies on that.

**F6. Mapping over a key of a multi-output task.** Airflow 3 refuses
`task.expand(month=plan()["months"])`; the `plan` task returns the plain list.

**F7. Synthetic data artefact.** The first version of the seed clamped
future timestamps to "end of history", creating hundreds of rows with the
exact same `updated_at`. Harmless for correctness, but unrealistic; the seed
now spreads them.

**F8. A scheduler outage would have lost hours.** The first version relied on
every interval getting a run. With `catchup=False`, intervals missed during
an outage are simply not scheduled. Found while writing D2; fixed in
`pipeline/jobs.py::extraction_start` and covered by a test.

**F9. A backfill could resurrect a deleted article.** Backfill extracts a
month, the article is then hard-deleted, the hourly run applies the tombstone
to a row that is not in core yet (no-op), and the backfill merge inserts the
article as live. Reconciliation would fix it a day later; instead every merge
now also applies stored tombstones for the ids it loads. The test fails
without the fix.
