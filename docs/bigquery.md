# The same design on BigQuery

The submission runs on PostgreSQL so it works with one `docker compose up`.
This page shows how each piece maps to BigQuery, the warehouse a media company
of kumparan's size would more likely run. The SQL below is a design reference
and has not been run against a BigQuery project.

## What changes

| Piece | PostgreSQL (this repo) | BigQuery |
| --- | --- | --- |
| Hand-off file | Parquet on a local volume | Parquet in a GCS bucket, `gs://…/articles/<batch_id>.parquet` |
| Load into raw | `COPY` | load job (`bq load` / `LOAD DATA`); batch load jobs are not billed as queries |
| Replace a batch in raw | `DELETE` + `COPY` in one transaction | `DELETE` + `INSERT` in a multi-statement transaction |
| Core table | partitioned by month of `created_at`, PK `(id, created_at)` | `PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)`, `CLUSTER BY id` |
| `is_deleted` | generated column | computed in the views |
| Upsert | `INSERT … ON CONFLICT … WHERE newer` | `MERGE … WHEN MATCHED AND newer` |
| Write lock between DAGs | advisory lock | not needed for correctness (BigQuery serialises conflicting DML on a table); keep `max_active_runs=1` |
| Cost model | none | bytes scanned: every query and MERGE must prune partitions |

## Tables

```sql
CREATE TABLE raw.articles (
  _batch_id     STRING    NOT NULL,
  _extracted_at TIMESTAMP NOT NULL,
  id INT64 NOT NULL, title STRING, content STRING, published_at TIMESTAMP,
  author_id INT64, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, deleted_at TIMESTAMP
)
PARTITION BY DATE(_extracted_at)
CLUSTER BY _batch_id
OPTIONS (partition_expiration_days = 90, require_partition_filter = TRUE);  -- raw retention

CREATE TABLE core.articles (
  id INT64 NOT NULL, title STRING, content STRING, published_at TIMESTAMP, author_id INT64,
  created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, deleted_at TIMESTAMP,
  hard_deleted_at TIMESTAMP, hard_delete_source STRING,
  _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)   -- ~130 partitions for 2016..2026
CLUSTER BY id;                                     -- the MERGE looks rows up by id

CREATE TABLE mart.daily_articles_by_author (
  published_date_wib DATE NOT NULL, author_id INT64 NOT NULL,
  articles_published INT64 NOT NULL, refreshed_at TIMESTAMP NOT NULL
)
PARTITION BY published_date_wib
CLUSTER BY author_id;

CREATE VIEW mart.articles AS
SELECT id, title, author_id, LENGTH(content) AS content_length, published_at,
       DATETIME(published_at, 'Asia/Jakarta') AS published_at_wib,
       DATE(published_at, 'Asia/Jakarta')     AS published_date_wib,
       created_at, updated_at
FROM core.articles
WHERE deleted_at IS NULL AND hard_deleted_at IS NULL;
```

## The merge, restricted to the months in the batch

Without a filter on the partition column, `MERGE` reads all of `core.articles`,
including `content`, every hour. The months a batch touches are known up front
(`created_at` never changes), so the target can be restricted to them:

```sql
DECLARE batch  STRING DEFAULT @batch_id;
DECLARE months ARRAY<TIMESTAMP> DEFAULT (
  SELECT ARRAY_AGG(DISTINCT TIMESTAMP_TRUNC(created_at, MONTH))
  FROM raw.articles
  WHERE _batch_id = batch AND DATE(_extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
);

MERGE core.articles AS t
USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
             PARTITION BY id ORDER BY updated_at DESC, deleted_at DESC, _extracted_at DESC) AS rn
    FROM raw.articles
    WHERE _batch_id = batch AND DATE(_extracted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  ) WHERE rn = 1
) AS s
ON  t.id = s.id
AND t.created_at = s.created_at
AND TIMESTAMP_TRUNC(t.created_at, MONTH) IN UNNEST(months)     -- partition pruning
WHEN MATCHED AND (
       s.updated_at > t.updated_at
    OR (s.updated_at = t.updated_at
        AND IFNULL(s.deleted_at, TIMESTAMP '0001-01-01') > IFNULL(t.deleted_at, TIMESTAMP '0001-01-01'))
) THEN UPDATE SET
  title = s.title, content = s.content, published_at = s.published_at, author_id = s.author_id,
  updated_at = s.updated_at, deleted_at = s.deleted_at, _batch_id = batch, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, _batch_id, _loaded_at)
  VALUES (s.id, s.title, s.content, s.published_at, s.author_id, s.created_at, s.updated_at,
          s.deleted_at, batch, CURRENT_TIMESTAMP());
```

Check the bytes billed with a dry run before and after adding the month
filter. Hourly batches mostly touch the current month plus a few older months
(edits to old articles), so the scan drops from the whole table to a handful of partitions.

## Hard deletes on GCP

- The trigger + reconciliation design carries over unchanged (reconciliation
  compares per-month aggregates; only differing months pull id lists).
- Managed log-based CDC (Datastream from PostgreSQL into BigQuery) is the
  production upgrade. It removes the trigger and the hourly batch latency.
  Before adopting it, check how it applies deletes to the destination and keep
  the daily reconciliation as a safety net.

## Sandbox note

The free BigQuery sandbox does not support DML (`MERGE`, `UPDATE`, `DELETE`),
so this design needs a project with billing (or the free trial) enabled.
