-- Warehouse schema.
--
--   raw   append-only copy of every extracted batch (all versions, replayable)
--   core  one row per article, current state, soft + hard delete flags
--   mart  analyst-facing tables/views (no article body, WIB dates, deleted rows removed)
--   meta  pipeline bookkeeping: run log, backfill state, affected dates
--
-- PostgreSQL is used as the warehouse so the whole project runs locally with
-- one command. docs/bigquery.md shows the same design on BigQuery.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- ---------------------------------------------------------------------------
-- raw
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.articles (
    _batch_id     TEXT        NOT NULL,
    _extracted_at TIMESTAMPTZ NOT NULL,
    id            BIGINT      NOT NULL,
    title         TEXT        NOT NULL,
    content       TEXT        NOT NULL,
    published_at  TIMESTAMPTZ,
    author_id     BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_raw_articles_batch ON raw.articles (_batch_id);

CREATE TABLE IF NOT EXISTS raw.article_hard_deletes (
    _batch_id     TEXT        NOT NULL,
    _extracted_at TIMESTAMPTZ NOT NULL,
    article_id    BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    deleted_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_hard_deletes_batch ON raw.article_hard_deletes (_batch_id);
-- Merges look up earlier tombstones for the ids they load. Keep this table out
-- of any raw retention policy: it is small and a dropped tombstone means a ghost article.
CREATE INDEX IF NOT EXISTS idx_raw_hard_deletes_article ON raw.article_hard_deletes (article_id);

-- ---------------------------------------------------------------------------
-- core
-- Partitioned by month of created_at: created_at is never NULL and never
-- changes, so a row never has to move between partitions. published_at would
-- be nicer for analysts but it can be NULL (drafts) and can change (reschedule).
-- The primary key must contain the partition key, hence (id, created_at).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.articles (
    id                 BIGINT      NOT NULL,
    title              TEXT        NOT NULL,
    content            TEXT        NOT NULL,
    published_at       TIMESTAMPTZ,
    author_id          BIGINT      NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    deleted_at         TIMESTAMPTZ,                -- soft delete, from the source
    hard_deleted_at    TIMESTAMPTZ,                -- hard delete, detected by the pipeline
    hard_delete_source TEXT CHECK (hard_delete_source IN ('trigger_log', 'reconcile')),
    is_deleted         BOOLEAN GENERATED ALWAYS AS (deleted_at IS NOT NULL OR hard_deleted_at IS NOT NULL) STORED,
    _batch_id          TEXT        NOT NULL,       -- last batch that changed this row
    _loaded_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Rows with a created_at outside every monthly partition land here.
-- A quality check warns when it is not empty.
CREATE TABLE IF NOT EXISTS core.articles_default PARTITION OF core.articles DEFAULT;

CREATE INDEX IF NOT EXISTS idx_core_articles_published_at ON core.articles (published_at);
CREATE INDEX IF NOT EXISTS idx_core_articles_author ON core.articles (author_id);
-- Lets the hourly freshness check read max(updated_at) from the index instead of scanning.
CREATE INDEX IF NOT EXISTS idx_core_articles_updated_at ON core.articles (updated_at);

-- Creates missing monthly partitions between two months (inclusive). Safe to call repeatedly.
CREATE OR REPLACE FUNCTION meta.ensure_core_partitions(p_from DATE, p_to DATE)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    m       DATE := date_trunc('month', p_from)::date;
    created INTEGER := 0;
    part    TEXT;
BEGIN
    WHILE m <= date_trunc('month', p_to)::date LOOP
        part := format('articles_y%sm%s', to_char(m, 'YYYY'), to_char(m, 'MM'));
        IF to_regclass('core.' || part) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE core.%I PARTITION OF core.articles FOR VALUES FROM (%L) TO (%L)',
                part, (m::timestamp AT TIME ZONE 'UTC'), ((m + INTERVAL '1 month')::timestamp AT TIME ZONE 'UTC'));
            created := created + 1;
        END IF;
        m := (m + INTERVAL '1 month')::date;
    END LOOP;
    RETURN created;
END $$;

SELECT meta.ensure_core_partitions('2016-01-01', (now() + INTERVAL '12 months')::date);

-- ---------------------------------------------------------------------------
-- mart
-- ---------------------------------------------------------------------------
-- Articles published per author per day (Asia/Jakarta). Deleted articles are excluded.
-- Refreshed incrementally: only the days touched by a batch are recomputed.
CREATE TABLE IF NOT EXISTS mart.daily_articles_by_author (
    published_date_wib DATE        NOT NULL,
    author_id          BIGINT      NOT NULL,
    articles_published INTEGER     NOT NULL,
    refreshed_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (published_date_wib, author_id)
);

-- Analyst entry point: live articles only, no article body, local-time columns ready.
CREATE OR REPLACE VIEW mart.articles AS
SELECT id,
       title,
       author_id,
       length(content)                                 AS content_length,
       published_at,
       (published_at AT TIME ZONE 'Asia/Jakarta')      AS published_at_wib,
       (published_at AT TIME ZONE 'Asia/Jakarta')::date AS published_date_wib,
       created_at,
       updated_at
FROM core.articles
WHERE NOT is_deleted;

-- ---------------------------------------------------------------------------
-- meta
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.etl_run_log (
    batch_id             TEXT PRIMARY KEY,
    run_type             TEXT NOT NULL CHECK (run_type IN ('hourly', 'backfill', 'reconcile')),
    window_start         TIMESTAMPTZ,
    window_end           TIMESTAMPTZ,
    extract_from         TIMESTAMPTZ,   -- window_start minus the lookback
    status               TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    attempts             INTEGER NOT NULL DEFAULT 1,
    rows_extracted       INTEGER,
    deletes_extracted    INTEGER,
    rows_inserted        INTEGER,
    rows_updated         INTEGER,
    rows_unchanged       INTEGER,       -- already up to date (overlap, reruns) or older than core
    hard_deletes_applied INTEGER,
    mart_days_refreshed  INTEGER,
    quality              JSONB,
    error                TEXT,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_run_log_type_window ON meta.etl_run_log (run_type, window_start);

-- Days whose mart rows must be recomputed because of a batch (old and new published dates).
CREATE TABLE IF NOT EXISTS meta.batch_affected_dates (
    batch_id           TEXT NOT NULL,
    published_date_wib DATE NOT NULL,
    PRIMARY KEY (batch_id, published_date_wib)
);

-- One row: the moment the historical backfill started (T0), taken from the SOURCE clock.
CREATE TABLE IF NOT EXISTS meta.backfill_state (
    pipeline   TEXT PRIMARY KEY,
    t0         TIMESTAMPTZ NOT NULL,
    planned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.backfill_checkpoint (
    month          DATE PRIMARY KEY,
    status         TEXT NOT NULL CHECK (status IN ('pending', 'done')),
    rows_extracted INTEGER,
    finished_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS meta.reconcile_result (
    batch_id          TEXT NOT NULL,
    month             DATE NOT NULL,
    source_rows       BIGINT NOT NULL,
    warehouse_rows    BIGINT NOT NULL,
    missing_in_wh     INTEGER,   -- in source, not in warehouse (healed if small)
    gone_from_source  INTEGER,   -- in warehouse, not in source -> hard deleted
    PRIMARY KEY (batch_id, month)
);
