-- Source OLTP database: a stand-in for kumparan's production `articles` table.
--
-- The table mirrors the schema from the assessment. Two things here are NOT
-- part of the original schema and would need approval from the team that owns
-- the production database:
--   1. indexes on updated_at / deleted_at / created_at (needed by the extractor)
--   2. articles_deleted_log + trigger (only needed for the hard-delete variant, bonus 2)
-- A read-only role is created for the pipeline so it can never write to the source.

CREATE TABLE IF NOT EXISTS articles (
    id           BIGSERIAL   PRIMARY KEY,
    title        TEXT        NOT NULL,
    content      TEXT        NOT NULL,
    published_at TIMESTAMPTZ,                         -- NULL = draft, never published
    author_id    BIGINT      NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),  -- filled when the article is created
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),  -- set by the app on create/update
    deleted_at   TIMESTAMPTZ                          -- set by the app on soft delete
);

-- Incremental extraction filters on these columns every hour.
CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON articles (updated_at);
-- Partial index: only soft-deleted rows are indexed, so it stays small.
CREATE INDEX IF NOT EXISTS idx_articles_deleted_at ON articles (deleted_at) WHERE deleted_at IS NOT NULL;
-- Backfill (per month) and reconciliation (per month) filter on created_at.
CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles (created_at);

-- ---------------------------------------------------------------------------
-- Hard-delete capture (bonus 2).
-- A hard DELETE leaves nothing behind for a watermark query to find, so the
-- trigger writes a tombstone row. created_at is copied so the warehouse can go
-- straight to the right partition.
-- Statement-level trigger with a transition table: one INSERT per DELETE
-- statement instead of one per row, which matters for bulk deletes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS articles_deleted_log (
    log_id     BIGSERIAL   PRIMARY KEY,
    article_id BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_articles_deleted_log_deleted_at ON articles_deleted_log (deleted_at);

CREATE OR REPLACE FUNCTION log_article_hard_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO articles_deleted_log (article_id, created_at)
    SELECT id, created_at FROM old_rows;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_articles_hard_delete ON articles;
CREATE TRIGGER trg_articles_hard_delete
    AFTER DELETE ON articles
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION log_article_hard_delete();

-- ---------------------------------------------------------------------------
-- Least privilege: the pipeline only ever reads from the source.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_reader') THEN
        CREATE ROLE etl_reader LOGIN PASSWORD 'etl_reader';
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO etl_reader;
GRANT SELECT ON articles, articles_deleted_log TO etl_reader;
