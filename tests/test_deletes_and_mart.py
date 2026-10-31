"""Hard deletes (trigger log + reconciliation) and the incremental mart."""

from __future__ import annotations

from datetime import date, timedelta

from pipeline.jobs import run_incremental
from pipeline.reconcile import run_reconcile
from tests.conftest import core_row, insert_article, mart_rows, mart_truth, utc

H10, H11, H12 = utc(2026, 10, 30, 10), utc(2026, 10, 30, 11), utc(2026, 10, 30, 12)


def test_hard_delete_is_applied_from_the_trigger_log(settings, source, warehouse_conn):
    article = insert_article(source, created_at=utc(2018, 3, 3), published_at=utc(2018, 3, 3, 5))
    run_incremental(settings, utc(2018, 1, 1), H10)
    assert core_row(warehouse_conn, article)["is_deleted"] is False

    source.execute("DELETE FROM articles WHERE id = %s", (article,))
    deleted_at = source.execute("SELECT deleted_at FROM articles_deleted_log").fetchone()[0]
    run_incremental(
        settings,
        deleted_at.replace(minute=0, second=0, microsecond=0),
        deleted_at.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1),
    )

    row = core_row(warehouse_conn, article)
    assert row["hard_deleted_at"] == deleted_at
    assert row["hard_delete_source"] == "trigger_log"
    assert row["is_deleted"] is True
    assert mart_rows(warehouse_conn) == mart_truth(warehouse_conn)


def test_tombstone_that_arrives_before_the_row_still_wins(settings, source, warehouse_conn):
    """Backfill snapshot taken, article hard-deleted, hourly run applies the
    tombstone (row not in core yet), THEN the backfill merges the old snapshot."""
    from pipeline.extract import extract_month
    from pipeline.load import load_batch
    from pipeline.merge import merge_batch

    article = insert_article(source, created_at=utc(2019, 7, 7), published_at=utc(2019, 7, 7, 3))
    snapshot = extract_month(settings, "backfill__2019-07", utc(2019, 7, 1), utc(2019, 8, 1))

    source.execute("DELETE FROM articles WHERE id = %s", (article,))
    hour = source.execute("SELECT date_trunc('hour', deleted_at) FROM articles_deleted_log").fetchone()[0]
    run_incremental(settings, hour, hour + timedelta(hours=1))
    assert core_row(warehouse_conn, article) is None  # nothing to mark yet

    load_batch(settings, snapshot.to_dict())
    merge_batch(settings, "backfill__2019-07")

    assert core_row(warehouse_conn, article)["is_deleted"] is True


def test_reconcile_finds_deletes_that_bypassed_the_trigger_and_heals_missing_rows(settings, source, warehouse_conn):
    kept = insert_article(source, created_at=utc(2017, 6, 1), published_at=utc(2017, 6, 1, 1))
    bypassed = insert_article(source, created_at=utc(2017, 6, 2), published_at=utc(2017, 6, 2, 1))
    lost = insert_article(source, created_at=utc(2021, 2, 2), published_at=utc(2021, 2, 2, 1))
    run_incremental(settings, utc(2017, 1, 1), H10)

    # A DBA deletes with triggers disabled; separately a row goes missing from the warehouse.
    source.execute("ALTER TABLE articles DISABLE TRIGGER trg_articles_hard_delete")
    source.execute("DELETE FROM articles WHERE id = %s", (bypassed,))
    source.execute("ALTER TABLE articles ENABLE TRIGGER trg_articles_hard_delete")
    warehouse_conn.execute("DELETE FROM core.articles WHERE id = %s", (lost,))

    result = run_reconcile(settings, date(2026, 10, 30))

    assert result["hard_deletes_applied"] == 1
    assert result["rows_inserted"] == 1
    assert core_row(warehouse_conn, bypassed)["hard_delete_source"] == "reconcile"
    assert core_row(warehouse_conn, lost) is not None
    assert core_row(warehouse_conn, kept)["is_deleted"] is False
    assert mart_rows(warehouse_conn) == mart_truth(warehouse_conn)

    again = run_reconcile(settings, date(2026, 10, 31))
    assert (again["months_mismatched"], again["hard_deletes_applied"], again["rows_inserted"]) == (0, 0, 0)


def test_reconcile_ignores_rows_newer_than_the_cutoff(settings, source, warehouse_conn):
    run_incremental(settings, H10, H11)  # cutoff = 11:00 - 15 min
    insert_article(source, created_at=H11 + timedelta(minutes=5))  # not loaded yet: in flight, not missing
    result = run_reconcile(settings, date(2026, 10, 30))
    assert result["months_mismatched"] == 0


def test_mart_is_correct_when_an_article_moves_to_another_day(settings, source, warehouse_conn):
    # 23:30 WIB on the 28th = 16:30 UTC; it moves to the 29th WIB.
    day28 = utc(2026, 10, 28, 16, 30)
    moved = insert_article(source, created_at=day28 - timedelta(hours=1), published_at=day28, author_id=7)
    insert_article(source, created_at=day28 - timedelta(hours=1), published_at=day28, author_id=7)
    run_incremental(settings, utc(2026, 10, 28), H10)
    assert (date(2026, 10, 28), 7, 2) in mart_rows(warehouse_conn)

    source.execute(
        "UPDATE articles SET published_at = %s, updated_at = %s WHERE id = %s",
        (utc(2026, 10, 29, 3), H10 + timedelta(minutes=10), moved),
    )
    run_incremental(settings, H10, H11)

    rows = mart_rows(warehouse_conn)
    assert (date(2026, 10, 28), 7, 1) in rows, "the OLD day must drop to 1"
    assert (date(2026, 10, 29), 7, 1) in rows
    assert rows == mart_truth(warehouse_conn)


def test_published_date_uses_jakarta_time(settings, source, warehouse_conn):
    # 18:00 UTC on the 29th is 01:00 WIB on the 30th.
    insert_article(source, created_at=utc(2026, 10, 29, 17), published_at=utc(2026, 10, 29, 18), author_id=3)
    run_incremental(settings, utc(2026, 10, 29), H10)
    assert mart_rows(warehouse_conn) == {(date(2026, 10, 30), 3, 1)}
