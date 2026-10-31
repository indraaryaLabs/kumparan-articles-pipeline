"""The hourly path: what gets extracted, and what happens when runs repeat."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pyarrow.parquet as pq
import pytest

from pipeline.extract import extract_incremental
from pipeline.jobs import run_incremental
from pipeline.load import load_batch
from pipeline.merge import merge_batch
from tests.conftest import core_checksum, core_row, insert_article, utc

H10, H11, H12 = utc(2026, 10, 30, 10), utc(2026, 10, 30, 11), utc(2026, 10, 30, 12)
US = timedelta(microseconds=1)


def extracted_ids(settings, start, end) -> set[int]:
    result = extract_incremental(settings, "t", start, end)
    return set(pq.read_table(result.articles_path).column("id").to_pylist())


def test_window_is_half_open_with_lookback(settings, source):
    lo = H10 - settings.lookback
    before_lookback = insert_article(source, created_at=lo - US)
    at_lookback = insert_article(source, created_at=lo)
    inside = insert_article(source, created_at=H10 + timedelta(minutes=30))
    last_microsecond = insert_article(source, created_at=H11 - US)
    at_end = insert_article(source, created_at=H11)

    ids = extracted_ids(settings, H10, H11)

    assert ids == {at_lookback, inside, last_microsecond}
    assert before_lookback not in ids
    assert at_end not in ids, "a row stamped exactly at the end belongs to the NEXT window"


def test_soft_delete_that_does_not_touch_updated_at_is_captured(settings, source, warehouse_conn):
    old = utc(2019, 5, 1)
    article = insert_article(source, created_at=old)
    run_incremental(settings, H10 - timedelta(days=4000), H10)  # wide window: initial state
    source.execute("UPDATE articles SET deleted_at = %s WHERE id = %s", (H11 + timedelta(minutes=5), article))

    run_incremental(settings, H11, H12)

    row = core_row(warehouse_conn, article)
    assert row["updated_at"] == old, "the source never changed updated_at"
    assert row["deleted_at"] is not None and row["is_deleted"] is True


def test_late_commit_is_caught_by_the_lookback(settings, source, warehouse_conn):
    """A transaction starts at 10:59:58 (so now() = 10:59:58) but commits after
    the 10:00-11:00 run has already read the table."""
    run_incremental(settings, H10, H11)  # the row does not exist yet at this point
    late = insert_article(source, created_at=H11 - timedelta(seconds=2))  # "commits" now

    run_incremental(settings, H11, H12)

    assert core_row(warehouse_conn, late) is not None


def test_without_lookback_the_late_commit_is_lost(settings, source, warehouse_conn):
    """Same scenario with lookback = 0: shows why the lookback exists."""
    no_lookback = replace(settings, lookback=timedelta(0))
    run_incremental(no_lookback, H10, H11)
    late = insert_article(source, created_at=H11 - timedelta(seconds=2))
    run_incremental(no_lookback, H11, H12)

    assert core_row(warehouse_conn, late) is None


def test_a_skipped_hour_is_read_by_the_next_run(settings, source, warehouse_conn):
    """Scheduler down 11:00-12:00: with catchup=False that run never exists.
    The 12:00 run must notice the gap and read it."""
    run_incremental(settings, H10, H11)
    changed_while_down = insert_article(source, created_at=H11 + timedelta(minutes=20))

    run_incremental(settings, H12, H12 + timedelta(hours=1))  # 11:00 run skipped

    assert core_row(warehouse_conn, changed_while_down) is not None
    extract_from = warehouse_conn.execute(
        "SELECT extract_from FROM meta.etl_run_log WHERE batch_id = 'hourly__20261030T1200Z'"
    ).fetchone()[0]
    assert extract_from == H11 - settings.lookback


def test_rerunning_a_window_changes_nothing(settings, source, warehouse_conn):
    for minute in range(0, 60, 7):
        insert_article(source, created_at=H10 + timedelta(minutes=minute), published_at=H10)
    edited = insert_article(source, created_at=utc(2020, 1, 1))
    source.execute(
        "UPDATE articles SET title = 'v2', updated_at = %s WHERE id = %s", (H10 + timedelta(minutes=3), edited)
    )

    first = run_incremental(settings, H10, H11)
    checksum = core_checksum(warehouse_conn)
    second = run_incremental(settings, H10, H11)

    assert core_checksum(warehouse_conn) == checksum, "a re-run must not rewrite anything"
    assert first["rows_inserted"] == 10
    assert (second["rows_inserted"], second["rows_updated"], second["rows_unchanged"]) == (0, 0, 10)
    raw_count = warehouse_conn.execute(
        "SELECT count(*) FROM raw.articles WHERE _batch_id = %s", (first["batch_id"],)
    ).fetchone()[0]
    assert raw_count == 10, "raw rows of a batch are replaced, never appended twice"
    attempts = warehouse_conn.execute(
        "SELECT attempts, status FROM meta.etl_run_log WHERE batch_id = %s", (first["batch_id"],)
    ).fetchone()
    assert attempts == (2, "success")


def test_an_older_version_never_overwrites_a_newer_one(settings, source, warehouse_conn):
    """Replaying an old batch (out-of-order run, manual fix of last week) is harmless."""
    article = insert_article(source, created_at=H10, title="v1")
    old_batch = extract_incremental(settings, "replay_old", H10, H11)  # captures v1
    source.execute(
        "UPDATE articles SET title = 'v2', updated_at = %s WHERE id = %s", (H11 + timedelta(minutes=1), article)
    )
    run_incremental(settings, H11, H12)  # core now has v2

    load_batch(settings, old_batch.to_dict())
    result = merge_batch(settings, "replay_old")

    assert result["rows_unchanged"] == 1
    assert core_row(warehouse_conn, article)["title"] == "v2"


def test_duplicate_versions_in_one_batch_keep_the_newest(settings, source, warehouse_conn):
    article = insert_article(source, created_at=H10, title="v1")
    batch = extract_incremental(settings, "dupes", H10, H11)
    load_batch(settings, batch.to_dict())
    # Same id twice in raw for one batch, as an overlapping extract could produce.
    warehouse_conn.execute(
        "INSERT INTO raw.articles SELECT _batch_id, _extracted_at, id, 'v2', content, published_at, author_id, "
        "created_at, updated_at + interval '1 minute', deleted_at FROM raw.articles WHERE _batch_id = 'dupes'"
    )
    merge_batch(settings, "dupes")
    assert core_row(warehouse_conn, article)["title"] == "v2"


def test_empty_window_is_rejected(settings):
    with pytest.raises(ValueError, match="Empty or inverted window"):
        extract_incremental(settings, "x", H10, H10)


def test_awkward_text_survives_the_round_trip(settings, source, warehouse_conn):
    content = 'koma, "kutip"\nbaris baru\r\n\ttab \\ backslash 🇮🇩 émoji <b>html</b> ;semicolon\x01'
    article = insert_article(source, created_at=H10, title='Judul, "dengan" kutip', content=content)
    run_incremental(settings, H10, H11)
    row = core_row(warehouse_conn, article)
    assert row["content"] == content
    assert row["title"] == 'Judul, "dengan" kutip'
