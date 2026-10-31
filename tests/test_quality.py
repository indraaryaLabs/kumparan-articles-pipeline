"""Quality checks must stop a bad batch, and the run log must say so."""

from __future__ import annotations

from datetime import timedelta

import pytest

from pipeline.jobs import run_incremental
from pipeline.quality import QualityCheckFailed
from tests.conftest import insert_article, utc

H10, H11, H12 = utc(2026, 10, 30, 10), utc(2026, 10, 30, 11), utc(2026, 10, 30, 12)


def test_a_changed_created_at_is_caught_as_a_duplicate(settings, source, warehouse_conn):
    """The design assumes created_at never changes (it is the partition key).
    If the source breaks that assumption, the same id would be stored twice;
    the run must fail loudly instead of publishing double counts."""
    article = insert_article(source, created_at=H10)
    run_incremental(settings, H10, H11)
    source.execute(
        "UPDATE articles SET created_at = %s, updated_at = %s WHERE id = %s",
        (utc(2025, 1, 1), H11 + timedelta(minutes=5), article),
    )

    with pytest.raises(QualityCheckFailed, match="no_duplicate_ids"):
        run_incremental(settings, H11, H12)

    status, error = warehouse_conn.execute(
        "SELECT status, error FROM meta.etl_run_log WHERE batch_id = 'hourly__20261030T1100Z'"
    ).fetchone()
    assert status == "failed"
    assert "no_duplicate_ids" in error
