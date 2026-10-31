"""Seed the source database with synthetic article history from 2016 until now.

The data is synthetic but shaped like the real problem:
  * volume grows over time (more articles in recent years than in 2016)
  * some articles were edited later (updated_at > created_at)
  * some are drafts (published_at IS NULL)
  * some are soft-deleted, and -- like the schema comment suggests -- the soft
    delete only sets deleted_at, it does NOT touch updated_at
  * content contains commas, quotes, newlines, tabs, backslashes and emoji,
    which is exactly what breaks CSV-based pipelines
  * a handful of legacy rows have published_at earlier than created_at
    (the kind of thing you find in 10-year-old data after a migration)

Usage:
    python source_db/seed.py --rows 100000
Environment:
    SOURCE_ADMIN_DSN  owner connection for the source DB (not the read-only ETL role)
"""

from __future__ import annotations

import argparse
import math
import os
import random
import time
from datetime import UTC, datetime, timedelta

import psycopg

START = datetime(2016, 1, 1, tzinfo=UTC)

WORDS = [
    "jakarta",
    "pemerintah",
    "ekonomi",
    "banjir",
    "pemilu",
    "timnas",
    "startup",
    "harga",
    "beras",
    "kereta",
    "kebijakan",
    "menteri",
    "warga",
    "sekolah",
    "kesehatan",
    "festival",
    "musik",
    "teknologi",
    "rupiah",
    "investasi",
    "transportasi",
    "bandara",
    "pelabuhan",
    "pasar",
    "umkm",
    "olahraga",
    "budaya",
]

# Strings that must survive the trip source -> parquet -> warehouse unchanged.
TRICKY_SNIPPETS = [
    'Ia berkata, "kami siap", lalu pergi.',
    "Baris pertama\nBaris kedua\n\nParagraf baru",
    "Kolom\tdipisah\ttab",
    "Path Windows C:\\Users\\redaksi\\draft.txt",
    "Emoji 🇮🇩🔥 dan aksara: é ü ñ",
    "<p>HTML <b>tebal</b> &amp; entity</p>",
    "koma, titik koma; dan 'kutip tunggal'",
]


def random_created_at(rng: random.Random, end: datetime) -> datetime:
    """Linear growth: density proportional to time since START, so sample with sqrt(u)."""
    span = (end - START).total_seconds()
    return START + timedelta(seconds=span * math.sqrt(rng.random()))


def make_content(rng: random.Random) -> str:
    n_words = rng.randint(60, 320)
    body = " ".join(rng.choice(WORDS) for _ in range(n_words))
    if rng.random() < 0.15:
        body = f"{body}\n\n{rng.choice(TRICKY_SNIPPETS)}"
    return body


def make_row(rng: random.Random, created_at: datetime, end: datetime):
    def later_than(ts: datetime, max_minutes: int) -> datetime:
        # A random moment after ts, never beyond the end of history.
        room = min(max_minutes * 60, (end - ts).total_seconds())
        return ts + timedelta(seconds=rng.uniform(0, max(room, 0)))

    title = " ".join(rng.choice(WORDS) for _ in range(rng.randint(4, 10))).capitalize()
    if rng.random() < 0.02:
        title += ' "eksklusif", wawancara'

    roll = rng.random()
    if roll < 0.05:
        published_at = None  # draft
    elif roll < 0.051:
        published_at = created_at - timedelta(days=rng.randint(1, 400))  # legacy import quirk
    else:
        published_at = later_than(created_at, 48 * 60)

    updated_at = created_at
    if rng.random() < 0.30:  # edited later
        updated_at = later_than(created_at, 60 * 24 * 30)

    deleted_at = None
    if rng.random() < 0.03:  # soft delete: only deleted_at moves
        deleted_at = later_than(updated_at, 60 * 24 * 60)

    author_id = int(rng.paretovariate(1.2)) % 800 + 1  # a few authors write a lot
    return (title, make_content(rng), published_at, author_id, created_at, updated_at, deleted_at)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dsn", default=os.environ.get("SOURCE_ADMIN_DSN"))
    parser.add_argument("--truncate", action="store_true", help="empty the table first")
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("Set SOURCE_ADMIN_DSN or pass --dsn")

    rng = random.Random(args.seed)
    # History ends two hours ago: anything newer belongs to the hourly pipeline.
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)

    started = time.monotonic()
    with psycopg.connect(args.dsn) as conn:
        if args.truncate:
            # TRUNCATE does not fire the row-delete trigger, so clear the log as well.
            conn.execute("TRUNCATE articles, articles_deleted_log RESTART IDENTITY")
        with (
            conn.cursor() as cur,
            cur.copy(
                "COPY articles (title, content, published_at, author_id, created_at, updated_at, deleted_at) FROM STDIN"
            ) as copy,
        ):
            # Sorted so that ids grow with created_at, like a real serial key.
            for created_at in sorted(random_created_at(rng, end) for _ in range(args.rows)):
                copy.write_row(make_row(rng, created_at, end))
        conn.commit()
        total, oldest, newest = conn.execute(
            "SELECT count(*), min(created_at), max(created_at) FROM articles"
        ).fetchone()

    print(
        f"seeded {args.rows:,} rows in {time.monotonic() - started:.1f}s "
        f"(table now {total:,} rows, {oldest.astimezone(UTC):%Y-%m-%d} .. "
        f"{newest.astimezone(UTC):%Y-%m-%d %H:%M} UTC)"
    )


if __name__ == "__main__":
    main()
