"""Simulate an hour of newsroom activity on the source database.

Each run does a mix of:
  * inserts          new articles (some published, some drafts)
  * edits            title changes on random articles, including very old ones,
                     so the warehouse merge has to touch old partitions
  * publish moves    published_at changes (a draft goes live or a story is rescheduled),
                     which moves the article to another day in the daily mart
  * soft deletes     deleted_at is set, updated_at is NOT touched (the schema trap)
  * hard deletes     DELETE FROM articles (bonus 2)

--at lets you stamp the activity with a fixed time so you can then run the
pipeline for exactly that hour, e.g.

    python source_db/simulate.py --at 2026-10-30T09:30:00Z
    python -m pipeline incremental --start 2026-10-30T09:00Z --end 2026-10-30T10:00Z
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import UTC, datetime

import psycopg


def parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def pick_ids(conn, n: int, rng: random.Random, where: str = "TRUE") -> list[int]:
    if n <= 0:
        return []
    lo, hi = conn.execute(f"SELECT min(id), max(id) FROM articles WHERE {where}").fetchone()
    if lo is None:
        return []
    candidates = {rng.randint(lo, hi) for _ in range(n * 4)}
    rows = conn.execute(
        f"SELECT id FROM articles WHERE id = ANY(%s) AND {where} LIMIT %s", (list(candidates), n)
    ).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", default=os.environ.get("SOURCE_ADMIN_DSN"))
    parser.add_argument("--at", type=parse_ts, default=None, help="timestamp to stamp changes with (default: now)")
    parser.add_argument("--inserts", type=int, default=400)
    parser.add_argument("--edits", type=int, default=200)
    parser.add_argument("--publish-moves", type=int, default=30)
    parser.add_argument("--soft-deletes", type=int, default=40)
    parser.add_argument("--hard-deletes", type=int, default=25)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("Set SOURCE_ADMIN_DSN or pass --dsn")

    rng = random.Random(args.seed)
    at = args.at or datetime.now(UTC)

    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (
                        f"Berita terkini #{i}",
                        "Isi berita, dengan koma\ndan baris baru.",
                        at if rng.random() < 0.8 else None,
                        rng.randint(1, 800),
                        at,
                        at,
                    )
                    for i in range(args.inserts)
                ],
            )

        edited = pick_ids(conn, args.edits, rng, "deleted_at IS NULL")
        conn.execute(
            "UPDATE articles SET title = title || ' (diperbarui)', updated_at = %s WHERE id = ANY(%s)",
            (at, edited),
        )

        moved = pick_ids(conn, args.publish_moves, rng, "deleted_at IS NULL")
        conn.execute("UPDATE articles SET published_at = %s, updated_at = %s WHERE id = ANY(%s)", (at, at, moved))

        # The trap: the app only fills deleted_at. updated_at keeps its old value.
        soft = pick_ids(conn, args.soft_deletes, rng, "deleted_at IS NULL")
        conn.execute("UPDATE articles SET deleted_at = %s WHERE id = ANY(%s)", (at, soft))

        hard = pick_ids(conn, args.hard_deletes, rng)
        max_log_id = conn.execute("SELECT coalesce(max(log_id), 0) FROM articles_deleted_log").fetchone()[0]
        conn.execute("DELETE FROM articles WHERE id = ANY(%s)", (hard,))
        if args.at:
            # Simulation only: the trigger stamps now(); align it with --at so the
            # tombstones fall into the same hour as the rest of the activity.
            conn.execute("UPDATE articles_deleted_log SET deleted_at = %s WHERE log_id > %s", (at, max_log_id))
        conn.commit()

    print(f"activity stamped at {at.isoformat()}")
    print(f"  inserted       {args.inserts}")
    print(f"  edited         {len(edited)}  e.g. {sorted(edited)[:5]}")
    print(f"  publish moves  {len(moved)}  e.g. {sorted(moved)[:5]}")
    print(f"  soft-deleted   {len(soft)}  e.g. {sorted(soft)[:5]}")
    print(f"  hard-deleted   {len(hard)}  e.g. {sorted(hard)[:5]}")


if __name__ == "__main__":
    main()
