"""Runtime settings, read from environment variables.

Keeping configuration in env vars means the same code runs from Airflow, from
the CLI and from the tests, each pointing at different databases.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    source_dsn: str
    warehouse_dsn: str
    data_dir: Path
    # How far back each hourly run looks before its own window. Covers
    # transactions that committed after the previous run read the table
    # (Postgres now() is the transaction START time) and replica lag.
    lookback: timedelta = timedelta(minutes=15)
    # Rows fetched per round trip from the source server-side cursor.
    fetch_size: int = 10_000
    # Hard stop for any single source query, so a stuck query cannot hold a
    # connection on the production replica forever.
    source_statement_timeout_ms: int = 15 * 60 * 1000
    # Reconciliation re-extracts missing rows itself only below this count;
    # above it, something bigger is wrong (e.g. backfill not run) and a human should look.
    reconcile_max_heal: int = 10_000

    @classmethod
    def from_env(cls) -> Settings:
        def required(name: str) -> str:
            value = os.environ.get(name)
            if not value:
                raise RuntimeError(f"Environment variable {name} is not set")
            return value

        return cls(
            source_dsn=required("SOURCE_DSN"),
            warehouse_dsn=required("WAREHOUSE_DSN"),
            data_dir=Path(os.environ.get("DATA_DIR", "./data")),
            lookback=timedelta(minutes=int(os.environ.get("LOOKBACK_MINUTES", "15"))),
            fetch_size=int(os.environ.get("EXTRACT_FETCH_SIZE", "10000")),
            source_statement_timeout_ms=int(os.environ.get("SOURCE_STATEMENT_TIMEOUT_MS", str(15 * 60 * 1000))),
            reconcile_max_heal=int(os.environ.get("RECONCILE_MAX_HEAL", "10000")),
        )
