"""
DuckDB DDL and incremental DML for the raw layer.

Tables managed:
    raw.cdc_events      — one row per CDC event, keyed on uuid
    raw.ingested_files  — one row per processed source file, drives incremental cutoff
    raw.ingestion_errors — dead-letter table; one row per skipped event

Idempotency contract:
    insert_batch() uses DELETE WHERE uuid IN (batch) + INSERT.
    Re-processing the same file replaces existing rows rather than skipping them.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import duckdb


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.cdc_events (
            uuid             VARCHAR PRIMARY KEY,
            source_timestamp TIMESTAMPTZ,
            read_timestamp   TIMESTAMPTZ,
            change_type      VARCHAR,
            payload          JSON,
            raw_event        JSON,
            ingested_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingested_files (
            file_path         VARCHAR PRIMARY KEY,
            last_modified_utc TIMESTAMPTZ,
            status            VARCHAR,
            ingested_at       TIMESTAMPTZ DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingestion_errors (
            file_path   VARCHAR,
            raw_line    TEXT,
            error_type  VARCHAR,
            error_msg   TEXT,
            ingested_at TIMESTAMPTZ DEFAULT now()
        )
    """)


def get_cutoff(conn: duckdb.DuckDBPyConnection, lookback_hours: int | None = None) -> datetime:
    if lookback_hours is None:
        lookback_hours = int(os.getenv("TAXFIX_LOOKBACK_HOURS", 24))

    max_ts = conn.execute("SELECT max(last_modified_utc) FROM raw.ingested_files").fetchone()[0]

    if max_ts is None:
        return datetime.min.replace(tzinfo=timezone.utc)

    if not isinstance(max_ts, datetime):
        max_ts = datetime.fromisoformat(str(max_ts))
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)

    return max_ts - timedelta(hours=lookback_hours)


def insert_batch(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    if not rows:
        return

    uuids = [r["uuid"] for r in rows]
    placeholders = ", ".join("?" for _ in uuids)
    conn.execute(f"DELETE FROM raw.cdc_events WHERE uuid IN ({placeholders})", uuids)

    conn.executemany(
        """
        INSERT INTO raw.cdc_events
            (uuid, source_timestamp, read_timestamp, change_type, payload, raw_event, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, now())
        """,
        [
            (
                r["uuid"],
                r["source_timestamp"],
                r["read_timestamp"],
                r["change_type"],
                json.dumps(r["payload"]),
                json.dumps(r["raw_event"]),
            )
            for r in rows
        ],
    )


def record_file(
    conn: duckdb.DuckDBPyConnection, file_path: str, last_modified_utc: datetime, status: str
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO raw.ingested_files (file_path, last_modified_utc, status, ingested_at)
        VALUES (?, ?, ?, now())
        """,
        [file_path, last_modified_utc, status],
    )


def record_error(
    conn: duckdb.DuckDBPyConnection,
    file_path: str,
    raw_line: str,
    error_type: str,
    error_msg: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw.ingestion_errors (file_path, raw_line, error_type, error_msg, ingested_at)
        VALUES (?, ?, ?, ?, now())
        """,
        [file_path, raw_line, error_type, error_msg],
    )
