from datetime import datetime, timedelta, timezone

import duckdb
import pytest

import loader


T = datetime(2026, 2, 5, 16, 0, 0, tzinfo=timezone.utc)

SAMPLE_ROW = {
    "uuid": "row-0001",
    "source_timestamp": T,
    "read_timestamp": T,
    "change_type": "INSERT",
    "payload": {"_id": "user-001", "email": "a@b.com"},
    "raw_event": {"uuid": "row-0001"},
}


def test_create_schema_and_tables(db_conn):
    # Tables already created by fixture; calling again must not raise
    loader.ensure_schema(db_conn)
    tables = {
        r[0]
        for r in db_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
    }
    assert "cdc_events" in tables
    assert "ingested_files" in tables
    assert "ingestion_errors" in tables


def test_get_cutoff_empty_table(db_conn):
    cutoff = loader.get_cutoff(db_conn, lookback_hours=24)
    assert cutoff == datetime.min.replace(tzinfo=timezone.utc)


def test_get_cutoff_with_data(db_conn):
    loader.record_file(db_conn, "f.jsonl", T, "success")
    cutoff = loader.get_cutoff(db_conn, lookback_hours=0)
    assert cutoff == T


def test_get_cutoff_applies_lookback(db_conn):
    loader.record_file(db_conn, "f.jsonl", T, "success")
    cutoff = loader.get_cutoff(db_conn, lookback_hours=24)
    assert cutoff == T - timedelta(hours=24)


def test_insert_batch_inserts_rows(db_conn):
    loader.insert_batch(db_conn, [SAMPLE_ROW])
    count = db_conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    assert count == 1


def test_insert_batch_replaces_on_duplicate_uuid(db_conn):
    loader.insert_batch(db_conn, [SAMPLE_ROW])
    updated = {**SAMPLE_ROW, "change_type": "DELETE"}
    loader.insert_batch(db_conn, [updated])
    row = db_conn.execute("SELECT change_type FROM raw.cdc_events WHERE uuid = 'row-0001'").fetchone()
    assert row[0] == "DELETE"
    count = db_conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    assert count == 1


def test_insert_batch_mixed_new_and_duplicate(db_conn):
    loader.insert_batch(db_conn, [SAMPLE_ROW])
    new_rows = [
        {**SAMPLE_ROW, "uuid": "row-0002"},
        {**SAMPLE_ROW, "uuid": "row-0003"},
        {**SAMPLE_ROW, "change_type": "UPDATE"},  # same uuid as SAMPLE_ROW
    ]
    loader.insert_batch(db_conn, new_rows)
    count = db_conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    assert count == 3


def test_record_file_inserts_row(db_conn):
    loader.record_file(db_conn, "path/f.jsonl", T, "success")
    row = db_conn.execute(
        "SELECT status FROM raw.ingested_files WHERE file_path = 'path/f.jsonl'"
    ).fetchone()
    assert row is not None
    assert row[0] == "success"


def test_record_file_upserts_on_repeat(db_conn):
    loader.record_file(db_conn, "path/f.jsonl", T, "success")
    T2 = T + timedelta(hours=1)
    loader.record_file(db_conn, "path/f.jsonl", T2, "partial")
    rows = db_conn.execute(
        "SELECT last_modified_utc, status FROM raw.ingested_files WHERE file_path = 'path/f.jsonl'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == T2
    assert rows[0][1] == "partial"


def test_record_error_inserts_row(db_conn):
    loader.record_error(db_conn, "f.jsonl", '{"bad": 1}', "schema_error", "missing uuid")
    row = db_conn.execute(
        "SELECT error_type, error_msg FROM raw.ingestion_errors"
    ).fetchone()
    assert row[0] == "schema_error"
    assert row[1] == "missing uuid"


def test_record_error_appends_multiple(db_conn):
    loader.record_error(db_conn, "f.jsonl", "line1", "schema_error", "err1")
    loader.record_error(db_conn, "f.jsonl", "line2", "rule_violation", "err2")
    count = db_conn.execute("SELECT count(*) FROM raw.ingestion_errors").fetchone()[0]
    assert count == 2
