import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

import loader
import raw as raw_module


T = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

VALID_INSERT = {
    "uuid": "e-insert-001",
    "source_timestamp": "2026-01-01T12:00:00Z",
    "read_timestamp": "2026-01-01T12:01:00Z",
    "source_metadata": {"change_type": "INSERT"},
    "payload": {"_id": "user-001", "email": "a@b.com"},
}
VALID_UPDATE = {
    "uuid": "e-update-001",
    "source_timestamp": "2026-01-01T13:00:00Z",
    "read_timestamp": "2026-01-01T13:01:00Z",
    "source_metadata": {"change_type": "UPDATE"},
    "payload": {"_id": "user-001", "email": "a_new@b.com"},
}
VALID_DELETE = {
    "uuid": "e-delete-001",
    "source_timestamp": "2026-01-01T14:00:00Z",
    "read_timestamp": "2026-01-01T14:01:00Z",
    "source_metadata": {"change_type": "DELETE"},
    "payload": {"_id": "user-002"},
}


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def _set_mtime(path: Path, dt: datetime) -> None:
    ts = dt.timestamp()
    os.utime(str(path), (ts, ts))


def test_full_pipeline_loads_events(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT, VALID_UPDATE, VALID_DELETE])

    stats = raw_module.run(db_path, str(data_dir), full_refresh=True)

    conn = duckdb.connect(db_path)
    event_count = conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    status = conn.execute("SELECT status FROM raw.ingested_files").fetchone()[0]
    conn.close()

    assert event_count == 3
    assert status == "success"


def test_incremental_skips_old_files(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT])

    # Seed cutoff at T (future relative to file mtime we'll set)
    conn = duckdb.connect(db_path)
    loader.ensure_schema(conn)
    loader.record_file(conn, "seed", T + timedelta(hours=25), "success")
    conn.close()

    # Set file mtime before the cutoff window
    _set_mtime(f, T - timedelta(hours=2))

    stats = raw_module.run(db_path, str(data_dir), full_refresh=False)
    assert stats["files_skipped"] == 1
    assert stats["events_loaded"] == 0


def test_incremental_loads_new_files(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT])

    # Seed cutoff at T - 48h so the file (mtime=T) is within the window
    conn = duckdb.connect(db_path)
    loader.ensure_schema(conn)
    loader.record_file(conn, "seed", T - timedelta(hours=24), "success")
    conn.close()

    _set_mtime(f, T)

    stats = raw_module.run(db_path, str(data_dir), full_refresh=False)
    assert stats["events_loaded"] == 1


def test_delete_insert_replaces_existing_rows(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT])
    raw_module.run(db_path, str(data_dir), full_refresh=True)

    # Overwrite the file with the same uuid but different change_type
    modified = {**VALID_INSERT, "source_metadata": {"change_type": "UPDATE"}}
    _write_jsonl(f, [modified])
    raw_module.run(db_path, str(data_dir), full_refresh=True)

    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT change_type FROM raw.cdc_events WHERE uuid = 'e-insert-001'"
    ).fetchone()
    count = conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    conn.close()

    assert row[0] == "UPDATE"
    assert count == 1


def test_full_refresh_reloads_all(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT, VALID_UPDATE])

    raw_module.run(db_path, str(data_dir), full_refresh=True)
    conn = duckdb.connect(db_path)
    count_before = conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    conn.close()

    raw_module.run(db_path, str(data_dir), full_refresh=True)
    conn = duckdb.connect(db_path)
    count_after = conn.execute("SELECT count(*) FROM raw.cdc_events").fetchone()[0]
    conn.close()

    assert count_before == count_after == 2


def test_schema_error_skipped(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    bad_event = {
        # missing uuid
        "source_timestamp": "2026-01-01T12:00:00Z",
        "read_timestamp": "2026-01-01T12:01:00Z",
        "source_metadata": {"change_type": "INSERT"},
        "payload": {"_id": "user-x", "email": "x@x.com"},
    }
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [bad_event])

    stats = raw_module.run(db_path, str(data_dir), full_refresh=True)
    assert stats["events_loaded"] == 0
    assert stats["events_skipped_schema"] == 1
    conn = duckdb.connect(db_path)
    error_row = conn.execute("SELECT error_type FROM raw.ingestion_errors").fetchone()
    file_status = conn.execute("SELECT status FROM raw.ingested_files").fetchone()[0]
    conn.close()
    assert error_row[0] == "schema_error"
    assert file_status == "failed"


def test_business_rule_violation_skipped(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    bad_delete = {
        "uuid": "bad-delete-001",
        "source_timestamp": "2026-01-01T12:00:00Z",
        "read_timestamp": "2026-01-01T12:01:00Z",
        "source_metadata": {"change_type": "DELETE"},
        "payload": {"_id": "user-x", "extra_field": "should_not_be_here"},
    }
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [bad_delete])

    stats = raw_module.run(db_path, str(data_dir), full_refresh=True)
    assert stats["events_loaded"] == 0
    assert stats["events_skipped_rules"] == 1
    conn = duckdb.connect(db_path)
    error_row = conn.execute(
        "SELECT error_type, raw_line FROM raw.ingestion_errors"
    ).fetchone()
    file_status = conn.execute("SELECT status FROM raw.ingested_files").fetchone()[0]
    conn.close()
    assert error_row[0] == "rule_violation"
    assert "bad-delete-001" in error_row[1]
    assert file_status == "failed"


def test_partial_file_status(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    bad_event = {
        # missing uuid
        "source_timestamp": "2026-01-01T12:00:00Z",
        "read_timestamp": "2026-01-01T12:01:00Z",
        "source_metadata": {"change_type": "INSERT"},
        "payload": {"_id": "user-x", "email": "x@x.com"},
    }
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT, bad_event])

    raw_module.run(db_path, str(data_dir), full_refresh=True)
    conn = duckdb.connect(db_path)
    error_count = conn.execute("SELECT count(*) FROM raw.ingestion_errors").fetchone()[0]
    file_status = conn.execute("SELECT status FROM raw.ingested_files").fetchone()[0]
    conn.close()
    assert error_count == 1
    assert file_status == "partial"


def test_summary_printed(tmp_path, capsys):
    db_path = str(tmp_path / "test.duckdb")
    data_dir = tmp_path / "data"
    f = data_dir / "events-001.jsonl"
    _write_jsonl(f, [VALID_INSERT])

    raw_module.run(db_path, str(data_dir), full_refresh=True)
    captured = capsys.readouterr()

    assert "files scanned" in captured.out
    assert "files loaded" in captured.out
    assert "files skipped" in captured.out
    assert "events loaded" in captured.out
    assert "skipped" in captured.out
