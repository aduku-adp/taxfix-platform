import json
import os
import sys
from datetime import datetime, timezone

import duckdb
import pytest

# Make the parent package importable when running pytest from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import loader  # noqa: E402  (after sys.path patch)


@pytest.fixture
def sample_insert_event():
    return {
        "uuid": "aaaa-0001",
        "source_timestamp": "2026-02-05T16:23:00Z",
        "read_timestamp": "2026-02-05T16:52:41Z",
        "source_metadata": {"change_type": "INSERT"},
        "payload": {"_id": "user-001", "email": "alice@example.com"},
    }


@pytest.fixture
def sample_update_event():
    return {
        "uuid": "aaaa-0002",
        "source_timestamp": "2026-02-05T17:00:00Z",
        "read_timestamp": "2026-02-05T17:10:00Z",
        "source_metadata": {"change_type": "UPDATE"},
        "payload": {"_id": "user-001", "email": "alice_new@example.com"},
    }


@pytest.fixture
def sample_delete_event():
    return {
        "uuid": "aaaa-0003",
        "source_timestamp": "2026-02-05T18:00:00Z",
        "read_timestamp": "2026-02-05T18:10:00Z",
        "source_metadata": {"change_type": "DELETE"},
        "payload": {"_id": "user-001"},
    }


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    loader.ensure_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def data_dir(tmp_path, sample_insert_event, sample_update_event, sample_delete_event):
    events_dir = tmp_path / "2026" / "02" / "05" / "16" / "23"
    events_dir.mkdir(parents=True)
    jsonl_file = events_dir / "events-20260205_162300.jsonl"
    lines = [
        json.dumps(sample_insert_event),
        json.dumps(sample_update_event),
        json.dumps(sample_delete_event),
    ]
    jsonl_file.write_text("\n".join(lines) + "\n")
    return tmp_path
