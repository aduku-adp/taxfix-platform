"""
CDC pipeline entrypoint.

Orchestrates incremental ingestion of MongoDB CDC events from JSONL source files
into the raw DuckDB layer (raw.cdc_events).

Usage:
    python raw.py <db_path> <data_dir> [--full-refresh]

Incremental behaviour:
    Cutoff = max(last_modified_utc) - TAXFIX_LOOKBACK_HOURS from raw.ingested_files.
    Files whose mtime < cutoff are skipped. --full-refresh sets cutoff to datetime.min.

Idempotency:
    DELETE WHERE uuid IN (batch) + INSERT — re-processing a file replaces rows.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from pydantic import ValidationError

import business_rules
import loader
from validation_models import CdcEvent


def _file_mtime_utc(path: str) -> datetime:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def run(db_path: str, data_dir: str, full_refresh: bool = False) -> dict:
    conn = duckdb.connect(db_path)
    loader.ensure_schema(conn)

    if full_refresh:
        cutoff = datetime.min.replace(tzinfo=timezone.utc)
    else:
        cutoff = loader.get_cutoff(conn)

    jsonl_files = sorted(Path(data_dir).rglob("events-*.jsonl"))

    stats = {
        "files_scanned": len(jsonl_files),
        "files_loaded": 0,
        "files_skipped": 0,
        "events_loaded": 0,
        "events_skipped_schema": 0,
        "events_skipped_rules": 0,
    }

    for file_path in jsonl_files:
        mtime = _file_mtime_utc(str(file_path))
        if mtime < cutoff:
            stats["files_skipped"] += 1
            continue

        batch: list[dict] = []
        file_errors = 0
        with open(file_path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue

                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    stats["events_skipped_schema"] += 1
                    file_errors += 1
                    loader.record_error(conn, str(file_path), line, "schema_error", str(exc))
                    continue

                try:
                    event = CdcEvent.model_validate(raw)
                except ValidationError as exc:
                    stats["events_skipped_schema"] += 1
                    file_errors += 1
                    loader.record_error(conn, str(file_path), line, "schema_error", str(exc))
                    continue

                violations = business_rules.validate_payload(event)
                if violations:
                    stats["events_skipped_rules"] += 1
                    file_errors += 1
                    loader.record_error(
                        conn, str(file_path), line, "rule_violation", "; ".join(violations)
                    )
                    continue

                batch.append(
                    {
                        "uuid": event.uuid,
                        "source_timestamp": event.source_timestamp,
                        "read_timestamp": event.read_timestamp,
                        "change_type": event.source_metadata.change_type,
                        "payload": event.payload,
                        "raw_event": raw,
                    }
                )

        loader.insert_batch(conn, batch)

        if file_errors == 0:
            status = "success"
        elif batch:
            status = "partial"
        else:
            status = "failed"

        loader.record_file(conn, str(file_path), mtime, status)
        stats["events_loaded"] += len(batch)
        if status != "failed":
            stats["files_loaded"] += 1

    conn.close()

    print(
        f"files scanned: {stats['files_scanned']} | "
        f"files loaded: {stats['files_loaded']} | "
        f"files skipped: {stats['files_skipped']} | "
        f"events loaded: {stats['events_loaded']} | "
        f"skipped (schema): {stats['events_skipped_schema']} | "
        f"skipped (rules): {stats['events_skipped_rules']}"
    )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CDC JSONL events into DuckDB raw layer")
    parser.add_argument("db_path", help="Path to DuckDB file")
    parser.add_argument("data_dir", help="Root directory for partitioned JSONL files")
    parser.add_argument("--full-refresh", action="store_true", help="Re-ingest all files")
    args = parser.parse_args()

    full_refresh = args.full_refresh or bool(os.getenv("TAXFIX_FULL_REFRESH"))
    run(args.db_path, args.data_dir, full_refresh=full_refresh)


if __name__ == "__main__":
    main()
