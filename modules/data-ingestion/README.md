# Data Ingestion

Incrementally ingests MongoDB CDC events from partitioned JSONL files into a DuckDB raw layer, with Pydantic schema validation, business-rule checks, and a dead-letter table for skipped events.

## Folder structure

```
modules/data-ingestion/
  raw.py               # CLI entrypoint + orchestrator
  loader.py            # DuckDB DDL + incremental DML
  validation_models.py # Pydantic schema for CDC events
  business_rules.py    # Business-rule checks per change_type
  queries/
    business_questions.sql  # Q1–Q5 labelled SQL queries
  tests/
    conftest.py             # Shared pytest fixtures
    test_validation_models.py
    test_business_rules.py
    test_loader.py
    test_raw.py
```

## How to run locally

```bash
cd modules/data-ingestion
python raw.py <db_path> <data_dir>             # incremental
python raw.py <db_path> <data_dir> --full-refresh  # backfill all files
```

Environment variables override CLI arguments when set:
- `TAXFIX_DB_PATH` — path to the DuckDB file
- `TAXFIX_DATA_DIR` — root directory of partitioned JSONL files
- `TAXFIX_LOOKBACK_HOURS` — re-scan window (default 24)
- `TAXFIX_FULL_REFRESH` — any non-empty value triggers full backfill

## Incremental behaviour

Cutoff = `max(last_modified_utc) - TAXFIX_LOOKBACK_HOURS` from `raw.ingested_files`.
Files whose mtime is before the cutoff are skipped.
`--full-refresh` resets the cutoff to `datetime.min`, forcing all files to be reprocessed.

## Late-arriving events

The lookback window (`TAXFIX_LOOKBACK_HOURS`, default 24 h) re-scans recently modified
files so late-arriving events are picked up on the next run.
DELETE + INSERT on `uuid` ensures re-processed files replace stale rows without duplicates.

## Schema evolution

`raw_event JSON` stores the full raw event. `payload JSON` is schema-flexible; new fields
in the source payload land in the JSON column and are available for retrospective extraction.
Pydantic is configured with `extra='allow'` so unknown fields are passed through rather
than rejected. Adding a new field to the dbt staging model exposes it downstream.

## Backfills

Set `TAXFIX_FULL_REFRESH=true` in `.env` (or pass `--full-refresh` on the CLI) to ignore
the file cutoff and reprocess everything from scratch. For dbt, run `dbt run --full-refresh`
to rebuild all tables.

## Idempotency

- **Raw layer:** DELETE WHERE uuid IN (batch) + INSERT — re-running the same file produces
  the same rows, never duplicates.
- **File tracking:** `raw.ingested_files` uses INSERT OR REPLACE — re-running updates the
  existing record.
- **dbt models:** materialized as `table` — full rebuild on every run.

## Pre-capture window assumption

Users whose first captured event is an UPDATE (no INSERT in the dataset) are treated as
valid current-state users. Their latest UPDATE is ground truth. This affects ~449 users.

## Validation

Two gates, both skip-not-crash:

| Gate | What is checked | Error type in dead-letter |
|------|----------------|--------------------------|
| Schema | Required fields (`uuid`, `source_timestamp`, `read_timestamp`, `source_metadata.change_type`, `payload`) and correct types via Pydantic | `schema_error` |
| Business rules | INSERT/UPDATE must have `_id` + at least one other field; DELETE must have only `_id` | `rule_violation` |

Skipped events are written to `raw.ingestion_errors` (one row per skipped event) with the
original raw line preserved for replay. Each skipped event is also emitted as a `WARNING`
to the console in the format:

```
WARNING: [<error_type>] <file_path>: <error_msg>
```

## Design decisions

| Concern | Decision |
|---------|----------|
| Current state | Last-write-wins on `source_timestamp` — valid because MongoDB CDC provides full documents in every INSERT/UPDATE, not diffs |
| DELETE handling | DELETE events are loaded into `raw.cdc_events` but filtered out in `clean.users`; snapshot records end via `dbt_valid_to` |
| Anonymization | `first_name`, `last_name`, `email` tagged as PII in `clean.users`; `clean.users_public` VIEW exposes only `email_domain` |
| Raw idempotency | DELETE WHERE uuid IN (batch) + INSERT — re-processing replaces rows |
| File cutoff | Based on file mtime, not event timestamp, so late-arriving files are caught by the lookback window |

## Assumptions

- `source_timestamp` ordering within a user reflects true event order.
- Payload schema is additive: new fields will not break existing Pydantic validation due to `extra='allow'`.
- Pre-capture-window users (first event is UPDATE) are treated as valid active users.

## Common failure modes

| Symptom | Likely cause |
|---------|-------------|
| `ModuleNotFoundError: duckdb` | Wrong Python environment; activate the project venv |
| `dbs/duckdb_data/ not found` | Run `mkdir -p dbs/duckdb_data` or use the `.gitkeep` directory |
| `Database not found` in DuckDB queries | Pipeline hasn't run yet; run `raw.py` first |
| Zero files loaded | All files are before the cutoff; use `--full-refresh` or check file mtimes |
| `WARNING: [schema_error] ...` in console | Malformed event; event is dead-lettered in `raw.ingestion_errors` |

## How to run tests

```bash
pytest -q modules/data-ingestion/tests
```

All 34 tests run in-memory (no external DB required).

## Business question results

Queries run against the full CDC dataset (`taxfix.duckdb` after pipeline execution):

| # | Question | Result |
|---|----------|--------|
| Q1 | Active user count | **18,997** |
| Q2 | Gmail user percentage | **30.38%** |
| Q3 | Top 3 countries by Gmail users | Germany (1,172), Switzerland (235), United Kingdom (228) |
| Q4a | Users who changed email at least once | **90** |
| Q4b | Top 5 email domain transitions | gmail.com→gmail.com (5), gmail.com→icloud.com (4), yahoo.com→outlook.com (4), outlook.com→outlook.com (4), zoho.com→gmail.com (3) |
| Q5 | Avg minutes first→last event (users with >1 event) | **2.86 min** |

Note: Q4b `gmail.com → gmail.com` represents users who changed their full email address
but stayed on the same domain.

## AI usage

Claude Code (claude-sonnet-4-6) was used throughout this module to:
- Scaffold Pydantic models, loader DDL/DML, and the raw.py orchestrator
- Design the dead-letter table and file status tracking
- Iterate on dbt model logic (age_group NULL guard, generate_schema_name macro)
- Write the business question SQL (including diagnosing the DuckDB `->>` operator-precedence bug in WHERE clauses)
- Generate pytest fixtures and unit/integration test cases

All generated code was reviewed and validated against actual data before committing.
