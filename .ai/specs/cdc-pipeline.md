# CDC Pipeline — Taxfix Case Study

## Why

Build a local data pipeline that ingests MongoDB CDC events, transforms them into analytics-ready tables in DuckDB via dbt, and answers five business questions via SQL. Required for the Data Platform Engineer case study submission.

## What

An Airflow DAG that: ingests raw CDC events into DuckDB via a Python script, then runs dbt to transform through staging → clean → snapshot layers. Business questions are answered with SQL queries against the final DuckDB tables.

## Pipeline Architecture

```
data/users/**/*.jsonl
        │
        ▼
[modules/data-ingestion/raw.py]
        │  DELETE+INSERT into raw.cdc_events (idempotent on uuid)
        ▼
[modules/dbt-taxfix] dbt run
        │  staging.stg_cdc_events   ← parse JSON, cast types
        │  clean.users              ← latest state per user, anonymized, DELETEs excluded
        ▼
[modules/dbt-taxfix] dbt test
        ▼
[modules/dbt-taxfix] dbt snapshot
        │  snapshots.users          ← SCD Type 2 full history via dbt snapshot
        ▼
[modules/dbt-taxfix] dbt test --select resource_type:snapshot
```

## Context

**Relevant files:**
- `.ai/context/case-study-requirements.md` — full requirements, data schema, business questions
- `data/users/YYYY/MM/DD/HH/mm/events-*.jsonl` — partitioned CDC JSONL source data
- `docker-compose.yaml` — existing stack (Airflow 3 CeleryExecutor + PostgreSQL 16 + Redis)
- `Dockerfile` — extends `apache/airflow:3.1.7`, installs `requirements.txt`
- `requirements.txt` — pip deps; add `duckdb` and `dbt-duckdb` here
- `modules/airflow/dags/company_etl_pipeline.py` — **DAG pattern to follow exactly**
- `.env` — env vars; `AIRFLOW_PROJ_DIR=./modules/airflow` mounts dags/logs/config/plugins

**Key fields in each CDC event:**
- `uuid` — event identifier (dedup key)
- `source_timestamp` — when change occurred in MongoDB
- `read_timestamp` — when CDC connector captured it
- `source_metadata.change_type` — `INSERT`, `UPDATE`, or `DELETE`
- `payload` — full document for INSERT/UPDATE; only `_id` for DELETE

**Key decisions already made:**
- Airflow 3.1.7 with CeleryExecutor — Docker stack already running, do not recreate
- DAGs live in `modules/airflow/dags/`; the whole repo is mounted at `/opt/airflow/repo`
- Pipeline ingestion script lives in `modules/data-ingestion/`
- dbt project lives in `modules/dbt-taxfix/` with `--profiles-dir /opt/airflow/repo/modules/dbt-taxfix`
- dbt target name: `airflow` (matches existing DAG convention)
- DuckDB file: `/opt/airflow/repo/dbs/duckdb_data/dev.duckdb` (container path); `./dbs/duckdb_data/dev.duckdb` (host)
- dbt adapter: `dbt-duckdb` (not dbt-postgres — different DB from the qam pipeline)
- Age group anonymization: `floor(age_years / 10) * 10` → label `[30-40]` (computed in dbt clean model)
- DAG task pattern: BashOperator with `cd /opt/airflow/repo/<module> && <command>`, `append_env=True`
- EmptyOperator sentinels `start` and `end` wrapping all tasks

**Schema layout in DuckDB:**

| Schema | Table | Built by | Contents |
|--------|-------|----------|----------|
| `raw` | `cdc_events` | data-ingestion Python script | All raw CDC events, one row per event |
| `staging` | `stg_cdc_events` | dbt model | Parsed + typed events (JSON fields extracted) |
| `clean` | `users` | dbt model | Current state per user; DELETEs excluded; `age_group` replaces `date_of_birth` |
| `snapshots` | `users_snapshot` | dbt snapshot | Full SCD Type 2 history of `clean.users` |

## Constraints

**Must:**
- `raw.cdc_events` must be idempotent on `uuid` (DELETE WHERE uuid IN batch + INSERT)
- CDC events applied in `source_timestamp` order per user in the clean model
- DELETE events exclude users from `clean.users`
- `date_of_birth` replaced by `age_group VARCHAR` (e.g. `[30-40]`) in `clean.users`
- Each Part 2 business question answered with a single SQL query
- README with setup instructions, design decisions, assumptions, and AI usage note

**Must not:**
- Do not modify `docker-compose.yaml` or `Dockerfile`
- Do not modify source JSONL files
- Do not add pip deps beyond `duckdb` and `dbt-duckdb`
- Do not use `PythonOperator` — use `BashOperator` following `company_etl_pipeline.py`
- Do not implement Parts 3 & 4 in code — discussion only

**Out of scope:**
- Cloud deployment (Snowflake/BigQuery) — discussion only
- Right-to-be-forgotten implementation — discussion only
- Unit tests (nice-to-have, not required)

## Resilience & Operability

Decisions for the four production concerns — implemented in the tasks below.

### Late-arriving and out-of-order events

File discovery uses the **file's UTC last-modified timestamp** (`os.path.getmtime` converted to UTC) as the cutoff signal, not `source_timestamp` from event payloads. The cutoff is `max(last_modified_utc) - LOOKBACK_HOURS` (default `24h`, env var `TAXFIX_LOOKBACK_HOURS`), read from the `raw.ingested_files` tracking table. This re-scans files modified within the lookback window on every run, catching files that were written or re-dropped after the last run.

Within dbt, `clean.users` uses `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY source_timestamp DESC)` — so even if a late event is ingested after a later event, the correct chronological latest state is produced on the next `dbt run`.

### Schema evolution

The raw layer is intentionally schema-flexible:
- `raw_event JSON` stores the complete original line — no data is ever discarded.
- `payload JSON` stores the full payload — new fields land here automatically.
- Pydantic model uses `extra='allow'` — new fields pass validation without code changes.

The dbt staging model uses explicit column extraction (`payload->>'field'`). A new field is **silently ignored** until `stg_cdc_events.sql` is updated to expose it — this is safe and deliberate. To surface a new field: add a column to the staging model and run `dbt run`. Historical values are recoverable from `raw_event`.

### Backfills and reprocessing

`raw.py` accepts a `--full-refresh` flag that sets the cutoff to `datetime.min`, causing all files to be re-scanned regardless of their `last_modified_utc`. The DELETE+INSERT pattern on `uuid` keeps the load idempotent — re-processing a file replaces existing rows rather than skipping or duplicating them.

For dbt layers: `dbt run --full-refresh` drops and recreates `staging` and `clean` tables from scratch.

To trigger a backfill via Airflow: set Airflow Variable `TAXFIX_FULL_REFRESH=true` before triggering the DAG — the `load_raw` task reads this variable and appends `--full-refresh` to the command.

### Idempotency

| Layer | Mechanism |
|-------|-----------|
| `raw.cdc_events` | DELETE WHERE uuid IN (batch) + INSERT — re-processing the same file replaces rows, never duplicates |
| `raw.ingested_files` | INSERT OR REPLACE on `file_path` — file tracking record is always up to date |
| `staging` / `clean` | dbt `materialized='table'` — each run drops and fully rebuilds from raw |
| `snapshots.users_snapshot` | dbt snapshot is append-only; re-running closes and reopens the same record, never duplicates |
| Airflow DAG | `catchup=False` — no automatic historical backfill on deploy |

## Tasks

### T1: Dependencies + scaffold

**Do:**
- Add `duckdb`, `dbt-duckdb`, and `pytest` to `requirements.txt`
- Add to both `.env-template` and `.env`:
  ```
  TAXFIX_DB_PATH=/opt/airflow/repo/dbs/duckdb_data/dev.duckdb
  TAXFIX_DATA_DIR=/opt/airflow/repo/data/users
  TAXFIX_LOOKBACK_HOURS=24
  TAXFIX_FULL_REFRESH=
  ```
- Create directory `dbs/duckdb_data/` with a `.gitkeep`; add `dbs/duckdb_data/*.duckdb` to `.gitignore`
- Create stub files: `modules/data-ingestion/__init__.py`, `modules/data-ingestion/raw.py`, `modules/data-ingestion/loader.py`, `modules/data-ingestion/validation_models.py`, `modules/data-ingestion/business_rules.py`
- Initialize dbt project in `modules/dbt-taxfix/` (`dbt init` or manually):
  - `dbt_project.yml` — project name `dbt_taxfix`, models path `models/`, snapshots path `snapshots/`
  - `profiles.yml` — profile `dbt_taxfix`, target `airflow`, adapter `duckdb`, path `"{{ env_var('TAXFIX_DB_PATH') }}"`
  - `models/sources.yml` — declare source `raw`, table `cdc_events`
  - Empty dirs: `models/staging/`, `models/clean/`, `snapshots/`

**Files:** `requirements.txt`, `.env-template`, `.env`, `.gitignore`, `dbs/duckdb_data/.gitkeep`, `modules/data-ingestion/`, `modules/dbt-taxfix/`

**Verify:** `docker compose build` succeeds; inside container `dbt debug --profiles-dir /opt/airflow/repo/modules/dbt-taxfix --project-dir /opt/airflow/repo/modules/dbt-taxfix` reports connection OK

---

### T2: Raw layer — incremental JSONL ingestion with validation

**Pipeline stages:** Discover files → Validate schema → Validate business rules → Load to DuckDB

**Module structure** (all in `modules/data-ingestion/`):

| File | Responsibility |
|------|----------------|
| `raw.py` | CLI entrypoint, pipeline orchestrator |
| `loader.py` | DuckDB DDL + incremental DML |
| `validation_models.py` | Pydantic schema validation for CDC events |
| `business_rules.py` | Business-rule checks on validated events |
| `tests/conftest.py` | Shared pytest fixtures (`sample_event`, `db_conn`, `data_dir`) |
| `tests/test_validation_models.py` | Unit tests for `CdcEvent` Pydantic model |
| `tests/test_business_rules.py` | Unit tests for `validate_payload` |
| `tests/test_loader.py` | Unit tests for `loader.py` (in-memory DuckDB) |
| `tests/test_raw.py` | Integration tests for full pipeline (tmp files) |

**Do:**

`loader.py`:
- Creates schema `raw` and two tables (if not exist):
  ```sql
  -- tracks raw CDC events
  raw.cdc_events (
    uuid             VARCHAR PRIMARY KEY,
    source_timestamp TIMESTAMPTZ,
    read_timestamp   TIMESTAMPTZ,
    change_type      VARCHAR,
    payload          JSON,
    raw_event        JSON,
    ingested_at      TIMESTAMPTZ DEFAULT now()
  )

  -- tracks processed source files; drives the incremental cutoff
  raw.ingested_files (
    file_path        VARCHAR PRIMARY KEY,
    last_modified_utc TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ DEFAULT now()
  )
  ```
- `get_cutoff(conn, lookback_hours: int) -> datetime` — returns `max(last_modified_utc) - timedelta(hours=lookback_hours)` from `raw.ingested_files`; returns `datetime.min` if table is empty. `lookback_hours` defaults to `int(os.getenv('TAXFIX_LOOKBACK_HOURS', 24))`.
- `insert_batch(conn, rows)` — idempotent DELETE + INSERT on `uuid`:
  1. `DELETE FROM raw.cdc_events WHERE uuid IN (<batch_uuids>)`
  2. `INSERT INTO raw.cdc_events VALUES (...)`
  Re-processing a file replaces existing rows with the latest data rather than skipping them.
- `record_file(conn, file_path, last_modified_utc)` — `INSERT OR REPLACE INTO raw.ingested_files` — upserts the file tracking record after a successful batch load.

`validation_models.py` (Pydantic):
- `CdcEvent` model — required fields: `uuid` (str), `source_timestamp` (datetime), `read_timestamp` (datetime), `source_metadata.change_type` (literal `INSERT`|`UPDATE`|`DELETE`), `payload` (dict)
- `model_config = ConfigDict(extra='allow')` — unknown fields pass validation silently (schema evolution safe)
- Validation errors captured and counted, not raised

`business_rules.py`:
- `validate_payload(event: CdcEvent) -> list[str]` — returns list of violations:
  - INSERT/UPDATE: `payload` must contain `_id` plus at least one other field (full document expected)
  - DELETE: `payload` must contain only `_id`
- Events with violations are skipped and counted; pipeline does not crash

`raw.py`:
- CLI: `python raw.py <db_path> <data_dir> [--full-refresh]`
- `--full-refresh`: sets cutoff to `datetime.min`; also triggered when env var `TAXFIX_FULL_REFRESH=true`
- Normal run:
  1. Reads cutoff from `loader.get_cutoff()` (file mtime-based, with lookback)
  2. Globs all `events-*.jsonl` under `data_dir` recursively
  3. For each file: reads `os.path.getmtime(file)` converted to UTC; **skips file if `last_modified_utc < cutoff`**
  4. For each event in an eligible file: parse JSON → Pydantic validation → business rule check → add valid events to batch
  5. Calls `loader.insert_batch(conn, batch)` per file
  6. Calls `loader.record_file(conn, file_path, last_modified_utc)` after each successful file load
- Prints summary: files scanned, files loaded, files skipped (too old), events loaded, events skipped (schema error / rule violation)

**Also create `modules/data-ingestion/tests/` with:**

`tests/conftest.py` — shared fixtures used across all test files:
- `sample_insert_event` — dict for a valid INSERT event with all required fields
- `sample_update_event` — dict for a valid UPDATE event
- `sample_delete_event` — dict for a valid DELETE event (only `_id` in payload)
- `db_conn` — in-memory DuckDB connection with schema initialised via `loader.ensure_schema()`; scoped to function (fresh per test)
- `data_dir` — `tmp_path`-based directory fixture pre-populated with one valid JSONL file

`tests/test_validation_models.py`:
- `test_valid_insert_event` — valid INSERT payload parses without error; all required fields present
- `test_valid_update_event` — valid UPDATE payload parses correctly
- `test_valid_delete_event` — valid DELETE payload (only `_id`) parses correctly
- `test_missing_uuid_raises` — event without `uuid` raises `ValidationError`
- `test_missing_source_timestamp_raises` — event without `source_timestamp` raises `ValidationError`
- `test_invalid_change_type_raises` — `change_type="UPSERT"` raises `ValidationError`
- `test_unknown_field_allowed` — event with an extra top-level field (e.g. `"new_field": "x"`) parses successfully (schema evolution)
- `test_source_timestamp_parsed_as_datetime` — `source_timestamp` is a `datetime` instance after parsing

`tests/test_business_rules.py`:
- `test_insert_with_full_payload_passes` — INSERT with `_id` + other fields → empty violations list
- `test_update_with_full_payload_passes` — UPDATE with `_id` + other fields → empty violations list
- `test_insert_with_id_only_fails` — INSERT with only `_id` in payload → violation returned
- `test_delete_with_id_only_passes` — DELETE with only `_id` → empty violations list
- `test_delete_with_extra_fields_fails` — DELETE with `_id` + extra field → violation returned
- `test_multiple_violations_returned` — event triggering more than one rule → all violations in list

`tests/test_loader.py` (uses `duckdb.connect(':memory:')`):
- `test_create_schema_and_tables` — `ensure_schema(conn)` creates both `raw.cdc_events` and `raw.ingested_files`; running twice does not raise
- `test_get_cutoff_empty_table` — `get_cutoff(conn, lookback_hours=24)` returns `datetime.min` when `raw.ingested_files` is empty
- `test_get_cutoff_with_data` — after calling `record_file(conn, 'f', T)`, `get_cutoff(conn, lookback_hours=0)` returns `T` exactly
- `test_get_cutoff_applies_lookback` — after `record_file(conn, 'f', T)`, `get_cutoff(conn, lookback_hours=24)` returns `T - 24h`
- `test_insert_batch_inserts_rows` — `insert_batch(conn, [row])` → `raw.cdc_events` has 1 row
- `test_insert_batch_replaces_on_duplicate_uuid` — `insert_batch` called twice with same `uuid` but different `change_type` → row count remains 1 and `change_type` reflects the second call (DELETE+INSERT replaces, unlike INSERT OR IGNORE which would preserve the first)
- `test_insert_batch_mixed_new_and_duplicate` — batch with 2 new + 1 existing uuid → row count increases by 2; existing row is replaced
- `test_record_file_inserts_row` — `record_file(conn, 'path/f.jsonl', T)` → `raw.ingested_files` has 1 row
- `test_record_file_upserts_on_repeat` — calling `record_file` twice with the same path but different `last_modified_utc` → still 1 row with the updated timestamp

`tests/test_raw.py` (uses `tmp_path` fixture for temp DB + temp JSONL files):
- `test_full_pipeline_loads_events` — write 3 valid JSONL events to a temp file, run pipeline → `raw.cdc_events` has 3 rows; `raw.ingested_files` has 1 row for that file
- `test_incremental_skips_old_files` — seed `raw.ingested_files` with a recent cutoff T; create a JSONL file and set its mtime to before T → file is skipped, 0 new rows loaded
- `test_incremental_loads_new_files` — seed cutoff at T; create a JSONL file with mtime after T → file is loaded, rows appear in `raw.cdc_events`
- `test_delete_insert_replaces_existing_rows` — load a file once; modify an event's `change_type` in the same file (same uuid); load again → row reflects the new `change_type`, count unchanged
- `test_full_refresh_reloads_all` — run once, then run with `--full-refresh` → row count unchanged (all rows replaced in place via DELETE+INSERT)
- `test_schema_error_skipped` — JSONL line missing `uuid` → loaded count is 0, error count is 1, no exception raised
- `test_business_rule_violation_skipped` — DELETE event with extra payload fields → loaded count is 0, violation count is 1
- `test_summary_printed` (capsys) — summary includes "files scanned", "files loaded", "files skipped", "events loaded", "skipped"

**Files:** `modules/data-ingestion/raw.py`, `modules/data-ingestion/loader.py`, `modules/data-ingestion/validation_models.py`, `modules/data-ingestion/business_rules.py`, `modules/data-ingestion/tests/__init__.py`, `modules/data-ingestion/tests/conftest.py`, `modules/data-ingestion/tests/test_validation_models.py`, `modules/data-ingestion/tests/test_business_rules.py`, `modules/data-ingestion/tests/test_loader.py`, `modules/data-ingestion/tests/test_raw.py`

**Verify:**
- `pytest -q modules/data-ingestion/tests` — all tests pass, 0 failures
- First run: `python modules/data-ingestion/raw.py dbs/duckdb_data/dev.duckdb data/users` → non-zero row count in `raw.cdc_events`; `raw.ingested_files` populated with one row per processed file
- Second run (idempotency): `raw.cdc_events` row count unchanged; `raw.ingested_files` rows updated; summary shows 0 files loaded
- Full-refresh: `python modules/data-ingestion/raw.py dbs/duckdb_data/dev.duckdb data/users --full-refresh` → same final row count (rows replaced via DELETE+INSERT, not added)
- Manual: modify an event field in a previously loaded JSONL file, re-run → the row in `raw.cdc_events` reflects the change (confirms DELETE+INSERT over INSERT OR IGNORE)

---

### T3: dbt staging + clean models

**Do:**
- `models/staging/stg_cdc_events.sql` — reads from `{{ source('raw', 'cdc_events') }}`, extracts JSON fields into typed columns:
  - `user_id` (from `payload->>'_id'`), `change_type`, `source_timestamp`, `email`, `first_name`, `last_name`, `country`, `date_of_birth`, `created_at`
  - Include INSERT, UPDATE, **and DELETE** rows — pass all change types through to staging so the clean model can handle DELETE exclusion with full context
  - **Pre-capture window edge case:** some users have no INSERT in the dataset — their first event is an UPDATE (they existed before the capture window). The staging model includes these UPDATE rows as-is; the clean model's window function treats the latest UPDATE as ground truth for these users. Document this assumption in `schema.yml` description.
- `models/clean/users.sql` — reads from `{{ ref('stg_cdc_events') }}`:
  - **Key assumption (document in schema.yml):** MongoDB CDC provides the **full document** in every INSERT and UPDATE payload — not a partial diff. This means "applying events in chronological order" reduces to **last-write-wins on `source_timestamp`**: the latest non-DELETE event already contains the complete current state. No incremental folding of events is needed.
  - Uses `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY source_timestamp DESC)` to identify the latest event per user; selects only `rn = 1`
  - Filters out users whose latest event is a DELETE (`change_type = 'DELETE'`) — these users are absent from `clean.users`
  - Edge cases covered by this approach:
    - INSERT → UPDATE(s) → UPDATE: latest UPDATE payload is current state ✓
    - INSERT → UPDATE → DELETE: user excluded ✓
    - INSERT → DELETE → INSERT: second INSERT is latest, user present ✓
    - UPDATE only (pre-capture window): latest UPDATE treated as ground truth ✓
  - Replaces `date_of_birth` with `age_group`: `'[' || (floor(date_diff('year', date_of_birth, current_date) / 10) * 10)::int || '-' || (floor(date_diff('year', date_of_birth, current_date) / 10) * 10 + 10)::int || ']'`
  - **Sensitive fields (optional req. 5):** create VIEW `clean.users_public` that omits `first_name`, `last_name`, and raw `email`; exposes `email_domain` (`split_part(email, '@', 2)`) instead. Document in `schema.yml` using dbt `meta: {sensitivity: pii}` tags on `first_name`, `last_name`, `email`.
- `models/staging/schema.yml` and `models/clean/schema.yml` — basic not_null + unique tests on primary keys
- Set `schema` config in `dbt_project.yml`: staging → `staging`, clean → `clean`
- Set `materialized='table'` for both staging and clean models in `dbt_project.yml` — full rebuild from `raw` on every run (idempotent; same raw input always produces same output)

**Custom dbt tests** (SQL files in `tests/`, referenced from `models/clean/schema.yml`):

`tests/assert_no_deleted_users.sql` — fails if any `user_id` in `clean.users` has a DELETE as its latest event in `raw.cdc_events`:
```sql
-- returns rows on failure (dbt custom test contract)
with latest_events as (
    select
        json_extract_string(payload, '$._id') as user_id,
        change_type,
        row_number() over (
            partition by json_extract_string(payload, '$._id')
            order by source_timestamp desc
        ) as rn
    from {{ source('raw', 'cdc_events') }}
),
deleted_users as (
    select user_id from latest_events where rn = 1 and change_type = 'DELETE'
)
select u.user_id
from {{ ref('users') }} u
inner join deleted_users d on u.user_id = d.user_id
```

`tests/assert_age_group_format.sql` — fails if any `age_group` does not match `[NN-NN]`:
```sql
select user_id
from {{ ref('users') }}
where not regexp_matches(age_group, '^\[\d+-\d+\]$')
   or age_group is null
```

`tests/assert_age_group_width_is_ten.sql` — fails if upper bound − lower bound ≠ 10:
```sql
select user_id
from {{ ref('users') }}
where cast(regexp_extract(age_group, '\[(\d+)-\d+\]', 1) as int)
    + 10
    != cast(regexp_extract(age_group, '\[\d+-(\d+)\]', 1) as int)
```

`tests/assert_email_contains_at.sql` — fails if any email is missing `@` or has more than one:
```sql
select user_id
from {{ ref('users') }}
where email is null
   or length(email) - length(replace(email, '@', '')) != 1
```

`tests/assert_user_count_matches_raw_active.sql` — fails if `clean.users` count differs from the count of raw user_ids whose latest event is not DELETE:
```sql
with raw_active as (
    select json_extract_string(payload, '$._id') as user_id
    from {{ source('raw', 'cdc_events') }}
    qualify row_number() over (
        partition by json_extract_string(payload, '$._id')
        order by source_timestamp desc
    ) = 1
    and change_type != 'DELETE'
),
counts as (
    select
        (select count(*) from {{ ref('users') }})          as clean_count,
        (select count(*) from raw_active)                  as raw_active_count
)
select * from counts where clean_count != raw_active_count
```

`tests/assert_all_users_have_raw_source.sql` — fails if any `user_id` in `clean.users` has no corresponding row in `raw.cdc_events`:
```sql
select u.user_id
from {{ ref('users') }} u
left join {{ source('raw', 'cdc_events') }} r
    on u.user_id = json_extract_string(r.payload, '$._id')
where r.uuid is null
```

Wire all six custom tests into `models/clean/schema.yml` under the `users` model:
```yaml
models:
  - name: users
    tests:
      - assert_no_deleted_users
      - assert_age_group_format
      - assert_age_group_width_is_ten
      - assert_email_contains_at
      - assert_user_count_matches_raw_active
      - assert_all_users_have_raw_source
    columns:
      - name: user_id
        tests: [not_null, unique]
      - name: age_group
        tests: [not_null]
      - name: email
        tests: [not_null]
```

**Files:** `modules/dbt_taxfix/models/staging/stg_cdc_events.sql`, `models/clean/users.sql`, `models/staging/schema.yml`, `models/clean/schema.yml`, `tests/assert_no_deleted_users.sql`, `tests/assert_age_group_format.sql`, `tests/assert_age_group_width_is_ten.sql`, `tests/assert_email_contains_at.sql`, `tests/assert_user_count_matches_raw_active.sql`, `tests/assert_all_users_have_raw_source.sql`, `dbt_project.yml`

**Verify:**
- `dbt run --profiles-dir ... --project-dir ...` — succeeds; `SELECT count(*) FROM clean.users` is non-zero
- `dbt test --profiles-dir ... --project-dir ... --exclude resource_type:snapshot` — all tests pass including all 6 custom tests
- Manual: temporarily remove the DELETE filter from `users.sql`, re-run `dbt run && dbt test` → `assert_no_deleted_users` and `assert_user_count_matches_raw_active` fail; restore filter → all pass

---

### T4: dbt snapshot

**Do:**
- `snapshots/users_snapshot.sql` — dbt snapshot on `clean.users`:
  ```sql
  {% snapshot users_snapshot %}
  {{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='user_id',
      check_cols='all',
    )
  }}
  select * from {{ ref('users') }}
  {% endsnapshot %}
  ```
- Add snapshot test in `snapshots/schema.yml` — unique + not_null on `user_id`

**Files:** `modules/dbt-taxfix/snapshots/users_snapshot.sql`, `snapshots/schema.yml`

**Verify:** `dbt snapshot --profiles-dir ... --select users_snapshot` succeeds; `SELECT count(*) FROM snapshots.users_snapshot` is non-zero; running twice adds rows with `dbt_valid_to` populated for changed records

---

### T5: Airflow DAG

**Do:**
- Create `modules/airflow/dags/cdc_pipeline.py` following `company_etl_pipeline.py` exactly:
  - DAG id `cdc_pipeline`, `schedule="@daily"`, `catchup=False`, `tags=["cdc", "duckdb", "dbt"]`
  - `DB_ENV` dict with `TAXFIX_DB_PATH`, `TAXFIX_DATA_DIR`, `TAXFIX_LOOKBACK_HOURS`, and `TAXFIX_FULL_REFRESH` (default empty) values
  - Six tasks in sequence:
    1. `start` — `EmptyOperator`
    2. `load_raw` — `BashOperator`: `cd /opt/airflow/repo/modules/data-ingestion && python raw.py $TAXFIX_DB_PATH $TAXFIX_DATA_DIR ${TAXFIX_FULL_REFRESH:+--full-refresh}`
    3. `run_dbt_models` — `BashOperator`: `cd /opt/airflow/repo/modules/dbt_taxfix && dbt run --profiles-dir /opt/airflow/repo/modules/dbt_taxfix --target airflow ${TAXFIX_FULL_REFRESH:+--full-refresh}`
    4. `run_dbt_model_tests` — `BashOperator`: `dbt test ... --exclude resource_type:snapshot`
    5. `run_dbt_snapshot` — `BashOperator`: `dbt snapshot ... --select users_snapshot`
    6. `run_dbt_snapshot_tests` — `BashOperator`: `dbt test ... --select resource_type:snapshot`
    7. `end` — `EmptyOperator`
  - Chain: `start >> load_raw >> run_dbt_models >> run_dbt_model_tests >> run_dbt_snapshot >> run_dbt_snapshot_tests >> end`

**Files:** `modules/airflow/dags/cdc_pipeline.py`

**Verify:** DAG visible in Airflow UI with no import errors; manual trigger completes all tasks green; re-triggering is idempotent

---

### T6: Business queries + README

**Do:**
- Create `modules/data-ingestion/queries/business_questions.sql` with five labelled queries against DuckDB:
  - Q1: Count of active users — `SELECT count(*) FROM clean.users`
  - Q2: % with `@gmail.com` — filter `email ILIKE '%@gmail.com'`
  - Q3: Top 3 countries by Gmail user count — `GROUP BY country ORDER BY count DESC LIMIT 3`
  - Q4: Users who changed email + top 5 domain transitions — window over `raw.cdc_events` ordered by `source_timestamp`, compare consecutive emails per `user_id`
  - Q5: Avg minutes first→last event for users with >1 event — `date_diff('minute', min(source_timestamp), max(source_timestamp))`
- Create `modules/data-ingestion/queries/run_queries.py` — takes `db_path` as `sys.argv[1]`, runs each query, prints label + tabular result

**Files:** `modules/data-ingestion/queries/business_questions.sql`, `modules/data-ingestion/queries/run_queries.py`

**Verify:** `python modules/data-ingestion/queries/run_queries.py dbs/duckdb_data/dev.duckdb` prints results for all 5 queries without error; Q1 is a non-zero integer; Q2 is between 0 and 100

---

### T7: Module READMEs + global README

**Do:**
- Create `modules/data-ingestion/README.md`:
  - What it does (incremental CDC JSONL ingestion into DuckDB `raw.cdc_events` with Pydantic schema validation and business rules)
  - Folder structure: `raw.py`, `loader.py`, `validation_models.py`, `business_rules.py`, `queries/`
  - How to run locally: `python raw.py <db_path> <data_dir>`
  - How to run queries: `python queries/run_queries.py <db_path>`
  - Incremental behavior: cutoff = `max(last_modified_utc) - TAXFIX_LOOKBACK_HOURS` from `raw.ingested_files`; files with `mtime < cutoff` skipped; `--full-refresh` resets cutoff to `datetime.min`
  - Late-arriving events: lookback window re-scans recently modified files; DELETE+INSERT on `uuid` ensures re-processed files replace stale rows
  - Schema evolution: `raw_event JSON` preserves all fields; Pydantic `extra='allow'`; new payload fields land in `payload JSON` and are available for retrospective extraction; dbt staging needs explicit column addition to expose them
  - Backfills: `--full-refresh` flag (or `TAXFIX_FULL_REFRESH=true` env var) overrides cutoff; `dbt run --full-refresh` rebuilds all models from scratch
  - Idempotency: DELETE+INSERT on `uuid` in raw; `raw.ingested_files` INSERT OR REPLACE; dbt `materialized='table'` full rebuild per run
  - Pre-capture window assumption: users with no INSERT event (first event is UPDATE) are treated as valid current-state users — their latest UPDATE is ground truth
  - Validation: schema (Pydantic required fields + types), business rules (INSERT/UPDATE full payload, DELETE `_id` only), skip-not-crash policy
  - Design decisions: raw layer contract, DELETE handling, anonymization formula (applied in dbt), sensitive fields (`first_name`, `last_name`, `email` via `clean.users_public`)
  - Assumptions: pre-capture-window records, `source_timestamp` ordering, payload schema is additive going forward
  - Common failure modes: malformed JSON lines, missing `uuid`, `dbs/duckdb_data/` directory missing, DB connection not found
  - How to run tests: `pytest -q modules/data-ingestion/tests`
  - Business question results (Q1–Q5 with actual output values — run after pipeline executes)
  - AI usage note
- Create `modules/dbt_taxfix/README.md`:
  - What it does (staging → clean → snapshot transformation on DuckDB)
  - Layer descriptions: `staging.stg_cdc_events`, `clean.users`, `snapshots.users_snapshot`
  - How to run locally: `dbt run / dbt test / dbt snapshot` with `--profiles-dir . --target dev`
  - How to serve docs: `dbt docs generate && dbt docs serve --port 8001`
- Create `modules/airflow/README.md`:
  - DAGs available: `company_etl_pipeline`, `cdc_pipeline`
  - How to trigger: Airflow UI at http://localhost:8080 (airflow/airflow)
  - Config location: `modules/airflow/config/airflow.cfg`
- Update top-level `README.md`:
  - Add `modules/data-ingestion/README.md` and `modules/dbt_taxfix/README.md` to the Module documentation list
  - Add `cdc_pipeline` under "Trigger a data ingestion" with credentials note
  - Add CDC data location: `data/users/YYYY/MM/DD/HH/mm/events-*.jsonl`
  - Add query runner instructions: `python modules/data-ingestion/queries/run_queries.py data/taxfix.db`

**Files:** `modules/data-ingestion/README.md`, `modules/dbt_taxfix/README.md`, `modules/airflow/README.md`, `README.md`

**Verify:** All four files exist and are non-empty; top-level README module documentation links all resolve; `modules/data-ingestion/README.md` contains actual Q1–Q5 results (not placeholders)

---

## Done

- [ ] `pytest -q modules/data-ingestion/tests` — all tests pass, 0 failures
- [ ] `cd tools && ./clean_deploy_stack.sh` — no errors; Airflow UI at http://localhost:8080 (airflow/airflow)
- [ ] `cdc_pipeline` DAG visible in Airflow UI with no import errors
- [ ] Manual DAG trigger — all 6 tasks complete green
- [ ] `SELECT count(*) FROM raw.cdc_events` — non-zero
- [ ] `SELECT count(*) FROM clean.users` — non-zero; no `date_of_birth` column; `age_group` in `[XX-YY]` format
- [ ] `SELECT count(*) FROM snapshots.users_snapshot` — non-zero
- [ ] Re-running the DAG is idempotent
- [ ] `python modules/data-ingestion/queries/run_queries.py dbs/duckdb_data/dev.duckdb` — all 5 queries return results
- [ ] `modules/data-ingestion/README.md`, `modules/dbt_taxfix/README.md`, `modules/airflow/README.md` all exist and are complete
- [ ] Top-level README module links all resolve
