# CLAUDE.md

## Project

Taxfix Data Platform Engineer case study. A local data pipeline that ingests MongoDB CDC events, transforms them into analytics-ready tables, and answers business questions via SQL.

**Spec:** `.ai/specs/cdc-pipeline.md`
**Requirements:** `.ai/context/case-study-requirements.md`

## Stack

| Component | Version / Notes |
|-----------|-----------------|
| Airflow | 3.1.7 (CeleryExecutor + Redis) |
| PostgreSQL | 16 (Airflow metadata DB + app DB `qam_db`) |
| DuckDB | for CDC pipeline analytics layer |
| dbt | dbt-core + dbt-postgres (qam pipeline) + dbt-duckdb (CDC pipeline, `modules/dbt_taxfix/`) |
| Python | 3.12 |

## Structure

```
modules/
  airflow/
    config/airflow.cfg
    dags/               # DAG files — mounted into Airflow container
    README.md           # DAG catalogue, how to trigger, config location
  data-ingestion/
    raw.py              # CLI entrypoint + pipeline orchestrator
    loader.py           # DuckDB DDL + incremental DML (cutoff, insert_batch, record_file)
    validation_models.py # Pydantic schema for CDC events (CdcEvent, extra='allow')
    business_rules.py   # Business-rule checks (payload completeness per change_type)
    queries/
      business_questions.sql  # Q1–Q5 labelled SQL queries
      run_queries.py          # CLI runner: prints all query results
    tests/
      conftest.py       # Shared fixtures: sample_insert_event, db_conn, data_dir
      test_validation_models.py
      test_business_rules.py
      test_loader.py    # In-memory DuckDB tests
      test_raw.py       # Integration tests (tmp_path)
    README.md           # Design decisions, assumptions, query results, AI usage
  dbt_taxfix/           # dbt project: staging → clean → snapshot (DuckDB)
    models/
      staging/          # stg_cdc_events: parse JSON, cast types, all change types
      clean/            # users: latest state per user, anonymized, DELETEs excluded
    snapshots/          # users_snapshot: SCD Type 2 (check strategy, check_cols='all')
    tests/              # Custom dbt tests (assert_no_deleted_users, assert_age_group_*, etc.)
    README.md           # Layer descriptions, how to run dbt locally, docs
dbs/
  duckdb_data/
    dev.duckdb          # DuckDB file (gitignored; directory tracked via .gitkeep)
data/
  users/YYYY/MM/DD/HH/mm/  # Partitioned CDC JSONL files (gitignored)
tools/
  sh/                   # Build and deploy shell scripts (clean_deploy_stack.sh)
  sql/                  # Index/partition DDL, postgres-init scripts
Dockerfile              # Extends apache/airflow:3.1.7
docker-compose.yaml     # Full stack: Airflow + PostgreSQL + Redis + qam-api
requirements.txt        # Extra pip deps: dbt-core, dbt-postgres, dbt-duckdb, duckdb, pytest, pydantic, …
.env-template           # Committed env var template — copy to .env
.env                    # Local env vars (gitignored)
```

## Key Conventions

**DAG pattern** (follow `modules/airflow/dags/company_etl_pipeline.py`):
- Use `BashOperator` for tasks that shell out to module scripts
- Inject DB env via `env=DB_ENV, append_env=True`
- Scripts run from `/opt/airflow/repo/<module>/` inside the container
- The whole repo is mounted at `/opt/airflow/repo` (see `docker-compose.yaml` volumes)
- Use `airflow.providers.standard.operators.*` (Airflow 3 provider namespace)
- DAG structure: `start (EmptyOperator) >> tasks >> end (EmptyOperator)`
- `catchup=False`, `schedule="@daily"`

**Environment variables:**
- DB connection vars: `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `DB_NAME`
- Airflow metadata DB: separate `airflow` database, user `airflow`
- `AIRFLOW_PROJ_DIR=./modules/airflow` — dags/logs/config/plugins mount root
- CDC pipeline vars (in `.env-template`):
  - `TAXFIX_DB_PATH=/opt/airflow/repo/dbs/duckdb_data/dev.duckdb`
  - `TAXFIX_DATA_DIR=/opt/airflow/repo/data/users`
  - `TAXFIX_LOOKBACK_HOURS=24` — re-scan window for late-arriving files
  - `TAXFIX_FULL_REFRESH=` — set to any non-empty value to trigger full backfill

**Airflow version note:** This uses Airflow 3. The executor is `CeleryExecutor`. The execution API server runs at `http://airflow-apiserver:8080/execution/`.

## Build & Run

```bash
# First time: copy env template
cp .env-template .env

# One-command build + start
cd tools && ./clean_deploy_stack.sh

# Airflow UI → http://localhost:8080  (credentials: airflow / airflow)
# dbt docs  → http://localhost:8001  (when served)
```

## Data

CDC events are in `data/users/YYYY/MM/DD/HH/mm/events-*.jsonl`. Each line is one event with fields: `uuid`, `source_timestamp`, `read_timestamp`, `source_metadata.change_type` (INSERT/UPDATE/DELETE), `payload`.

DuckDB file lives at `dbs/duckdb_data/dev.duckdb` (host) / `/opt/airflow/repo/dbs/duckdb_data/dev.duckdb` (container). The directory is tracked via `.gitkeep`; the `.duckdb` file is gitignored.

Do not modify source JSONL files.

## Module READMEs

Each module has its own README scoped to that module:

| File | Covers |
|------|--------|
| `modules/data-ingestion/README.md` | Ingestion script usage, design decisions, assumptions, query results, AI usage |
| `modules/dbt_taxfix/README.md` | Layer descriptions, dbt run/test/snapshot commands, docs serving |
| `modules/airflow/README.md` | DAG catalogue, trigger instructions, config location |

The top-level `README.md` links to each of these — add new modules there when created.

## CDC Pipeline Design Decisions

Key decisions that are already locked in — do not re-litigate:

| Concern | Decision |
|---------|----------|
| **Current state** | Last-write-wins on `source_timestamp` — valid because MongoDB CDC provides full documents in every INSERT/UPDATE payload, not diffs |
| **Pre-capture window users** | Users whose first event is an UPDATE (no INSERT in dataset) are treated as valid; latest UPDATE is ground truth |
| **Raw idempotency** | DELETE WHERE uuid IN (batch) + INSERT — re-processing a file replaces rows, never duplicates |
| **File cutoff** | `max(last_modified_utc) - TAXFIX_LOOKBACK_HOURS` from `raw.ingested_files` — file mtime, not event timestamp |
| **dbt snapshot strategy** | `check` with `check_cols='all'` — catches changes even when `updated_at` moves backward (late-arriving events) |
| **Sensitive fields** | `clean.users_public` VIEW omits `first_name`, `last_name`, raw `email`; exposes `email_domain` only |

## Constraints

- `duckdb`, `dbt-duckdb`, and `pytest` are already approved additions to `requirements.txt`; no other new deps without explicit instruction
- Do not modify `docker-compose.yaml` or `Dockerfile` unless asked
- Parts 3 & 4 of the case study (production architecture, privacy) are discussion-only — no code needed
