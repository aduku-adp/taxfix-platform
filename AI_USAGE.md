# AI Usage

This document describes how AI tools were used during development of this case study, in accordance with the submission guidelines.

## Tools Used

| Tool | Version / Model | Role |
|------|----------------|------|
| **Claude Code** | claude-sonnet-4-6 (CLI) | Primary development assistant — code generation, debugging, refactoring, SQL, docs |

No other AI tools (ChatGPT, GitHub Copilot, Cursor, etc.) were used.

## How Claude Code Was Used

Claude Code was used as an interactive CLI assistant throughout the project. All output was reviewed, tested against real data, and committed only after validation. The workflow was conversational: I provided requirements and constraints, Claude proposed implementations, and I iterated or corrected as needed.

### Data Ingestion (`modules/data-ingestion/`)

| Task | AI involvement |
|------|---------------|
| Pydantic validation models (`validation_models.py`) | Scaffolded initial model; I refined field types and `extra='allow'` policy |
| DuckDB DDL + DML (`loader.py`) | Generated schema, `ensure_schema()`, `insert_batch()`, `record_file()`, `record_error()` |
| Pipeline orchestrator (`raw.py`) | Drafted CLI interface, incremental cutoff logic, and file-status tracking |
| Dead-letter table design | Suggested `raw.ingestion_errors` with `error_type` enum; I validated the schema |
| Business question SQL (`queries/business_questions.sql`) | Wrote all 5 queries; diagnosed DuckDB `->>` operator-precedence bug in `WHERE`/`PARTITION BY` clauses (required switching to `json_extract_string()`) |
| pytest fixtures and tests | Generated `conftest.py` fixtures and test cases for all four test modules |

### dbt Layer (`modules/dbt_taxfix/`)

| Task | AI involvement |
|------|---------------|
| Staging model (`stg_cdc_events.sql`) | Drafted JSON extraction and type casting |
| Clean model (`clean/users.sql`) | Wrote last-write-wins window logic; I added the NULL guard for `age_group` |
| `generate_schema_name` macro | Proposed macro to prevent dbt appending `main_` prefix to schema names |
| Custom SQL tests (`tests/`) | Generated all 6 test files; I validated logic against actual data |
| Snapshot configuration | Suggested `check` strategy with `check_cols='all'` for late-arriving event safety |

### Airflow DAG (`modules/airflow/dags/cdc_pipeline.py`)

| Task | AI involvement |
|------|---------------|
| DAG structure | Scaffolded using `BashOperator` + `EmptyOperator` sentinels pattern |
| Task ordering and env injection | Proposed `env=DB_ENV, append_env=True` pattern for container-safe env passing |

### Documentation

| File | AI involvement |
|------|---------------|
| `modules/data-ingestion/README.md` | Drafted; I added actual query results after pipeline execution |
| `modules/dbt_taxfix/README.md` | Drafted; I verified layer descriptions against model output |
| `modules/airflow/README.md` | Drafted |
| `.ai/discussion.md` (Part 3 & 4 responses) | Generated production architecture and privacy responses based on the actual implementation |
| This file | Generated |

## What I Validated Manually

- Ran the full pipeline against the actual CDC dataset and verified row counts matched expected values (Q1–Q5 results in `modules/data-ingestion/README.md`)
- Confirmed DuckDB schema created correctly and queries returned accurate results
- Ran `pytest -q modules/data-ingestion/tests` — all 34 tests passed
- Ran `dbt run`, `dbt test`, `dbt snapshot` locally and verified model output
- Reviewed all generated SQL for correctness, especially window function ordering and NULL handling

## Chat Logs

The full conversation history is available via the Claude Code session. Key exchanges are summarised below — personal workspace paths have been redacted.

### Session 1 — 2026-03-23: Initial scaffolding (T1–T4)

Covered:
- Project initialisation (CLAUDE.md, spec, directory structure)
- Raw ingestion layer: Pydantic models, DuckDB DDL/DML, orchestrator, dead-letter table
- dbt staging + clean models, `generate_schema_name` macro, age group formula
- SCD Type 2 snapshot configuration

Key correction: I caught that the initial `age_group` formula produced incorrect labels when `date_of_birth` was NULL — Claude added a `CASE WHEN date_of_birth IS NOT NULL` guard.

### Session 2 — 2026-03-23: Business questions + Airflow DAG

Covered:
- Q1–Q5 SQL queries
- DuckDB operator-precedence bug diagnosis (`->>` in `WHERE`/`PARTITION BY` — switched to `json_extract_string()`)
- Airflow `cdc_pipeline` DAG with BashOperator tasks

Key correction: Q4/Q5 originally used `raw.cdc_events` with `json_extract_string(payload, ...)`. Later updated to use `staging.stg_cdc_events` with named columns for cleaner SQL.
