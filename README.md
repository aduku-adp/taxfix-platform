# Tax Fix Platform

Data platform with:
- Python ingestion + validation pipeline
- dbt warehouse transformations + snapshots
- Airflow orchestration

## Module documentation

- Data ingestion: [modules/data-ingestion/README.md](/modules/data-ingestion/README.md)
- dbt transformations: [modules/dbt_taxfix/README.md](/modules/dbt_taxfix/README.md)
- Airflow orchestration: [modules/airflow/README.md](/modules/airflow/README.md)


## Architecture Diagram

![TaxFix architecture diagram](/images/taxfix-architecture.png)

## Stack startup

Follow this process to startup your stack correctly.

### 0. Clone the project
- Clone the project
```bash
git clone git@github.com:aduku-adp/taxfix-platform.git
```

- Create an .env file from provided template
```bash
cd taxfix-platform
cp .env-template .env
```


### 1. One-command stack startup

```bash
cd tools/
./clean_deploy_stack.sh
```

## Key URLs

- Airflow UI: `http://localhost:8080`
- dbt docs (if served): `http://localhost:8001`


### 2. Trigger a data ingestion via airflow

- Connect to airflow UI with default airflow credentials:
  - Username: `airflow`
  - Password: `airflow`

- **CDC pipeline:** run DAG `cdc_pipeline`


## Data location

CDC events (users change stream) are expected in:

- `data/users/YYYY/MM/DD/HH/mm/events-*.jsonl`

Download the CDC dataset: see `.ai/context/case-study-requirements.md` for the link. Extract into `data/users/`.


## dbt docs

From `taxfix-platform/modules/dbt_taxfix`:

```bash
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir . --target dev --port 8001
```

---

## Business Questions

SQL queries run against the DuckDB analytics layer after pipeline execution. Full query file: [`modules/data-ingestion/queries/business_questions.sql`](modules/data-ingestion/queries/business_questions.sql)

### Q1 — How many distinct active users are in the current snapshot (not deleted)?

```sql
SELECT count(*) AS active_users
FROM clean.users;
```

**Result:** `18,997`

---

### Q2 — What percentage of active users use Gmail as their email provider?

```sql
SELECT
    round(
        100.0 * count(*) FILTER (WHERE email ILIKE '%@gmail.com') / count(*),
        2
    ) AS gmail_pct
FROM clean.users;
```

**Result:** `30.38%`

---

### Q3 — Which are the top 3 countries by number of Gmail users?

```sql
SELECT
    country,
    count(*) AS gmail_users
FROM clean.users
WHERE email ILIKE '%@gmail.com'
GROUP BY country
ORDER BY gmail_users DESC
LIMIT 3;
```

**Result:**

| country | gmail_users |
|---------|-------------|
| Germany | 1,172 |
| Switzerland | 235 |
| United Kingdom | 228 |

---

### Q4a — How many users changed their email address at least once?

```sql
WITH email_events AS (
    SELECT
        user_id,
        email,
        source_timestamp,
        lag(email) OVER (
            PARTITION BY user_id
            ORDER BY source_timestamp
        ) AS prev_email
    FROM staging.stg_cdc_events
    WHERE change_type IN ('INSERT', 'UPDATE')
      AND email IS NOT NULL
)
SELECT count(DISTINCT user_id) AS users_with_email_change
FROM email_events
WHERE prev_email IS NOT NULL
  AND email != prev_email;
```

**Result:** `90`

---

### Q4b — What are the top 5 email domain transitions?

```sql
WITH email_events AS (
    SELECT
        email,
        lag(email) OVER (
            PARTITION BY user_id
            ORDER BY source_timestamp
        ) AS prev_email
    FROM staging.stg_cdc_events
    WHERE change_type IN ('INSERT', 'UPDATE')
      AND email IS NOT NULL
),
transitions AS (
    SELECT
        split_part(prev_email, '@', 2) || ' -> ' || split_part(email, '@', 2) AS transition
    FROM email_events
    WHERE prev_email IS NOT NULL
      AND split_part(email, '@', 2) != split_part(prev_email, '@', 2)
)
SELECT transition, count(*) AS occurrences
FROM transitions
GROUP BY transition
ORDER BY occurrences DESC
LIMIT 5;
```

**Result:**

| transition | occurrences |
|------------|-------------|
| gmail.com -> icloud.com | 4 |
| yahoo.com -> outlook.com | 4 |
| zoho.com -> gmail.com | 3 |
| gmail.com -> yahoo.com | 2 |
| outlook.com -> gmail.com | 2 |

---

### Q5 — What is the average time between first and last CDC event per user?

```sql
SELECT
    round(
        avg(date_diff('minute', first_event, last_event)),
        2
    ) AS avg_minutes_first_to_last
FROM (
    SELECT
        min(source_timestamp) AS first_event,
        max(source_timestamp) AS last_event
    FROM staging.stg_cdc_events
    GROUP BY user_id
    HAVING count(*) > 1
) t;
```

**Result:** `2.86 minutes`

---

## Design Decisions and Trade-offs

| Concern | Decision | Trade-off |
|---------|----------|-----------|
| **Current state** | Last-write-wins on `source_timestamp` | Valid because MongoDB CDC provides full documents in every INSERT/UPDATE payload — not diffs. Would need revisiting if the source ever switched to partial updates. |
| **Pre-capture window users** | Users whose first event is an UPDATE (no INSERT in dataset) are treated as valid; latest UPDATE is ground truth | Treats ~449 users as active with incomplete history. The alternative — dropping them — would silently undercount active users. |
| **Raw idempotency** | DELETE WHERE `uuid IN (batch)` + INSERT on re-processing | Ensures re-running a file produces the same result. INSERT OR IGNORE was rejected because it would silently leave stale rows if a field changed. |
| **File cutoff** | `max(last_modified_utc) - TAXFIX_LOOKBACK_HOURS` from `raw.ingested_files` — based on file mtime, not event timestamp | File mtime is a reliable proxy for when data arrived; using event timestamp would miss files that arrived late but contain old events. |
| **Validation errors** | Skip-not-crash: bad events go to `raw.ingestion_errors` dead-letter table and are emitted as `WARNING` to the console | Keeps the pipeline running through partial data issues. The raw line is preserved for replay. The alternative — aborting on first error — would halt ingestion for a single malformed event. |
| **dbt snapshot strategy** | `check` with `check_cols='all'` | Catches changes even when `updated_at` moves backward (late-arriving events). The `timestamp` strategy would miss these. |
| **Sensitive fields** | `clean.users_public` VIEW omits `first_name`, `last_name`, raw `email`; exposes `email_domain` only | Consumers default to the public view. The full `clean.users` table is available to privileged roles only. |
| **Age group formula** | `floor(age_years / 10) * 10` → `[30-40]` label | Irreversible anonymization of date of birth. NULL when birthday is absent in source (~449 users). |

Full details: [`modules/data-ingestion/README.md`](modules/data-ingestion/README.md) · [`modules/dbt_taxfix/README.md`](modules/dbt_taxfix/README.md)

---

## Assumptions

- `source_timestamp` ordering within a user reflects true event order.
- Payload schema is additive going forward: new fields will not break existing validation (Pydantic `extra='allow'` passes unknown fields through into the raw JSON column).
- Pre-capture-window users (first event is UPDATE, no INSERT in dataset) are valid active users — their latest UPDATE is treated as ground truth.
- DELETE events contain only `_id` in the payload (as documented in the case study). INSERT and UPDATE contain the full document.
- File modification time (`mtime`) is a reliable proxy for when data arrived at the ingestion layer.

---

## AI Assistance

**Tool used:** Claude Code (claude-sonnet-4-6) — interactive CLI assistant.

Claude was used throughout implementation to scaffold code, debug issues, and write documentation. All output was reviewed and validated against the actual dataset before committing.

| Component | How AI helped |
|-----------|--------------|
| Pydantic validation models | Scaffolded initial model; field types and `extra='allow'` policy refined manually |
| DuckDB DDL/DML (`loader.py`) | Generated schema, incremental DML, file-status tracking |
| Pipeline orchestrator (`raw.py`) | Drafted CLI interface and cutoff logic |
| Business question SQL | Wrote Q1–Q5; diagnosed DuckDB `->>` operator-precedence bug requiring `json_extract_string()` |
| dbt models + macro | Staged JSON extraction, clean last-write-wins logic, `generate_schema_name` macro |
| pytest fixtures + tests | Generated all 34 test cases across 4 test modules |
| Airflow DAG | Scaffolded `BashOperator` + `EmptyOperator` pattern |
| Documentation | Drafted all READMEs and discussion responses (Parts 3 & 4) |

Full details: [`AI_USAGE.md`](AI_USAGE.md)
