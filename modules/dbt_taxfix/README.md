# dbt Taxfix

Transforms raw CDC events in DuckDB through a three-layer pipeline:
`raw.cdc_events` → `staging.stg_cdc_events` → `clean.users` → `snapshots.users_snapshot`.

## Layer descriptions

### `staging.stg_cdc_events`

Parses and casts the raw JSON payload into typed columns. All change types (INSERT, UPDATE,
DELETE) pass through. No rows are dropped at this layer.

Key fields extracted: `user_id` (`payload._id`), `change_type`, `source_timestamp`,
`read_timestamp`, `email`, `first_name`, `last_name`, `country`, `date_of_birth`, `created_at`.

### `clean.users`

Current state of each user (last-write-wins on `source_timestamp`). DELETE events are
excluded. `age_group` is a 10-year bracket (`[20-30]`, etc.) derived from `date_of_birth`;
NULL when birthday is not present in the source (~449 users).

PII fields (`first_name`, `last_name`, `email`) are tagged with `meta: pii: true` in
`schema.yml`. A companion VIEW `clean.users_public` omits those fields and exposes only
`email_domain`.

### `snapshots.users_snapshot`

SCD Type 2 history of `clean.users`. Uses the `check` strategy with `check_cols='all'`
so any field change — including `source_timestamp` moving backward due to late-arriving
events — opens a new snapshot record. `dbt_valid_from` / `dbt_valid_to` track the active
period of each version.

## Custom tests

Six standalone SQL tests in `tests/`:

| Test | What it checks |
|------|---------------|
| `assert_no_deleted_users` | No DELETE change_type rows in `clean.users` |
| `assert_age_group_format` | All non-null `age_group` values match `[NN-NN]` pattern |
| `assert_age_group_width_is_ten` | Upper bound − lower bound = 10 for all age groups |
| `assert_email_contains_at` | Every `email` in `clean.users` contains `@` |
| `assert_user_count_matches_raw_active` | Row count matches active events in raw layer |
| `assert_all_users_have_raw_source` | Every user in `clean.users` has at least one raw event |

## How to run locally

```bash
cd modules/dbt_taxfix

# Run all models
dbt run --profiles-dir . --target dev

# Run tests (models only, not snapshot)
dbt test --profiles-dir . --target dev --exclude resource_type:snapshot

# Run snapshot
dbt snapshot --profiles-dir . --target dev --select users_snapshot

# Run snapshot tests
dbt test --profiles-dir . --target dev --select resource_type:snapshot

# Full refresh (rebuild all tables from scratch)
dbt run --profiles-dir . --target dev --full-refresh
```

Requires the raw layer to be populated first (`python modules/data-ingestion/raw.py ...`).

## How to serve docs

```bash
cd modules/dbt_taxfix
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir . --target dev --port 8001
# Open http://localhost:8001
```

## Schema naming

A `generate_schema_name` macro overrides dbt's default so `+schema: clean` resolves to
`clean` (not `main_clean`). This applies to both `staging` and `clean` schemas.
