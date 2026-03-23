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

- **Company pipeline:** run DAG `company_etl_pipeline`
- **CDC pipeline:** run DAG `cdc_pipeline`


### 3. Run CDC business queries

After the `cdc_pipeline` DAG completes:

```bash
python modules/data-ingestion/queries/run_queries.py dbs/duckdb_data/dev.duckdb
```

### 4. Go to FastAPI Swagger: `http://localhost:8000/docs`


## Data location

Corporate input files are expected in:

- `data/corporates/*.xlsm|*.xlsx`

CDC events (users change stream) are expected in:

- `data/users/YYYY/MM/DD/HH/mm/events-*.jsonl`

Download the CDC dataset: see `.ai/context/case-study-requirements.md` for the link. Extract into `data/users/`.


## dbt docs

From `taxfix-platform/modules/dbt_taxfix`:

```bash
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir . --target dev --port 8001
```
