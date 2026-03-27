"""Airflow DAG for the daily CDC ingestion + dbt build workflow.

Pipeline stages:
1. Load raw CDC events from JSONL files into DuckDB (raw layer).
2. Build/refresh dbt staging and clean models.
3. Validate transformed models.
4. Build dbt snapshot (SCD Type 2 history).
5. Validate snapshot outputs.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DBT_DIR = "/opt/airflow/repo/modules/dbt_taxfix"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --target airflow"

DB_ENV = {
    "TAXFIX_DB_PATH": "/opt/airflow/repo/dbs/duckdb_data/taxfix.duckdb",
    "TAXFIX_DATA_DIR": "/opt/airflow/repo/data/users",
    "TAXFIX_LOOKBACK_HOURS": "24",
    "TAXFIX_FULL_REFRESH": "",
}

with DAG(
    dag_id="cdc_pipeline",
    description="Ingest CDC events, run dbt models + tests, run snapshot + tests",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["cdc", "duckdb", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=(
            "cd /opt/airflow/repo/modules/data-ingestion "
            "&& python raw.py $TAXFIX_DB_PATH $TAXFIX_DATA_DIR "
            "${TAXFIX_FULL_REFRESH:+--full-refresh}"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_DIR} "
            f"&& dbt run {DBT_FLAGS} "
            "${TAXFIX_FULL_REFRESH:+--full-refresh}"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_model_tests = BashOperator(
        task_id="run_dbt_model_tests",
        bash_command=(
            f"cd {DBT_DIR} "
            f"&& dbt test {DBT_FLAGS} --exclude resource_type:snapshot"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command=(
            f"cd {DBT_DIR} "
            f"&& dbt snapshot {DBT_FLAGS} --select users_snapshot"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_snapshot_tests = BashOperator(
        task_id="run_dbt_snapshot_tests",
        bash_command=(
            f"cd {DBT_DIR} "
            f"&& dbt test {DBT_FLAGS} --select resource_type:snapshot"
        ),
        env=DB_ENV,
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> load_raw
        >> run_dbt_models
        >> run_dbt_model_tests
        >> run_dbt_snapshot
        >> run_dbt_snapshot_tests
        >> end
    )
