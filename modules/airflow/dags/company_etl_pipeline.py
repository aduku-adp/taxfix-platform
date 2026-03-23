"""Airflow DAG for the daily company ingestion + dbt build workflow.

Pipeline stages:
1. Extract and load raw company history from Excel files.
2. Build/refresh dbt models.
3. Validate transformed models.
4. Build dbt snapshot state.
5. Validate snapshot outputs.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DB_ENV = {
    "PG_HOST": "postgres",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_PASSWORD": "postgres",
    "DB_NAME": "qam_db",
}

# Shared DB connection settings injected into each Bash task.

with DAG(
    dag_id="company_etl_pipeline",
    description="Extract company data, run dbt models + tests, run snapshot + tests",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["ratings", "etl", "dbt"],
) as dag:
    # Orchestration sentinels make the run graph explicit in the UI.
    start = EmptyOperator(task_id="start")

    extract_company_data = BashOperator(
        task_id="extract_company_data",
        bash_command=(
            "cd /opt/airflow/repo/modules/data-extraction "
            "&& python extract_company_history.py"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/repo/modules/dbt_taxfix "
            "&& dbt run --profiles-dir /opt/airflow/repo/modules/dbt_taxfix --target airflow"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_model_tests = BashOperator(
        task_id="run_dbt_model_tests",
        bash_command=(
            "cd /opt/airflow/repo/modules/dbt_taxfix "
            "&& dbt test --profiles-dir /opt/airflow/repo/modules/dbt_taxfix --target airflow --exclude resource_type:snapshot"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command=(
            "cd /opt/airflow/repo/modules/dbt_taxfix "
            "&& dbt snapshot --profiles-dir /opt/airflow/repo/modules/dbt_taxfix --target airflow --select snap_company"
        ),
        env=DB_ENV,
        append_env=True,
    )

    run_dbt_snapshot_tests = BashOperator(
        task_id="run_dbt_snapshot_tests",
        bash_command=(
            "cd /opt/airflow/repo/modules/dbt_taxfix "
            "&& dbt test --profiles-dir /opt/airflow/repo/modules/dbt_taxfix --target airflow --select resource_type:snapshot"
        ),
        env=DB_ENV,
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    # Linear execution keeps failure boundaries clear for observability.
    (
        start
        >> extract_company_data
        >> run_dbt_models
        >> run_dbt_model_tests
        >> run_dbt_snapshot
        >> run_dbt_snapshot_tests
        >> end
    )
