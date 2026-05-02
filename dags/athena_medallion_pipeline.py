from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

ATHENA_SCRIPT_DIR = "/opt/airflow"


with DAG(
    dag_id="athena_medallion_pipeline",
    description="Build Athena bronze, silver, and gold layers for the ecommerce data lake",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "data_engineer",
        "retries": 0,
        "execution_timeout": timedelta(hours=2),
    },
    orientation="LR",
    tags=["ecommerce", "athena", "bronze", "silver", "gold"],
) as dag:

    build_bronze = BashOperator(
        task_id="build_bronze_athena_tables",
        bash_command=f"python {ATHENA_SCRIPT_DIR}/run_athena_ddl.py",
        append_env=True,
    )

    build_silver = BashOperator(
        task_id="build_silver_parquet_tables",
        bash_command=f"python {ATHENA_SCRIPT_DIR}/run_athena_silver.py",
        append_env=True,
    )

    build_gold = BashOperator(
        task_id="build_gold_analytics_tables",
        bash_command=f"python {ATHENA_SCRIPT_DIR}/run_athena_gold.py",
        append_env=True,
    )

    build_bronze >> build_silver >> build_gold
