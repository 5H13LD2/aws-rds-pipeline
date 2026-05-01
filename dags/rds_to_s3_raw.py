from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.stats import Stats
from airflow.utils.dates import days_ago
from psycopg2 import sql

RAW_S3_PREFIX = 'raw'
TABLE_NAMES = [
    'users',
    'products',
    'categories',
    'orders',
    'order_items',
    'payments',
    'shipping_events',
    'product_reviews',
    'inventory_logs',
    'cart_events',
]

POSTGRES_CONN_ID = 'postgres_rds'
S3_CONN_ID = 'aws_default'
LOCAL_OUTPUT_DIR = Path('/opt/airflow/output')
LOGGER = logging.getLogger(__name__)


def get_local_csv_path(table_name: str, run_date: str, output_dir: str) -> Path:
    path = Path(output_dir) / RAW_S3_PREFIX / table_name / f'dt={run_date}'
    path.mkdir(parents=True, exist_ok=True)
    return path / f'{table_name}.csv'


def ensure_known_table(table_name: str) -> None:
    if table_name not in TABLE_NAMES:
        raise ValueError(f'Unsupported table name: {table_name}')


def get_row_count(cursor, table_name: str) -> int:
    cursor.execute(
        sql.SQL('SELECT COUNT(*) FROM {};').format(sql.Identifier(table_name))
    )
    return int(cursor.fetchone()[0])


def copy_table_to_csv_file(cursor, table_name: str, file_obj) -> None:
    copy_sql = sql.SQL(
        'COPY (SELECT * FROM {} ORDER BY 1) TO STDOUT WITH CSV HEADER'
    ).format(sql.Identifier(table_name))
    cursor.copy_expert(copy_sql.as_string(cursor), file_obj)


def get_configured_s3_bucket() -> Optional[str]:
    return (
        Variable.get('s3_bucket', default_var=None)
        or Variable.get('s3_bronze_bucket', default_var=None)
        or os.getenv('S3_BRONZE_BUCKET')
        or os.getenv('S3_BUCKET')
    )


with DAG(
    dag_id='rds_to_s3_raw',
    description='Extract ecommerce source data from RDS PostgreSQL and load raw CSV to S3 or local output',
    schedule_interval='@daily',
    start_date=days_ago(2),
    catchup=False,
    default_args={
        'owner': 'data_engineer',
        'retries': 1,
    },
    orientation='LR',
    tags=['ecommerce', 'rds', 's3', 'raw'],
) as dag:

    @task
    def get_connection_config() -> Dict[str, Optional[str]]:
        context = get_current_context()
        return {
            'postgres_conn_id': POSTGRES_CONN_ID,
            's3_conn_id': S3_CONN_ID,
            's3_bucket': get_configured_s3_bucket(),
            'local_output_dir': Variable.get('local_output_dir', default_var=str(LOCAL_OUTPUT_DIR)),
            'run_date': context['ds'],
        }

    @task
    def validate_s3_bucket(config: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        s3_bucket = config['s3_bucket']
        if not s3_bucket:
            LOGGER.warning('No S3 bucket configured; raw extracts will be written locally')
            return config

        s3 = S3Hook(aws_conn_id=config['s3_conn_id'])
        if not s3.check_for_bucket(s3_bucket):
            raise ValueError(f'S3 bucket does not exist or is not accessible: {s3_bucket}')

        LOGGER.info('Validated S3 bronze bucket: s3://%s/%s/', s3_bucket, RAW_S3_PREFIX)
        return config

    @task
    def export_table_to_csv(table_name: str, config: Dict[str, Optional[str]]) -> str:
        ensure_known_table(table_name)
        pg = PostgresHook(postgres_conn_id=config['postgres_conn_id'])
        s3_bucket = config['s3_bucket']

        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                row_count = get_row_count(cursor, table_name)
                Stats.gauge(f'rds_to_s3_raw.{table_name}.row_count', row_count)
                LOGGER.info('Source table %s row_count=%s', table_name, row_count)

                if row_count == 0:
                    LOGGER.warning('Table %s has no rows; writing header-only raw extract', table_name)

                if s3_bucket:
                    s3 = S3Hook(aws_conn_id=config['s3_conn_id'])
                    key = f"{RAW_S3_PREFIX}/{table_name}/dt={config['run_date']}/{table_name}.csv"
                    with tempfile.NamedTemporaryFile(
                        mode='w+b',
                        suffix='.csv',
                    ) as tmp_file:
                        copy_table_to_csv_file(cursor, table_name, tmp_file)
                        tmp_file.flush()
                        s3.load_file(
                            filename=tmp_file.name,
                            key=key,
                            bucket_name=s3_bucket,
                            replace=True,
                        )
                    LOGGER.info('Exported table %s to s3://%s/%s', table_name, s3_bucket, key)
                    return f's3://{s3_bucket}/{key}'

                output_path = get_local_csv_path(
                    table_name,
                    config['run_date'],
                    config['local_output_dir'] or str(LOCAL_OUTPUT_DIR),
                )
                with output_path.open(mode='wb') as output_file:
                    copy_table_to_csv_file(cursor, table_name, output_file)

        LOGGER.info('Exported table %s to %s', table_name, output_path)
        return str(output_path)

    config = validate_s3_bucket(get_connection_config())

    export_tasks = []
    for table in TABLE_NAMES:
        task = export_table_to_csv.override(task_id=f'export_{table}')(table, config)
        export_tasks.append(task)

    export_tasks
