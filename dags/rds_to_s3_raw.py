from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

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


def write_local_csv(table_name: str, run_date: str, content: str) -> str:
    path = LOCAL_OUTPUT_DIR / RAW_S3_PREFIX / table_name / run_date
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / f'{table_name}.csv'
    file_path.write_text(content, encoding='utf-8')
    return str(file_path)


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
    tags=['ecommerce', 'rds', 's3', 'raw'],
) as dag:

    @task
    def get_connection_config() -> Dict[str, Optional[str]]:
        return {
            'postgres_conn_id': POSTGRES_CONN_ID,
            's3_conn_id': S3_CONN_ID,
            's3_bucket': Variable.get('s3_bucket', default_var=None),
            'run_date': datetime.utcnow().strftime('%Y-%m-%d'),
        }

    @task
    def export_table_to_csv(table_name: str, config: Dict[str, Optional[str]]) -> str:
        pg = PostgresHook(postgres_conn_id=config['postgres_conn_id'])
        query = f'SELECT * FROM {table_name};'
        conn = pg.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        if not records:
            raise ValueError(f'No rows found in table {table_name}')

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(records)
        content = buffer.getvalue()

        if config['s3_bucket']:
            s3 = S3Hook(aws_conn_id=config['s3_conn_id'])
            key = f"{RAW_S3_PREFIX}/{table_name}/{config['run_date']}/{table_name}.csv"
            s3.load_string(content, key, bucket_name=config['s3_bucket'], replace=True)
            return f's3://{config["s3_bucket"]}/{key}'

        return write_local_csv(table_name, config['run_date'], content)

    config = get_connection_config()

    export_tasks = []
    for table in TABLE_NAMES:
        task = export_table_to_csv.override(task_id=f'export_{table}')(table, config)
        export_tasks.append(task)

    export_tasks
