from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from psycopg2.extras import execute_values

POSTGRES_CONN_ID = 'postgres_rds'

REPO_DIR = Path(__file__).resolve().parent.parent
AIRFLOW_DIR = Path('/opt/airflow')

for candidate in (REPO_DIR, AIRFLOW_DIR):
    if (candidate / 'scripts').exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from scripts.generate_fake_data import generate_data

SCHEMA_CANDIDATES = [
    REPO_DIR / 'sql' / 'rds_schema.sql',
    AIRFLOW_DIR / 'sql' / 'rds_schema.sql',
]

INSERT_ORDER = [
    'categories',
    'products',
    'users',
    'orders',
    'order_items',
    'payments',
    'shipping_events',
    'product_reviews',
    'inventory_logs',
    'cart_events',
]

TRUNCATE_ORDER = list(reversed(INSERT_ORDER))

PRIMARY_KEYS = {
    'categories': 'category_id',
    'products': 'product_id',
    'users': 'user_id',
    'orders': 'order_id',
    'order_items': 'order_item_id',
    'payments': 'payment_id',
    'shipping_events': 'shipping_event_id',
    'product_reviews': 'review_id',
    'inventory_logs': 'inventory_log_id',
    'cart_events': 'cart_event_id',
}


def get_schema_file() -> Path:
    for path in SCHEMA_CANDIDATES:
        if path.exists():
            return path
    checked = ', '.join(str(path) for path in SCHEMA_CANDIDATES)
    raise FileNotFoundError(f'RDS schema file not found. Checked: {checked}')


def get_create_schema_sql(schema_sql: str) -> str:
    create_start = schema_sql.find('CREATE TABLE')
    if create_start == -1:
        raise ValueError('RDS schema file does not contain CREATE TABLE statements')
    return schema_sql[create_start:]


def safe_suffix(value: str) -> str:
    suffix = re.sub(r'[^a-zA-Z0-9]+', '_', value).strip('_').lower()
    return suffix[-40:] or 'manual'


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute('SELECT to_regclass(%s);', (table_name,))
    return cursor.fetchone()[0] is not None


def insert_rows(
    cursor,
    table_name: str,
    rows: List[Dict[str, Any]],
    returning_column: Optional[str] = None,
) -> List[int]:
    if not rows:
        return []

    columns = list(rows[0].keys())
    values = [[row[column] for column in columns] for row in rows]
    columns_sql = ', '.join(f'"{column}"' for column in columns)
    insert_sql = f'INSERT INTO {table_name} ({columns_sql}) VALUES %s'

    if returning_column:
        insert_sql = f'{insert_sql} RETURNING {returning_column}'
        returned_rows = execute_values(cursor, insert_sql, values, fetch=True)
        return [row[0] for row in returned_rows]

    execute_values(cursor, insert_sql, values)
    return []


def remap_column(rows: List[Dict[str, Any]], column: str, id_map: Dict[int, int]) -> None:
    for row in rows:
        row[column] = id_map[row[column]]


def insert_seed_data(cursor, data: Dict[str, List[Dict[str, Any]]], append_mode: bool) -> Dict[str, int]:
    if append_mode:
        suffix = safe_suffix(get_current_context()['run_id'])
        for category in data['categories']:
            category['name'] = f"{category['name']} {suffix}"
        for product in data['products']:
            product['sku'] = f"{product['sku']}-{suffix}"
        for user in data['users']:
            local_part, domain = user['email'].rsplit('@', 1)
            user['email'] = f'{local_part}+{suffix}@{domain}'

    category_ids = insert_rows(cursor, 'categories', data['categories'], PRIMARY_KEYS['categories'])
    category_id_map = dict(enumerate(category_ids, start=1))

    remap_column(data['products'], 'category_id', category_id_map)
    product_ids = insert_rows(cursor, 'products', data['products'], PRIMARY_KEYS['products'])
    product_id_map = dict(enumerate(product_ids, start=1))

    user_ids = insert_rows(cursor, 'users', data['users'], PRIMARY_KEYS['users'])
    user_id_map = dict(enumerate(user_ids, start=1))

    remap_column(data['orders'], 'user_id', user_id_map)
    order_ids = insert_rows(cursor, 'orders', data['orders'], PRIMARY_KEYS['orders'])
    order_id_map = dict(enumerate(order_ids, start=1))

    for row in data['order_items']:
        row['order_id'] = order_id_map[row['order_id']]
        row['product_id'] = product_id_map[row['product_id']]
    insert_rows(cursor, 'order_items', data['order_items'], PRIMARY_KEYS['order_items'])

    remap_column(data['payments'], 'order_id', order_id_map)
    insert_rows(cursor, 'payments', data['payments'], PRIMARY_KEYS['payments'])

    remap_column(data['shipping_events'], 'order_id', order_id_map)
    insert_rows(cursor, 'shipping_events', data['shipping_events'], PRIMARY_KEYS['shipping_events'])

    for row in data['product_reviews']:
        row['product_id'] = product_id_map[row['product_id']]
        row['user_id'] = user_id_map[row['user_id']]
    insert_rows(cursor, 'product_reviews', data['product_reviews'], PRIMARY_KEYS['product_reviews'])

    remap_column(data['inventory_logs'], 'product_id', product_id_map)
    insert_rows(cursor, 'inventory_logs', data['inventory_logs'], PRIMARY_KEYS['inventory_logs'])

    for row in data['cart_events']:
        row['user_id'] = user_id_map[row['user_id']]
        row['product_id'] = product_id_map[row['product_id']]
    insert_rows(cursor, 'cart_events', data['cart_events'], PRIMARY_KEYS['cart_events'])

    return {table_name: len(data[table_name]) for table_name in INSERT_ORDER}


def get_table_counts(cursor) -> Dict[str, int]:
    counts = {}
    for table_name in INSERT_ORDER:
        if table_exists(cursor, table_name):
            cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
            counts[table_name] = cursor.fetchone()[0]
        else:
            counts[table_name] = 0
    return counts


with DAG(
    dag_id='seed_rds_fake_data',
    description='Generate fake ecommerce data and seed it into RDS PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'owner': 'data_engineer',
        'retries': 1,
    },
    params={
        'fresh': Param(False, type='boolean'),
        'truncate': Param(True, type='boolean'),
        'users_count': Param(200, type='integer', minimum=1),
        'product_count': Param(120, type='integer', minimum=1),
        'orders_count': Param(300, type='integer', minimum=1),
        'review_count': Param(180, type='integer', minimum=0),
        'inventory_log_count': Param(150, type='integer', minimum=0),
        'cart_event_count': Param(240, type='integer', minimum=0),
    },
    tags=['ecommerce', 'rds', 'seed', 'fake-data'],
) as dag:

    @task
    def get_seed_config() -> Dict[str, Any]:
        params = get_current_context()['params']
        return {
            'postgres_conn_id': POSTGRES_CONN_ID,
            'fresh': params['fresh'],
            'truncate': params['truncate'],
            'users_count': params['users_count'],
            'product_count': params['product_count'],
            'orders_count': params['orders_count'],
            'review_count': params['review_count'],
            'inventory_log_count': params['inventory_log_count'],
            'cart_event_count': params['cart_event_count'],
        }

    @task
    def seed_database(config: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        data = generate_data(
            users_count=config['users_count'],
            product_count=config['product_count'],
            orders_count=config['orders_count'],
            review_count=config['review_count'],
            inventory_log_count=config['inventory_log_count'],
            cart_event_count=config['cart_event_count'],
        )

        schema_sql = get_schema_file().read_text(encoding='utf-8')
        pg = PostgresHook(postgres_conn_id=config['postgres_conn_id'])

        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                if config['fresh']:
                    print('Running in FRESH mode: dropping and recreating schema')
                    cursor.execute(schema_sql)
                    append_mode = False
                else:
                    cursor.execute(get_create_schema_sql(schema_sql))
                    if config['truncate']:
                        print('Running in TRUNCATE mode: truncating tables and restarting identities')
                        for table_name in TRUNCATE_ORDER:
                            if table_exists(cursor, table_name):
                                cursor.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;')
                    else:
                        print('Running in APPEND mode: preserving existing data and inserting additional rows')
                    append_mode = not config['truncate']

                inserted_counts = insert_seed_data(cursor, data, append_mode=append_mode)
                total_counts = get_table_counts(cursor)

        print(f'Inserted row counts: {inserted_counts}')
        print(f'Post-seed table counts: {total_counts}')
        return {
            'inserted_counts': inserted_counts,
            'total_counts': total_counts,
        }

    seed_database(get_seed_config())
