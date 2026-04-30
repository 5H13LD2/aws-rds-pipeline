#!/usr/bin/env python3

"""Seed RDS PostgreSQL with generated ecommerce data."""

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.generate_fake_data import generate_data

SCHEMA_FILE = BASE_DIR / 'sql' / 'rds_schema.sql'


def load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE_DIR / '.env')
    except ImportError:
        pass


def get_db_config():
    return {
        'host': os.getenv('RDS_HOST', os.getenv('POSTGRES_RDS_HOST')),
        'port': int(os.getenv('RDS_PORT', os.getenv('POSTGRES_RDS_PORT', 5432))),
        'dbname': os.getenv('RDS_DB', os.getenv('POSTGRES_RDS_SCHEMA')),
        'user': os.getenv('RDS_USER', os.getenv('POSTGRES_RDS_LOGIN')),
        'password': os.getenv('RDS_PASSWORD', os.getenv('POSTGRES_RDS_PASSWORD')),
    }


def connect_db(config):
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Missing required database config: {', '.join(missing)}")
    return psycopg2.connect(**config)


def execute_schema(cursor):
    with SCHEMA_FILE.open('r', encoding='utf-8') as fp:
        sql = fp.read()
    sql = sql[sql.find('CREATE TABLE'):]
    cursor.execute(sql)


def insert_rows(cursor, table_name, rows):
    if not rows:
        return
    columns = list(rows[0].keys())
    values = [[row[col] for col in columns] for row in rows]
    columns_sql = ', '.join(f'"{col}"' for col in columns)
    insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES %s"
    execute_values(cursor, insert_sql, values)


def seed_database(truncate: bool = False):
    load_env()
    config = get_db_config()
    data = generate_data()

    with connect_db(config) as conn:
        with conn.cursor() as cursor:
            execute_schema(cursor)

            if truncate:
                for table in [
                    'cart_events',
                    'inventory_logs',
                    'product_reviews',
                    'shipping_events',
                    'payments',
                    'order_items',
                    'orders',
                    'products',
                    'categories',
                    'users',
                ]:
                    cursor.execute(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;')

            insert_rows(cursor, 'categories', data['categories'])
            insert_rows(cursor, 'products', data['products'])
            insert_rows(cursor, 'users', data['users'])
            insert_rows(cursor, 'orders', data['orders'])
            insert_rows(cursor, 'order_items', data['order_items'])
            insert_rows(cursor, 'payments', data['payments'])
            insert_rows(cursor, 'shipping_events', data['shipping_events'])
            insert_rows(cursor, 'product_reviews', data['product_reviews'])
            insert_rows(cursor, 'inventory_logs', data['inventory_logs'])
            insert_rows(cursor, 'cart_events', data['cart_events'])

    print('RDS PostgreSQL seed completed successfully.')


def usage():
    print('Usage: python scripts/seed_rds.py [--truncate]')


if __name__ == '__main__':
    truncate_flag = '--truncate' in sys.argv
    seed_database(truncate=truncate_flag)
