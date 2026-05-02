import os
import re
import time
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from urllib.parse import urlparse


def load_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    pattern = re.compile(r"\$\{([^}]+)\}")
    loaded = {}
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        value = pattern.sub(lambda m: loaded.get(m.group(1), os.environ.get(m.group(1), "")), value)
        os.environ.setdefault(key, value)
        loaded[key] = os.environ[key]


load_env()

REGION          = os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-2"
S3_BUCKET       = os.environ.get("S3_BRONZE_BUCKET") or "retail-logistics-data-lake"
SILVER_DATABASE = os.environ.get("ATHENA_SILVER_DATABASE") or "retail_logistics_silver"
GOLD_DATABASE   = os.environ.get("ATHENA_GOLD_DATABASE") or "retail_logistics_gold"
GOLD_PREFIX     = (os.environ.get("ATHENA_GOLD_PREFIX") or "gold").strip("/")
OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT_LOCATION") or f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client("athena", region_name=REGION)
s3     = boto3.client("s3", region_name=REGION)


def gold_location(table_name: str) -> str:
    return f"s3://{S3_BUCKET}/{GOLD_PREFIX}/{table_name}/"


def run(statement: str, label: str, database: str = GOLD_DATABASE) -> bool:
    try:
        resp = athena.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"  FAIL  {label}\n        {exc}", flush=True)
        return False

    exec_id = resp["QueryExecutionId"]
    while True:
        try:
            result = athena.get_query_execution(QueryExecutionId=exec_id)
        except (BotoCoreError, ClientError) as exc:
            print(f"  FAIL  {label}\n        {exc}", flush=True)
            return False
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state == "SUCCEEDED":
        print(f"  OK    {label}", flush=True)
        return True

    reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
    print(f"  FAIL  {label}\n        {reason}", flush=True)
    return False


def fetch_rows(statement: str, label: str, database: str = GOLD_DATABASE):
    try:
        resp = athena.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc

    exec_id = resp["QueryExecutionId"]
    while True:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"{label} failed: {reason}")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=exec_id):
        rows.extend(page["ResultSet"]["Rows"])
    return rows


def delete_s3_prefix(s3_uri: str) -> None:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket or not prefix:
        raise ValueError(f"Refusing to delete unsafe S3 prefix: {s3_uri}")

    paginator  = s3.get_paginator("list_objects_v2")
    batch      = []
    deleted    = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
    print(f"  OK    CLEAN {s3_uri} ({deleted} objects)", flush=True)


# ─── GOLD CTAS DEFINITIONS ────────────────────────────────────────────────────

S = SILVER_DATABASE  # shorthand

GOLD_TABLES = {

    # 1. Daily Sales — revenue and order volume per day
    "daily_sales": f"""
        SELECT
            DATE(o.order_date)                          AS sale_date,
            COUNT(DISTINCT o.order_id)                  AS total_orders,
            COUNT(DISTINCT o.user_id)                   AS unique_customers,
            SUM(o.subtotal)                             AS subtotal,
            SUM(o.tax)                                  AS total_tax,
            SUM(o.shipping_cost)                        AS total_shipping,
            SUM(o.total_amount)                         AS total_revenue,
            AVG(o.total_amount)                         AS avg_order_value
        FROM {S}.orders o
        WHERE o.status IN ('delivered', 'shipped', 'processing')
        GROUP BY DATE(o.order_date)
        ORDER BY sale_date
    """,

    # 2. Customer Lifetime Value — total spend and behaviour per user
    "customer_lifetime_value": f"""
        SELECT
            u.user_id,
            u.first_name,
            u.last_name,
            u.email,
            u.country,
            COUNT(DISTINCT o.order_id)                  AS total_orders,
            SUM(o.total_amount)                         AS lifetime_value,
            AVG(o.total_amount)                         AS avg_order_value,
            MIN(DATE(o.order_date))                     AS first_order_date,
            MAX(DATE(o.order_date))                     AS last_order_date,
            DATE_DIFF('day',
                MIN(o.order_date),
                MAX(o.order_date))                      AS customer_lifespan_days
        FROM {S}.users u
        LEFT JOIN {S}.orders o
            ON u.user_id = o.user_id
            AND o.status IN ('delivered', 'shipped', 'processing')
        GROUP BY
            u.user_id, u.first_name, u.last_name, u.email, u.country
        ORDER BY lifetime_value DESC
    """,

    # 3. Top Products — revenue and units sold per product
    "top_products": f"""
        SELECT
            p.product_id,
            p.name                                      AS product_name,
            p.sku,
            c.name                                      AS category_name,
            p.price                                     AS unit_price,
            SUM(oi.quantity)                            AS total_units_sold,
            COUNT(DISTINCT oi.order_id)                 AS total_orders,
            SUM(oi.total_price)                         AS total_revenue,
            AVG(pr.rating)                              AS avg_rating,
            COUNT(DISTINCT pr.review_id)                AS total_reviews
        FROM {S}.products p
        LEFT JOIN {S}.categories c
            ON p.category_id = c.category_id
        LEFT JOIN {S}.order_items oi
            ON p.product_id = oi.product_id
        LEFT JOIN {S}.product_reviews pr
            ON p.product_id = pr.product_id
        GROUP BY
            p.product_id, p.name, p.sku, c.name, p.price
        ORDER BY total_revenue DESC
    """,

    # 4. Inventory Movement — stock changes over time per product
    "inventory_movement": f"""
        SELECT
            il.product_id,
            p.name                                      AS product_name,
            p.sku,
            il.event_type,
            DATE(il.event_timestamp)                    AS event_date,
            COUNT(*)                                    AS event_count,
            SUM(il.quantity)                            AS total_quantity_moved,
            AVG(il.new_quantity)                        AS avg_stock_after_event
        FROM {S}.inventory_logs il
        LEFT JOIN {S}.products p
            ON il.product_id = p.product_id
        GROUP BY
            il.product_id, p.name, p.sku, il.event_type, DATE(il.event_timestamp)
        ORDER BY event_date DESC, product_name
    """,

    # 5. Payment Success Rates — success vs failure per payment method
    "payment_success_rates": f"""
        SELECT
            payment_method,
            payment_status,
            COUNT(*)                                    AS transaction_count,
            SUM(amount)                                 AS total_amount,
            AVG(amount)                                 AS avg_amount,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (PARTITION BY payment_method),
            2)                                          AS pct_of_method
        FROM {S}.payments
        GROUP BY payment_method, payment_status
        ORDER BY payment_method, payment_status
    """,

    # 6. Shipping Performance — event counts and carrier activity per day
    "shipping_performance": f"""
        SELECT
            carrier,
            event_type,
            DATE(event_timestamp)                       AS event_date,
            COUNT(*)                                    AS event_count,
            COUNT(DISTINCT order_id)                    AS orders_affected
        FROM {S}.shipping_events
        GROUP BY carrier, event_type, DATE(event_timestamp)
        ORDER BY event_date DESC, carrier, event_type
    """,
}


def ctas_sql(table_name: str) -> str:
    return f"""
        CREATE TABLE {GOLD_DATABASE}.{table_name}
        WITH (
            format            = 'PARQUET',
            external_location = '{gold_location(table_name)}'
        )
        AS
        {GOLD_TABLES[table_name]}
    """


def verify_counts() -> None:
    print(f"\n  {'TABLE':<30} {'ROWS':>8}", flush=True)
    print(f"  {'-'*30} {'-'*8}", flush=True)
    for table in GOLD_TABLES:
        rows = fetch_rows(
            f"SELECT COUNT(*) AS cnt FROM {GOLD_DATABASE}.{table}",
            f"count {table}",
        )
        count = rows[1]["Data"][0]["VarCharValue"] if len(rows) > 1 else "?"
        print(f"  {table:<30} {count:>8}", flush=True)


def main() -> None:
    # STEP 0 — create gold database
    print("=" * 60, flush=True)
    print("STEP 0: Creating gold database...", flush=True)
    print("=" * 60, flush=True)
    if not run(
        f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}",
        f"CREATE DATABASE {GOLD_DATABASE}",
        database=GOLD_DATABASE,
    ):
        raise SystemExit(1)

    # STEP 1 — build each gold table
    print("\n" + "=" * 60, flush=True)
    print("STEP 1: Building gold Parquet tables...", flush=True)
    print("=" * 60, flush=True)
    for table in GOLD_TABLES:
        if not run(f"DROP TABLE IF EXISTS {GOLD_DATABASE}.{table}", f"DROP TABLE {table}"):
            raise SystemExit(1)
        try:
            delete_s3_prefix(gold_location(table))
        except (BotoCoreError, ClientError, ValueError) as exc:
            print(f"  FAIL  CLEAN {gold_location(table)}\n        {exc}", flush=True)
            raise SystemExit(1)
        if not run(ctas_sql(table), f"CTAS {table}"):
            raise SystemExit(1)

    # STEP 2 — verify row counts
    print("\n" + "=" * 60, flush=True)
    print("STEP 2: Verifying gold row counts...", flush=True)
    print("=" * 60, flush=True)
    verify_counts()

    print("\nDone!", flush=True)


if __name__ == "__main__":
    main()
