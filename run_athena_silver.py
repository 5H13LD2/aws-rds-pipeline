import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def load_env(path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from .env without requiring python-dotenv."""
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

        value = pattern.sub(lambda match: loaded.get(match.group(1), os.environ.get(match.group(1), "")), value)
        os.environ.setdefault(key, value)
        loaded[key] = os.environ[key]


load_env()

REGION = os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"
S3_BUCKET = os.environ.get("S3_BRONZE_BUCKET") or "retail-logistics-data-lake"
BRONZE_DATABASE = os.environ.get("ATHENA_DATABASE") or "retail_logistics_raw"
SILVER_DATABASE = os.environ.get("ATHENA_SILVER_DATABASE") or "retail_logistics_silver"
SILVER_PREFIX = (os.environ.get("ATHENA_SILVER_PREFIX") or "silver").strip("/")
OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT_LOCATION") or f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def silver_location(table_name: str) -> str:
    return f"s3://{S3_BUCKET}/{SILVER_PREFIX}/{table_name}/"


def clean_ts(column_name: str) -> str:
    return f"TRY_CAST(regexp_replace({column_name}, '\\\\+00$', '') AS TIMESTAMP)"


def clean_date(column_name: str) -> str:
    return f"CAST({clean_ts(column_name)} AS DATE)"


def run(statement: str, label: str) -> bool:
    try:
        resp = athena.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": BRONZE_DATABASE},
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


def fetch_rows(statement: str, label: str):
    try:
        resp = athena.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": BRONZE_DATABASE},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc

    exec_id = resp["QueryExecutionId"]
    while True:
        try:
            result = athena.get_query_execution(QueryExecutionId=exec_id)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"{label} failed: {exc}") from exc

        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"{label} failed: {reason}")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    try:
        for page in paginator.paginate(QueryExecutionId=exec_id):
            rows.extend(page["ResultSet"]["Rows"])
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc

    return rows


def delete_s3_prefix(s3_uri: str) -> None:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket or not prefix:
        raise ValueError(f"Refusing to delete unsafe S3 prefix: {s3_uri}")

    paginator = s3.get_paginator("list_objects_v2")
    delete_batch = []
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            delete_batch.append({"Key": obj["Key"]})
            if len(delete_batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_batch})
                deleted += len(delete_batch)
                delete_batch = []

    if delete_batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_batch})
        deleted += len(delete_batch)

    print(f"  OK    CLEAN {s3_uri} ({deleted} objects)", flush=True)


TABLES = [
    "users",
    "products",
    "categories",
    "orders",
    "order_items",
    "payments",
    "shipping_events",
    "product_reviews",
    "inventory_logs",
    "cart_events",
]

CTAS_SELECTS = {
    "users": f"""
SELECT
  CAST(user_id AS BIGINT) AS user_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  TRIM(email) AS email,
  TRIM(phone) AS phone,
  TRIM(address) AS address,
  TRIM(city) AS city,
  TRIM(state) AS state,
  TRIM(country) AS country,
  TRIM(postal_code) AS postal_code,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.users
""",
    "products": f"""
SELECT
  CAST(product_id AS BIGINT) AS product_id,
  CAST(category_id AS BIGINT) AS category_id,
  TRIM(regexp_replace(name,        '[\\r\\n\\t]+', ' ')) AS name,
  TRIM(regexp_replace(description, '[\\r\\n\\t]+', ' ')) AS description,
  CAST(price AS DOUBLE) AS price,
  TRIM(sku) AS sku,
  CAST(inventory_quantity AS INTEGER) AS inventory_quantity,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.products
""",
    "categories": f"""
SELECT
  CAST(category_id AS BIGINT) AS category_id,
  TRIM(name) AS name,
  TRIM(description) AS description,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.categories
""",
    "orders": f"""
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(user_id AS BIGINT) AS user_id,
  {clean_date("order_date")} AS order_date,
  TRIM(status) AS status,
  TRIM(currency) AS currency,
  CAST(subtotal AS DOUBLE) AS subtotal,
  CAST(tax AS DOUBLE) AS tax,
  CAST(shipping_cost AS DOUBLE) AS shipping_cost,
  CAST(total_amount AS DOUBLE) AS total_amount,
  TRIM(shipping_address) AS shipping_address,
  TRIM(billing_address) AS billing_address,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.orders
""",
    "order_items": f"""
SELECT
  CAST(order_item_id AS BIGINT) AS order_item_id,
  CAST(order_id AS BIGINT) AS order_id,
  CAST(product_id AS BIGINT) AS product_id,
  CAST(quantity AS INTEGER) AS quantity,
  CAST(unit_price AS DOUBLE) AS unit_price,
  CAST(total_price AS DOUBLE) AS total_price,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.order_items
""",
    "payments": f"""
SELECT
  CAST(payment_id AS BIGINT) AS payment_id,
  CAST(order_id AS BIGINT) AS order_id,
  TRIM(payment_method) AS payment_method,
  TRIM(payment_status) AS payment_status,
  CAST(amount AS DOUBLE) AS amount,
  TRIM(currency) AS currency,
  TRIM(transaction_id) AS transaction_id,
  {clean_ts("processed_at")} AS processed_at,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.payments
""",
    "shipping_events": f"""
SELECT
  CAST(shipping_event_id AS BIGINT) AS shipping_event_id,
  CAST(order_id AS BIGINT) AS order_id,
  TRIM(event_type) AS event_type,
  {clean_ts("event_timestamp")} AS event_timestamp,
  TRIM(carrier) AS carrier,
  TRIM(tracking_number) AS tracking_number,
  TRIM(location) AS location,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.shipping_events
""",
    "product_reviews": f"""
SELECT
  CAST(review_id AS BIGINT) AS review_id,
  CAST(product_id AS BIGINT) AS product_id,
  CAST(user_id AS BIGINT) AS user_id,
  CAST(rating AS INTEGER) AS rating,
  TRIM(regexp_replace(title, '[\\r\\n\\t]+', ' ')) AS title,
  TRIM(regexp_replace(body,  '[\\r\\n\\t]+', ' ')) AS body,
  {clean_date("review_date")} AS review_date,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.product_reviews
""",
    "inventory_logs": f"""
SELECT
  CAST(inventory_log_id AS BIGINT) AS inventory_log_id,
  CAST(product_id AS BIGINT) AS product_id,
  TRIM(event_type) AS event_type,
  CAST(quantity AS INTEGER) AS quantity,
  CAST(previous_quantity AS INTEGER) AS previous_quantity,
  CAST(new_quantity AS INTEGER) AS new_quantity,
  {clean_ts("event_timestamp")} AS event_timestamp,
  TRIM(notes) AS notes,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.inventory_logs
""",
    "cart_events": f"""
SELECT
  CAST(cart_event_id AS BIGINT) AS cart_event_id,
  CAST(user_id AS BIGINT) AS user_id,
  CAST(product_id AS BIGINT) AS product_id,
  TRIM(event_type) AS event_type,
  CAST(quantity AS INTEGER) AS quantity,
  {clean_ts("event_timestamp")} AS event_timestamp,
  TRIM(session_id) AS session_id,
  {clean_ts("created_at")} AS created_at,
  {clean_ts("updated_at")} AS updated_at,
  dt
FROM {BRONZE_DATABASE}.cart_events
""",
}


def ctas_sql(table_name: str) -> str:
    return f"""
CREATE TABLE {SILVER_DATABASE}.{table_name}
WITH (
  format = 'PARQUET',
  external_location = '{silver_location(table_name)}'
)
AS
{CTAS_SELECTS[table_name]}
"""


def verify_counts() -> None:
    union_parts = []
    for table in TABLES:
        union_parts.append(
            f"""
SELECT
  '{table}' AS table_name,
  (SELECT COUNT(*) FROM {BRONZE_DATABASE}.{table}) AS bronze_rows,
  (SELECT COUNT(*) FROM {SILVER_DATABASE}.{table}) AS silver_rows
"""
        )

    rows = fetch_rows("\nUNION ALL\n".join(union_parts), "Verify silver row counts")

    print(f"\n  {'TABLE':<25} {'BRONZE':>10} {'SILVER':>10} {'STATUS':>8}", flush=True)
    print(f"  {'-' * 25} {'-' * 10} {'-' * 10} {'-' * 8}", flush=True)
    mismatches = []
    for row in rows[1:]:
        table_name = row["Data"][0]["VarCharValue"]
        bronze_rows = int(row["Data"][1]["VarCharValue"])
        silver_rows = int(row["Data"][2]["VarCharValue"])
        status = "OK" if bronze_rows == silver_rows else "MISMATCH"
        if status != "OK":
            mismatches.append(table_name)
        print(f"  {table_name:<25} {bronze_rows:>10} {silver_rows:>10} {status:>8}", flush=True)

    if mismatches:
        raise SystemExit(f"Silver row count mismatch: {', '.join(mismatches)}")


def main() -> None:
    print("=" * 60, flush=True)
    print("STEP 0: Creating silver database...", flush=True)
    print("=" * 60, flush=True)
    if not run(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}", f"CREATE DATABASE {SILVER_DATABASE}"):
        raise SystemExit(1)

    print("\n" + "=" * 60, flush=True)
    print("STEP 1: Rebuilding silver Parquet tables...", flush=True)
    print("=" * 60, flush=True)
    for table in TABLES:
        if not run(f"DROP TABLE IF EXISTS {SILVER_DATABASE}.{table}", f"DROP TABLE {table}"):
            raise SystemExit(1)
        try:
            delete_s3_prefix(silver_location(table))
        except (BotoCoreError, ClientError, ValueError) as exc:
            print(f"  FAIL  CLEAN {silver_location(table)}\n        {exc}", flush=True)
            raise SystemExit(1)
        if not run(ctas_sql(table), f"CTAS {table}"):
            raise SystemExit(1)

    print("\n" + "=" * 60, flush=True)
    print("STEP 2: Verifying bronze vs silver row counts...", flush=True)
    print("=" * 60, flush=True)
    verify_counts()

    print("\nDone!", flush=True)


if __name__ == "__main__":
    main()
