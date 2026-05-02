import boto3
import os
import re
import time
from botocore.exceptions import BotoCoreError, ClientError
from pathlib import Path


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

# ─── CONFIG ───────────────────────────────────────────────────────────────────
REGION = os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"
DATABASE = os.environ.get("ATHENA_DATABASE") or "retail_logistics_raw"
S3_BUCKET = os.environ.get("S3_BRONZE_BUCKET") or "retail-logistics-data-lake"
RAW_PREFIX = (os.environ.get("ATHENA_RAW_PREFIX") or "raw").strip("/")
OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT_LOCATION") or f"s3://{S3_BUCKET}/athena-results/"
# ──────────────────────────────────────────────────────────────────────────────

client = boto3.client("athena", region_name=REGION)


def table_location(table_name: str) -> str:
    return f"s3://{S3_BUCKET}/{RAW_PREFIX}/{table_name}/"


def run(statement: str, label: str = "") -> bool:
    """Execute a single Athena statement and wait for completion."""
    try:
        resp = client.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"  ✗  {label}\n     → {exc}")
        return False

    exec_id = resp["QueryExecutionId"]

    while True:
        try:
            result = client.get_query_execution(QueryExecutionId=exec_id)
        except (BotoCoreError, ClientError) as exc:
            print(f"  ✗  {label}\n     → {exc}")
            return False
        state  = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state == "SUCCEEDED":
        print(f"  ✓  {label}")
        return True
    else:
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
        print(f"  ✗  {label}\n     → {reason}")
        return False


def fetch_results(statement: str, label: str):
    """Execute a SELECT statement and return all result rows."""
    try:
        resp = client.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc

    exec_id = resp["QueryExecutionId"]

    while True:
        try:
            result = client.get_query_execution(QueryExecutionId=exec_id)
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
    paginator = client.get_paginator("get_query_results")
    try:
        for page in paginator.paginate(QueryExecutionId=exec_id):
            rows.extend(page["ResultSet"]["Rows"])
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc
    return rows


# ─── 1. CREATE EXTERNAL TABLES ────────────────────────────────────────────────
TABLES = [
    "users", "products", "categories", "orders", "order_items",
    "payments", "shipping_events", "product_reviews", "inventory_logs", "cart_events",
]

DDL_STATEMENTS = [
    ("users", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.users (
          user_id     BIGINT,
          first_name  STRING,
          last_name   STRING,
          email       STRING,
          phone       STRING,
          address     STRING,
          city        STRING,
          state       STRING,
          country     STRING,
          postal_code STRING,
          created_at  STRING,
          updated_at  STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("users")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("products", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.products (
          product_id         BIGINT,
          category_id        BIGINT,
          name               STRING,
          description        STRING,
          price              DOUBLE,
          sku                STRING,
          inventory_quantity INT,
          created_at         STRING,
          updated_at         STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("products")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("categories", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.categories (
          category_id BIGINT,
          name        STRING,
          description STRING,
          created_at  STRING,
          updated_at  STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("categories")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("orders", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.orders (
          order_id         BIGINT,
          user_id          BIGINT,
          order_date       STRING,
          status           STRING,
          currency         STRING,
          subtotal         DOUBLE,
          tax              DOUBLE,
          shipping_cost    DOUBLE,
          total_amount     DOUBLE,
          shipping_address STRING,
          billing_address  STRING,
          created_at       STRING,
          updated_at       STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("orders")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("order_items", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.order_items (
          order_item_id BIGINT,
          order_id      BIGINT,
          product_id    BIGINT,
          quantity      INT,
          unit_price    DOUBLE,
          total_price   DOUBLE,
          created_at    STRING,
          updated_at    STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("order_items")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("payments", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.payments (
          payment_id     BIGINT,
          order_id       BIGINT,
          payment_method STRING,
          payment_status STRING,
          amount         DOUBLE,
          currency       STRING,
          transaction_id STRING,
          processed_at   STRING,
          created_at     STRING,
          updated_at     STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("payments")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("shipping_events", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.shipping_events (
          shipping_event_id BIGINT,
          order_id          BIGINT,
          event_type        STRING,
          event_timestamp   STRING,
          carrier           STRING,
          tracking_number   STRING,
          location          STRING,
          created_at        STRING,
          updated_at        STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("shipping_events")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("product_reviews", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.product_reviews (
          review_id   BIGINT,
          product_id  BIGINT,
          user_id     BIGINT,
          rating      INT,
          title       STRING,
          body        STRING,
          review_date STRING,
          created_at  STRING,
          updated_at  STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("product_reviews")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("inventory_logs", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.inventory_logs (
          inventory_log_id  BIGINT,
          product_id        BIGINT,
          event_type        STRING,
          quantity          INT,
          previous_quantity INT,
          new_quantity      INT,
          event_timestamp   STRING,
          notes             STRING,
          created_at        STRING,
          updated_at        STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("inventory_logs")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
    ("cart_events", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.cart_events (
          cart_event_id   BIGINT,
          user_id         BIGINT,
          product_id      BIGINT,
          event_type      STRING,
          quantity        INT,
          event_timestamp STRING,
          session_id      STRING,
          created_at      STRING,
          updated_at      STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='"')
        LOCATION '{table_location("cart_events")}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """),
]

print("=" * 50)
print("STEP 0: Ensuring Athena database exists...")
print("=" * 50)
if not run(f"CREATE DATABASE IF NOT EXISTS {DATABASE}", label=f"CREATE DATABASE {DATABASE}"):
    print("Cannot continue until the Athena database can be created or verified.")
    raise SystemExit(1)

print("=" * 50)
print("STEP 1: Creating external tables...")
print("=" * 50)
for table in TABLES:
    run(f"DROP TABLE IF EXISTS {DATABASE}.{table}", label=f"DROP TABLE {table}")

for table, ddl in DDL_STATEMENTS:
    run(ddl, label=f"CREATE TABLE {table}")


# ─── 2. MSCK REPAIR (register partitions) ────────────────────────────────────
print("\n" + "=" * 50)
print("STEP 2: Registering partitions (MSCK REPAIR)...")
print("=" * 50)
for table in TABLES:
    run(f"MSCK REPAIR TABLE {DATABASE}.{table}", label=f"REPAIR {table}")


# ─── 3. ROW COUNT VERIFICATION ────────────────────────────────────────────────
VERIFY_SQL = f"""
SELECT 'users'           AS table_name, COUNT(*) AS row_count FROM {DATABASE}.users
UNION ALL SELECT 'products',        COUNT(*) FROM {DATABASE}.products
UNION ALL SELECT 'categories',      COUNT(*) FROM {DATABASE}.categories
UNION ALL SELECT 'orders',          COUNT(*) FROM {DATABASE}.orders
UNION ALL SELECT 'order_items',     COUNT(*) FROM {DATABASE}.order_items
UNION ALL SELECT 'payments',        COUNT(*) FROM {DATABASE}.payments
UNION ALL SELECT 'shipping_events', COUNT(*) FROM {DATABASE}.shipping_events
UNION ALL SELECT 'product_reviews', COUNT(*) FROM {DATABASE}.product_reviews
UNION ALL SELECT 'inventory_logs',  COUNT(*) FROM {DATABASE}.inventory_logs
UNION ALL SELECT 'cart_events',     COUNT(*) FROM {DATABASE}.cart_events
"""

print("\n" + "=" * 50)
print("STEP 3: Verifying row counts...")
print("=" * 50)

try:
    rows = fetch_results(VERIFY_SQL, "Verify row counts")
    print(f"\n  {'TABLE':<25} {'ROWS':>8}")
    print(f"  {'-'*25} {'-'*8}")
    for row in rows[1:]:  # skip header
        tname = row["Data"][0]["VarCharValue"]
        count = row["Data"][1]["VarCharValue"]
        print(f"  {tname:<25} {count:>8}")
except RuntimeError as exc:
    print(f"  ✗  {exc}")

print("\nDone!")
