# AWS Ecommerce Data Platform

End-to-end ecommerce data engineering project that simulates a production-style AWS analytics pipeline. The platform generates transactional source data, loads it into a PostgreSQL database that represents Amazon RDS, orchestrates extraction with Apache Airflow, lands raw CSV extracts in S3, and builds a full Bronze → Silver → Gold lakehouse on Amazon Athena.

## Architecture Overview

```text
Faker Data Generator
        |
        v
PostgreSQL / Amazon RDS Source Schema
        |
        v
Apache Airflow DAGs
        |
        v
Raw CSV Extracts
        |
        v
Amazon S3 Bronze Layer (raw/immutable CSV)
        |
        v
Amazon Athena — Silver Layer (Parquet, typed + cleaned)
        |
        v
Amazon Athena — Gold Layer (Parquet, analytics aggregates)
```

## Technical Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Source simulation | Python, Faker | Generates realistic ecommerce entities and event data |
| OLTP source | PostgreSQL / Amazon RDS pattern | Stores normalized transactional tables with keys and constraints |
| Orchestration | Apache Airflow 2.10.2 | Schedules, parameterizes, and monitors data workflows |
| Storage | Amazon S3 (`ap-southeast-2`) | Immutable bronze layer partitioned by run date |
| Lakehouse | Amazon Athena + AWS Glue Catalog | Silver and gold CTAS Parquet tables for analytics |
| Runtime | Docker Compose | Runs Airflow webserver, scheduler, metadata DB, and mounted project code |

## S3 Layout

```text
s3://retail-logistics-data-lake/
  ├── raw/                          ← Bronze: immutable CSV extracts
  │   └── {table}/dt={run_date}/{table}.csv
  ├── silver/                       ← Silver: typed, cleaned Parquet
  │   └── {table}/
  ├── gold/                         ← Gold: analytics aggregates Parquet
  │   └── {table}/
  └── athena-results/               ← Athena query output location
```

## Athena Databases

| Database | Tables | Format | Description |
| --- | --- | --- | --- |
| `retail_logistics_raw` | 10 | CSV (external) | Bronze — points directly to raw S3 CSV |
| `retail_logistics_silver` | 10 | Parquet (CTAS) | Silver — proper types, cleaned text |
| `retail_logistics_gold` | 6 | Parquet (CTAS) | Gold — analytics-ready aggregates |

## Data Model

The source schema models a normalized ecommerce system with referential integrity and basic data quality constraints.

Core tables (all layers):
- `categories`
- `products`
- `users`
- `orders`
- `order_items`
- `payments`
- `shipping_events`
- `product_reviews`
- `inventory_logs`
- `cart_events`

Gold analytics tables:
- `daily_sales`
- `customer_lifetime_value`
- `top_products`
- `inventory_movement`
- `payment_success_rates`
- `shipping_performance`

## Row Counts (Current Seed)

| Table | Rows |
| --- | --- |
| users | 200 |
| products | 120 |
| categories | 10 |
| orders | 300 |
| order_items | 934 |
| payments | 300 |
| shipping_events | 730 |
| product_reviews | 180 |
| inventory_logs | 150 |
| cart_events | 240 |

---

## Pipeline Flow

### 1. Generate and Seed Source Data

Airflow DAG: `seed_rds_fake_data`

This DAG generates ecommerce source data and inserts it into the PostgreSQL/RDS source connection named `postgres_rds`.

Operational features:
- Parameterized row counts for users, products, orders, reviews, inventory logs, and cart events.
- `fresh=true` mode for dropping and recreating the schema.
- `truncate=true` mode for clearing existing records with `RESTART IDENTITY`.
- Append mode support with generated suffixes to avoid duplicate SKUs and emails.
- Batch inserts through `psycopg2.extras.execute_values`.
- Post-load table counts logged for validation.

![Fake data loaded to RDS](screenshots/fakedata_to_rds.png)

### 2. Extract RDS Tables to Raw CSV

Airflow DAG: `rds_to_s3_raw`

This DAG exports each source table into raw CSV format, partitioned by Airflow run date.

Raw object layout:

```text
raw/{table_name}/dt={run_date}/{table_name}.csv
```

Key implementation details:
- Uses `PostgresHook` for RDS/PostgreSQL access.
- Uses PostgreSQL `COPY ... TO STDOUT WITH CSV HEADER` for efficient exports.
- Uses `S3Hook` for uploads when `S3_BRONZE_BUCKET` or the Airflow `s3_bucket` variable is configured.
- Falls back to `output/raw/` locally when no bucket is configured.
- Emits per-table row count metrics through Airflow Stats.
- Runs one export task per table for clear observability in the Airflow graph.

![Convert source data to raw CSV](screenshots/convert_data_to_raw_csv.png)

### 3. Bronze Layer — Raw CSV in S3

The bronze layer is immutable, date-partitioned raw storage. Files are never modified after landing. Athena external tables in `retail_logistics_raw` point directly to these CSV files using `OpenCSVSerde`.

![Raw CSV landed in data lake](screenshots/raw_csv_to_data_lake.png)

### 4. Silver Layer — Cleaned Parquet in Athena

Script: `run_athena_silver.py`

Reads from `retail_logistics_raw` (bronze) and writes cleaned, typed Parquet tables into `retail_logistics_silver` using Athena CTAS.

Transformations applied:
- All `STRING` timestamps cast to `TIMESTAMP` using `TRY_CAST(regexp_replace(col, '\\+00$', '') AS TIMESTAMP)`
- Date fields cast to `DATE`
- Numeric fields cast to proper `DOUBLE` or `INTEGER` types
- Free-text fields (`description`, `body`, `title`) sanitized with `regexp_replace` to remove embedded newlines and tabs
- All text fields `TRIM`med
- Bronze CSVs in S3 are never modified — all cleaning happens inside CTAS queries only

### 5. Gold Layer — Analytics Aggregates in Athena

Script: `run_athena_gold.py`

Reads from `retail_logistics_silver` and writes analytics-ready Parquet tables into `retail_logistics_gold` using Athena CTAS.

| Gold Table | Description |
| --- | --- |
| `daily_sales` | Revenue, order count, and unique customers per day |
| `customer_lifetime_value` | Total spend, order history, and lifespan per customer |
| `top_products` | Revenue, units sold, ratings per product |
| `inventory_movement` | Stock change events per product per day |
| `payment_success_rates` | Success vs failure rates per payment method |
| `shipping_performance` | Carrier event counts and orders affected per day |

---

## Repository Structure

```text
.
├── dags/
│   ├── seed_rds_fake_data.py
│   └── rds_to_s3_raw.py
├── scripts/
│   ├── generate_fake_data.py
│   └── seed_rds.py
├── sql/
│   ├── rds_schema.sql
│   ├── redshift_schema.sql
│   └── gold_models.sql
├── screenshots/
│   ├── fakedata_to_rds.png
│   ├── convert_data_to_raw_csv.png
│   └── raw_csv_to_data_lake.png
├── run_athena_ddl.py           ← Creates bronze external tables in Athena
├── run_athena_silver.py        ← Builds silver Parquet layer
├── run_athena_gold.py          ← Builds gold analytics layer
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Local Setup

Create a virtual environment for local script execution:

```bash
cd /home/jerico/Desktop/aws-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Prepare local output permissions for Airflow bind mounts:

```bash
mkdir -p output/raw
sudo chown -R 50000:0 output
sudo chmod -R 775 output
```

Start the Airflow stack:

```bash
docker compose up -d --build
```

Open Airflow at `http://localhost:8081`

Default local Airflow user: `admin` / `admin`

---

## AWS Configuration

### IAM User

IAM user `aws-rds-athena` with the following policies attached:
- `AmazonAthenaFullAccess`
- `AWSGlueConsoleFullAccess`
- `AmazonS3FullAccess`

### .env File

```bash
AIRFLOW_UID=50000
AWS_DEFAULT_REGION=ap-southeast-2
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BRONZE_BUCKET=retail-logistics-data-lake
POSTGRES_RDS_LOGIN=your_user
POSTGRES_RDS_PASSWORD=your_password
POSTGRES_RDS_HOST=your_host
POSTGRES_RDS_PORT=5432
POSTGRES_RDS_SCHEMA=your_database
ATHENA_DATABASE=retail_logistics_raw
ATHENA_SILVER_DATABASE=retail_logistics_silver
ATHENA_GOLD_DATABASE=retail_logistics_gold
ATHENA_OUTPUT_LOCATION=s3://retail-logistics-data-lake/athena-results/
```

### Airflow Connections

- `postgres_rds` — PostgreSQL/RDS source connection (required)
- `aws_default` — AWS connection used by `S3Hook` (optional)

---

## Full Pipeline Runbook

### Step 1 — Start the stack

```bash
docker compose up -d --build
```

### Step 2 — Seed source data

Trigger `seed_rds_fake_data` in Airflow at `http://localhost:8081`. Review inserted counts in the task logs.

### Step 3 — Extract to S3 bronze

Trigger `rds_to_s3_raw` in Airflow. Confirm CSV outputs appear in `s3://retail-logistics-data-lake/raw/` or locally in `output/raw/`.

### Step 4 — Build Athena bronze layer

```bash
source .venv/bin/activate
python run_athena_ddl.py
```

### Step 5 — Build Athena silver layer

```bash
python run_athena_silver.py
```

### Step 6 — Build Athena gold layer

```bash
python run_athena_gold.py
```

---

## Athena Demo Queries

Run these in the **AWS Athena Query Editor** (`ap-southeast-2`). Set the database to the relevant layer before running.

### Bronze — Spot-check raw data

```sql
-- Verify all 10 tables loaded correctly
SELECT 'users'           AS table_name, COUNT(*) AS row_count FROM retail_logistics_raw.users
UNION ALL SELECT 'products',        COUNT(*) FROM retail_logistics_raw.products
UNION ALL SELECT 'categories',      COUNT(*) FROM retail_logistics_raw.categories
UNION ALL SELECT 'orders',          COUNT(*) FROM retail_logistics_raw.orders
UNION ALL SELECT 'order_items',     COUNT(*) FROM retail_logistics_raw.order_items
UNION ALL SELECT 'payments',        COUNT(*) FROM retail_logistics_raw.payments
UNION ALL SELECT 'shipping_events', COUNT(*) FROM retail_logistics_raw.shipping_events
UNION ALL SELECT 'product_reviews', COUNT(*) FROM retail_logistics_raw.product_reviews
UNION ALL SELECT 'inventory_logs',  COUNT(*) FROM retail_logistics_raw.inventory_logs
UNION ALL SELECT 'cart_events',     COUNT(*) FROM retail_logistics_raw.cart_events;
```

### Silver — Verify type casting worked

```sql
-- Timestamps are now proper TIMESTAMP, not raw strings
SELECT
    user_id,
    first_name,
    last_name,
    email,
    created_at,           -- TIMESTAMP
    typeof(created_at)    -- should return 'timestamp'
FROM retail_logistics_silver.users
LIMIT 5;
```

### Gold — Daily revenue trend

```sql
SELECT
    sale_date,
    total_orders,
    unique_customers,
    ROUND(total_revenue, 2)   AS total_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value
FROM retail_logistics_gold.daily_sales
ORDER BY sale_date DESC
LIMIT 10;
```

### Gold — Top 5 customers by lifetime value

```sql
SELECT
    user_id,
    first_name || ' ' || last_name AS full_name,
    country,
    total_orders,
    ROUND(lifetime_value, 2) AS lifetime_value,
    first_order_date,
    last_order_date
FROM retail_logistics_gold.customer_lifetime_value
ORDER BY lifetime_value DESC
LIMIT 5;
```

### Gold — Best selling products

```sql
SELECT
    product_name,
    category_name,
    sku,
    total_units_sold,
    total_orders,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_rating, 2)    AS avg_rating,
    total_reviews
FROM retail_logistics_gold.top_products
ORDER BY total_revenue DESC
LIMIT 10;
```

### Gold — Payment method success rates

```sql
SELECT
    payment_method,
    payment_status,
    transaction_count,
    ROUND(total_amount, 2)  AS total_amount,
    ROUND(pct_of_method, 2) AS pct_of_method
FROM retail_logistics_gold.payment_success_rates
ORDER BY payment_method, payment_status;
```

### Gold — Carrier shipping performance

```sql
SELECT
    carrier,
    event_type,
    SUM(event_count)      AS total_events,
    SUM(orders_affected)  AS total_orders_affected
FROM retail_logistics_gold.shipping_performance
GROUP BY carrier, event_type
ORDER BY carrier, event_type;
```

### Gold — Low stock products (from silver)

```sql
SELECT
    product_id,
    name,
    sku,
    inventory_quantity
FROM retail_logistics_silver.products
WHERE inventory_quantity < 20
ORDER BY inventory_quantity ASC;
```

---

## Design Notes

- **Bronze-first pattern** — raw extracts are preserved as immutable, date-partitioned CSV before any transformation.
- **Silver cleans, never touches source** — all type casting and text sanitization happens inside Athena CTAS queries. S3 raw files are never modified.
- **Gold reads from silver only** — analytics aggregates are always built on top of the cleaned silver layer, not raw bronze.
- **Re-runnable scripts** — all three pipeline scripts (`run_athena_ddl.py`, `run_athena_silver.py`, `run_athena_gold.py`) drop and recreate tables and clean S3 prefixes before each run, making them safe to re-run at any time.
- **Parquet for silver and gold** — columnar format reduces Athena query costs and improves scan performance vs CSV.
- **TRY_CAST everywhere** — silver uses `TRY_CAST` for all type conversions so bad rows return NULL instead of crashing the entire CTAS.

## Future Enhancements

- Add Airflow DAGs to trigger silver and gold scripts automatically after `rds_to_s3_raw` completes.
- Add Great Expectations or custom data quality checks between bronze and silver.
- Build a QuickSight dashboard on top of the gold layer for visual reporting.
- Add CI validation for DAG import, SQL syntax, and Python formatting.
- Implement incremental silver/gold loads instead of full rebuilds.