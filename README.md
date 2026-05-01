# AWS Ecommerce Data Platform

End-to-end ecommerce data engineering project that simulates a production-style AWS analytics pipeline. The platform generates transactional source data, loads it into a PostgreSQL database that represents Amazon RDS, orchestrates extraction with Apache Airflow, and lands raw CSV extracts in an S3-style bronze data lake path for downstream Redshift and gold-layer analytics.

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
        +--> Amazon S3 Bronze Layer
        |
        +--> Local fallback: output/raw/{table}/dt={run_date}/
        |
        v
Amazon Redshift / Analytics Models
```

## Technical Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| Source simulation | Python, Faker | Generates realistic ecommerce entities and event data |
| OLTP source | PostgreSQL / Amazon RDS pattern | Stores normalized transactional tables with keys and constraints |
| Orchestration | Apache Airflow 2.10.2 | Schedules, parameterizes, and monitors data workflows |
| Storage | Amazon S3 bronze layer or local fallback | Persists raw table extracts partitioned by run date |
| Warehouse target | Amazon Redshift SQL scaffold | Defines the intended staging and analytics warehouse layer |
| Runtime | Docker Compose | Runs Airflow webserver, scheduler, metadata DB, and mounted project code |

## Data Model

The source schema models a normalized ecommerce system with referential integrity and basic data quality constraints.

Core tables:
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

The schema in `sql/rds_schema.sql` includes primary keys, foreign keys, uniqueness constraints, check constraints, timestamp columns, and controlled status values for orders, payments, shipping, inventory, and cart events.

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

This DAG exports each source table into raw CSV format. It validates the configured S3 bucket when available and falls back to local storage when no bucket is configured.

Raw object layout:

```text
raw/{table_name}/dt={run_date}/{table_name}.csv
```

Key implementation details:
- Uses `PostgresHook` for RDS/PostgreSQL access.
- Uses PostgreSQL `COPY ... TO STDOUT WITH CSV HEADER` for efficient exports.
- Uses `S3Hook` for S3 uploads when `S3_BRONZE_BUCKET` or the Airflow `s3_bucket` variable is configured.
- Emits per-table row count metrics through Airflow Stats.
- Runs one export task per table for clear observability in the Airflow graph.

![Convert source data to raw CSV](screenshots/convert_data_to_raw_csv.png)

### 3. Land Raw Data in the Data Lake

The bronze layer is designed as immutable, date-partitioned raw storage. In local demo mode, the same layout is written under `output/raw/`; in AWS mode, the files are uploaded to S3.

![Raw CSV landed in data lake](screenshots/raw_csv_to_data_lake.png)

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
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
└── README.md
```

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

Open Airflow:

```text
http://localhost:8081
```

Default local Airflow user:

```text
username: admin
password: admin
```

## Airflow Configuration

Required connection:
- `postgres_rds` - PostgreSQL/RDS source connection.

Optional AWS connection:
- `aws_default` - AWS connection used by `S3Hook`.

S3 configuration options:
- Set `S3_BRONZE_BUCKET` in `.env`.
- Or set Airflow Variable `s3_bucket`.
- If neither is configured, raw extracts are written locally to `output/raw/{table}/dt={run_date}/`.

Recommended `.env` values:

```bash
AIRFLOW_UID=50000
AWS_DEFAULT_REGION=ap-southeast-1
S3_BRONZE_BUCKET=your-bronze-bucket-name
POSTGRES_RDS_LOGIN=your_user
POSTGRES_RDS_PASSWORD=your_password
POSTGRES_RDS_HOST=your_host
POSTGRES_RDS_PORT=5432
POSTGRES_RDS_SCHEMA=your_database
```

## Demo Runbook

1. Start the stack with `docker compose up -d --build`.
2. Open Airflow at `http://localhost:8081`.
3. Confirm the `postgres_rds` connection is configured.
4. Trigger `seed_rds_fake_data`.
5. Review inserted counts in the Airflow task logs.
6. Trigger `rds_to_s3_raw`.
7. Confirm CSV outputs in S3 or in `output/raw/`.
8. Walk through the screenshots to show the data moving from generation, to RDS, to raw data lake storage.

## Design Notes

- The platform uses a bronze-first pattern: raw extracts are preserved before transformation.
- The raw path is partitioned by logical Airflow run date for replayability and historical inspection.
- DAG parameters make the seed process repeatable for demos and scalable test data generation.
- Source schema constraints catch bad synthetic data before it reaches the lake.
- The Redshift and gold SQL files are present as the warehouse expansion layer for staging, dimensional modeling, and reporting aggregates.

## Future Enhancements

- Add Redshift `COPY` jobs from S3 bronze into staging tables.
- Implement silver cleansing models for typed, deduplicated warehouse tables.
- Build gold marts such as daily sales, customer lifetime value, top products, inventory movement, and payment success rates.
- Add Great Expectations or custom data quality checks after extract and before warehouse load.
- Add CI validation for DAG import, SQL syntax, and Python formatting.
