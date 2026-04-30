# AWS Ecommerce Data Platform

A sample AWS-native ecommerce data platform project scaffold.

## Architecture

- Python fake data generator
- Amazon RDS PostgreSQL source database
- Apache Airflow orchestration
- Amazon S3 raw data layer
- Amazon Redshift warehouse
- Gold/analytics tables for dashboard reporting

## Project Structure

- `dags/` - Airflow DAG definitions
- `scripts/` - Python scripts for data generation and RDS seeding
- `sql/` - SQL DDL for RDS, Redshift, and gold tables
- `docker/` - Docker-related configuration and images
- `docs/` - Documentation and architecture notes
- `README.md` - Project overview
- `docker-compose.yml` - Local orchestration definition
- `.env.example` - Environment variable template

## Next steps

1. Define the RDS schema in `sql/rds_schema.sql`.
2. Build the faker data generator in `scripts/generate_fake_data.py`.
3. Create the RDS seed loader in `scripts/seed_rds.py`.
4. Add Airflow DAGs to extract from RDS and load to S3.
5. Build Redshift schema SQL in `sql/redshift_schema.sql`.
6. Create gold analytics models in `sql/gold_models.sql`.

## Local setup

Create a Python virtual environment and install dependencies:

```bash
cd /home/jerico/Desktop/aws-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

The DAG `dags/seed_rds_fake_data.py` generates fake ecommerce data and seeds it into the `postgres_rds` RDS/PostgreSQL connection. Trigger it manually from Airflow; by default it truncates existing ecommerce tables with `RESTART IDENTITY` before inserting fresh rows. Set `fresh=true` to drop and recreate the schema before seeding, or set both `fresh=false` and `truncate=false` to append rows. You can override row counts in the trigger form.

The DAG `dags/rds_to_s3_raw.py` exports seeded RDS tables to S3 or local raw CSV files.

Start Airflow locally using Docker Compose:

```bash
docker compose up airflow-init

docker compose up
```

Open the Airflow UI at `http://localhost:8081`.

Airflow configuration:
- Create a Postgres connection with ID `postgres_rds`.
- Create an AWS connection with ID `aws_default` if you want S3 export.
- Set the Airflow Variable `s3_bucket` to your S3 bucket name to enable S3 upload.
- If `s3_bucket` is not configured, the DAG writes raw CSV files locally to `output/raw/{table}/{run_date}/`.

Airflow DAGs should be placed in `dags/` and are loaded automatically by the Airflow scheduler.
