import os

# ─── Database ────────────────────────────────────────────────────────────────
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@superset-db:5432/superset"

# ─── Security ────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "retail_logistics_superset_secret_key_2024")

# ─── Feature Flags ───────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ─── Athena connection string (for reference in UI setup) ────────────────────
# awsathena+rest://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}@athena.{region}.amazonaws.com/{database}
# ?s3_staging_dir={s3_output_location}&work_group=primary

ATHENA_REGION        = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2")
ATHENA_OUTPUT        = os.environ.get("ATHENA_OUTPUT_LOCATION", "")
AWS_KEY              = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET           = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# Expose as a ready-to-paste SQLAlchemy URI for the gold database
ATHENA_GOLD_URI = (
    f"awsathena+rest://{AWS_KEY}:{AWS_SECRET}"
    f"@athena.{ATHENA_REGION}.amazonaws.com/retail_logistics_gold"
    f"?s3_staging_dir={ATHENA_OUTPUT}&work_group=primary"
)
