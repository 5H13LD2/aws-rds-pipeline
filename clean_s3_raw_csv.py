import csv
import io
import os
import re
from pathlib import Path

import boto3

TABLES = {
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
}


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
        value = pattern.sub(lambda match: loaded.get(match.group(1), os.environ.get(match.group(1), "")), value)
        os.environ.setdefault(key, value)
        loaded[key] = os.environ[key]


def clean_field(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def clean_csv_bytes(content: bytes) -> tuple[bytes, int]:
    source = io.StringIO(content.decode("utf-8"))
    target = io.StringIO()
    reader = csv.reader(source)
    writer = csv.writer(target, lineterminator="\n")

    row_count = 0
    for row in reader:
        writer.writerow([clean_field(value) for value in row])
        row_count += 1

    return target.getvalue().encode("utf-8"), max(row_count - 1, 0)


def main() -> None:
    load_env()
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
    bucket = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake")
    raw_prefix = os.environ.get("ATHENA_RAW_PREFIX", "raw").strip("/")

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")

    cleaned_files = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{raw_prefix}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            parts = key.split("/")
            if len(parts) < 4 or parts[0] != raw_prefix or parts[1] not in TABLES:
                continue

            response = s3.get_object(Bucket=bucket, Key=key)
            original = response["Body"].read()
            cleaned, row_count = clean_csv_bytes(original)
            s3.put_object(Bucket=bucket, Key=key, Body=cleaned, ContentType="text/csv")
            cleaned_files += 1
            print(f"OK  s3://{bucket}/{key} ({row_count} rows)", flush=True)

    print(f"\nCleaned {cleaned_files} raw CSV files.", flush=True)


if __name__ == "__main__":
    main()
