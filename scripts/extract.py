"""
Extraction step — mock CSVs (Data/) -> MinIO raw-zone, partitioned by date.

This is Step 1 of the confirmed pipeline (plan.md Section 6):
    Mock CSVs -> extract.py -> MinIO raw-zone -> DuckDB/dbt -> load.py -> MSSQL marts

Run this from the host (not inside a container) against MinIO's published port —
see docker-compose.yml. Requires the MinIO container to be up:
    docker compose up -d minio

Environment variables expected (set in .env or exported before running):
    MINIO_ENDPOINT        e.g. "http://localhost:9000"
    MINIO_ACCESS_KEY
    MINIO_SECRET_KEY
    MINIO_BUCKET_RAW      default "raw-zone"
    MINIO_BUCKET_STAGING  default "staging-zone"
"""

import os
from datetime import date
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_RAW = os.environ.get("MINIO_BUCKET_RAW", "raw-zone")
BUCKET_STAGING = os.environ.get("MINIO_BUCKET_STAGING", "staging-zone")

DATA_DIR = Path(__file__).resolve().parent.parent / "Data"

CSV_FILES = {
    "participants": "participants.csv",
    "policies": "policies.csv",
    "contributions": "contributions.csv",
    "claims": "claims.csv",
    "agency_transactions": "agency_transactions.csv",
}


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket_name):
    """Creates the bucket if it doesn't exist yet — idempotent."""
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"  Bucket already exists: {bucket_name}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket_name)
            print(f"  Created bucket: {bucket_name}")
        else:
            raise


def upload_csv(client, table_name, csv_filename, run_date):
    """Uploads one CSV to raw-zone/{table}/{yyyy-mm-dd}/{table}.csv.

    Re-running on the same day overwrites that day's partition (idempotent);
    re-running on a later day creates a new partition, preserving history.
    """
    csv_path = DATA_DIR / csv_filename
    with open(csv_path, "rb") as f:
        row_count = sum(1 for _ in f) - 1  # minus header

    key = f"{table_name}/{run_date.isoformat()}/{csv_filename}"
    client.upload_file(str(csv_path), BUCKET_RAW, key)
    print(f"  Uploaded {row_count:>6} rows -> s3://{BUCKET_RAW}/{key}")


def run_extraction():
    print(f"MinIO endpoint: {MINIO_ENDPOINT}\n")
    client = get_client()

    print("Step 1: Ensuring buckets exist...")
    ensure_bucket(client, BUCKET_RAW)
    ensure_bucket(client, BUCKET_STAGING)

    print("\nStep 2: Uploading CSVs to raw-zone...")
    run_date = date.today()
    for table_name, csv_filename in CSV_FILES.items():
        upload_csv(client, table_name, csv_filename, run_date)

    print(f"\nDone. Raw data landed in MinIO under '{BUCKET_RAW}/*/{run_date.isoformat()}/'.")
    print("Next step: DuckDB/dbt reads FROM these raw-zone files to build staging + mart models.")


if __name__ == "__main__":
    run_extraction()
