"""
Load step — dbt marts (local DuckDB) -> Parquet (MinIO staging-zone, audit
copy) -> bulk-load into MSSQL CuratedTakafulPOC.marts.

This is Step 4 of the confirmed pipeline (plan.md Section 6):
    ... -> dbt (transform + test) -> load.py -> MSSQL CuratedTakafulPOC.marts -> Power BI

Run this AFTER `dbt run` and `dbt test` have both succeeded — dbt test is the
hard gate; this script does not re-validate the data, it trusts dbt's tests
already passed (see plan.md Section 6 Step 5, the Airflow DAG: test >> load).

Raw and curated data live in separate MSSQL databases on the same server
(TakafulPOC for raw, CuratedTakafulPOC for curated) — a deliberate split so
they can carry different access controls. See CLAUDE.md "Database schema".

Environment variables expected (set in .env or exported before running):
    MSSQL_SERVER             e.g. "localhost,1433"
    MSSQL_CURATED_DATABASE   e.g. "CuratedTakafulPOC"
    MSSQL_USER               e.g. "sa"
    MSSQL_PASSWORD
    MSSQL_ODBC_DRIVER        e.g. "ODBC Driver 17 for SQL Server"
    DBT_DUCKDB_PATH          path to the dbt-duckdb working file (dev target)
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_STAGING
"""

import os
import tempfile
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus

import boto3
import duckdb
import pandas as pd
import sqlalchemy as sa
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MSSQL_SERVER = os.environ.get("MSSQL_SERVER", "localhost,1433")
MSSQL_CURATED_DATABASE = os.environ.get("MSSQL_CURATED_DATABASE", "CuratedTakafulPOC")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_PASSWORD", "YourStrong!Passw0rd")
ODBC_DRIVER = os.environ.get("MSSQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
MARTS_SCHEMA = "marts"

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt_takaful"
DUCKDB_PATH = os.environ.get("DBT_DUCKDB_PATH")
if DUCKDB_PATH:
    # DBT_DUCKDB_PATH is relative to dbt_takaful/ in profiles.yml; resolve the
    # same way here so this script works whether it's run from repo root or
    # from within dbt_takaful/.
    DUCKDB_FILE = (DBT_PROJECT_DIR / DUCKDB_PATH).resolve()
else:
    DUCKDB_FILE = (DBT_PROJECT_DIR / ".." / "duckdb_data" / "takaful_transform.duckdb").resolve()

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_STAGING = os.environ.get("MINIO_BUCKET_STAGING", "staging-zone")

# Same URL-encoding + encryption-flag pattern as Migration_Script/load_raw_to_mssql.py
# (CLAUDE.md gotchas #1, #3) — a password containing '@' would otherwise silently
# corrupt the parsed connection string.
CONN_EXTRA = "&Encrypt=yes&TrustServerCertificate=yes"
_encoded_user = quote_plus(MSSQL_USER)
_encoded_password = quote_plus(MSSQL_PASSWORD)

MASTER_CONN_STR = (
    f"mssql+pyodbc://{_encoded_user}:{_encoded_password}@{MSSQL_SERVER}/master"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}{CONN_EXTRA}"
)
CURATED_CONN_STR = (
    f"mssql+pyodbc://{_encoded_user}:{_encoded_password}@{MSSQL_SERVER}/{MSSQL_CURATED_DATABASE}"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}{CONN_EXTRA}"
)

# ---------------------------------------------------------------------------
# Mart table definitions — explicit DDL, mirroring the raw loader's approach
# (Migration_Script/load_raw_to_mssql.py) rather than letting pandas guess types.
# Column types/lengths match what dbt actually produces (checked via `describe`
# against the local DuckDB marts) and the raw DDL's conventions where a column
# is shared (e.g. has_pif BIT, contribution_amount DECIMAL(12,2)).
# ---------------------------------------------------------------------------
TABLE_DDL = {
    "dim_policies": """
        CREATE TABLE marts.dim_policies (
            policy_sk            VARCHAR(64) PRIMARY KEY,
            policy_id            VARCHAR(20),
            participant_id       VARCHAR(20),
            product_name         NVARCHAR(100),
            product_category     NVARCHAR(20),
            has_pif              BIT,
            start_date           DATE,
            end_date             DATE,
            term_years           INT,
            payment_mode         NVARCHAR(20),
            status               NVARCHAR(20),
            agent_id             VARCHAR(20),
            contribution_amount  DECIMAL(12,2),
            valid_from           DATETIME2,
            valid_to             DATETIME2,
            is_current           BIT
        )
    """,
    "dim_participants": """
        CREATE TABLE marts.dim_participants (
            participant_sk   VARCHAR(64) PRIMARY KEY,
            participant_id   VARCHAR(20),
            full_name        NVARCHAR(200),
            ic_number        VARCHAR(20),
            state            NVARCHAR(50),
            date_of_birth    DATE,
            gender           CHAR(1),
            join_date        DATE,
            valid_from       DATETIME2,
            valid_to         DATETIME2,
            is_current       BIT
        )
    """,
    "dim_product": """
        CREATE TABLE marts.dim_product (
            product_sk        VARCHAR(64) PRIMARY KEY,
            product_name      NVARCHAR(100),
            product_category  NVARCHAR(20)
        )
    """,
    "dim_date": """
        CREATE TABLE marts.dim_date (
            date_day       DATE PRIMARY KEY,
            year           INT,
            quarter        INT,
            month          INT,
            month_name     VARCHAR(20),
            day_of_month   INT,
            day_of_week    INT,
            day_name       VARCHAR(20),
            is_weekend     BIT
        )
    """,
    "fct_contributions": """
        CREATE TABLE marts.fct_contributions (
            contribution_id                 VARCHAR(20) PRIMARY KEY,
            policy_id                       VARCHAR(20),
            policy_sk                       VARCHAR(64),
            contribution_date               DATE,
            gross_amount                    DECIMAL(12,2),
            prf_amount                      DECIMAL(12,2),
            pif_amount                      DECIMAL(12,2),
            wakalah_fee_shareholders_fund   DECIMAL(12,2),
            payment_method                  NVARCHAR(30)
        )
    """,
    "fct_claims": """
        CREATE TABLE marts.fct_claims (
            claim_id        VARCHAR(20) PRIMARY KEY,
            policy_id       VARCHAR(20),
            policy_sk       VARCHAR(64),
            claim_type      NVARCHAR(50),
            claim_date      DATE,
            claim_amount    DECIMAL(12,2),
            fund_type       VARCHAR(10),
            status          NVARCHAR(20),
            approval_date   DATE NULL
        )
    """,
    "fct_agency_commissions": """
        CREATE TABLE marts.fct_agency_commissions (
            transaction_id                          VARCHAR(20) PRIMARY KEY,
            agent_id                                VARCHAR(20),
            policy_id                               VARCHAR(20),
            policy_sk                               VARCHAR(64),
            transaction_type                        NVARCHAR(20),
            transaction_date                        DATE,
            commission_rate                         DECIMAL(5,2),
            commission_amount_shareholders_fund     DECIMAL(12,2)
        )
    """,
}

MART_TABLES = list(TABLE_DDL.keys())


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket_name):
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket_name)
            print(f"  Created bucket: {bucket_name}")
        else:
            raise


def ensure_curated_database_exists():
    """Connects to master and creates CuratedTakafulPOC if it doesn't exist yet —
    same pattern as Migration_Script/load_raw_to_mssql.py's ensure_database_exists()."""
    engine = sa.create_engine(MASTER_CONN_STR, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT database_id FROM sys.databases WHERE name = :name"),
            {"name": MSSQL_CURATED_DATABASE},
        ).fetchone()
        if result is None:
            print(f"Database '{MSSQL_CURATED_DATABASE}' not found — creating it...")
            conn.execute(text(f"CREATE DATABASE [{MSSQL_CURATED_DATABASE}]"))
        else:
            print(f"Database '{MSSQL_CURATED_DATABASE}' already exists.")
    engine.dispose()


def ensure_schema_and_tables(engine):
    """Creates the marts schema and drops/recreates each mart table for a clean
    reload — same truncate+reload-via-drop/recreate pattern as the raw loader."""
    with engine.connect() as conn:
        conn.execute(text(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{MARTS_SCHEMA}') "
            f"EXEC('CREATE SCHEMA {MARTS_SCHEMA}')"
        ))
        conn.commit()

        for table_name, ddl in TABLE_DDL.items():
            conn.execute(text(
                f"IF OBJECT_ID('{MARTS_SCHEMA}.{table_name}', 'U') IS NOT NULL "
                f"DROP TABLE {MARTS_SCHEMA}.{table_name}"
            ))
            conn.execute(text(ddl))
            print(f"  Created table: {MARTS_SCHEMA}.{table_name}")
        conn.commit()


def export_to_parquet_and_upload(duck_con, minio_client, table_name, run_date):
    """Exports one mart table to a local Parquet file, then uploads it to
    MinIO staging-zone as a point-in-time audit artifact (plan.md Section 8)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / f"{table_name}.parquet"
        duck_con.sql(f"COPY {table_name} TO '{local_path.as_posix()}' (FORMAT PARQUET)")

        key = f"{table_name}/{run_date.isoformat()}/{table_name}.parquet"
        minio_client.upload_file(str(local_path), BUCKET_STAGING, key)
        print(f"  Exported -> s3://{BUCKET_STAGING}/{key}")


def load_table_to_mssql(duck_con, mssql_engine, table_name):
    """Reads one mart table from DuckDB and bulk-loads it into MSSQL.
    fast_executemany=True is set on the engine; method='multi' is deliberately
    NOT used (CLAUDE.md gotcha #2 — SQL Server's ODBC driver caps out around
    ~2,100 parameters per statement)."""
    df = duck_con.sql(f"select * from {table_name}").df()
    df.to_sql(
        table_name,
        con=mssql_engine,
        schema=MARTS_SCHEMA,
        if_exists="append",  # table already created explicitly above with correct types
        index=False,
        chunksize=1000,
    )
    print(f"  Loaded {len(df):>6} rows into {MARTS_SCHEMA}.{table_name}")


def main():
    print(f"DuckDB source: {DUCKDB_FILE}")
    print(f"MSSQL target: {MSSQL_SERVER} / database '{MSSQL_CURATED_DATABASE}'\n")

    print("Step 1: Ensuring CuratedTakafulPOC database exists...")
    ensure_curated_database_exists()

    mssql_engine = sa.create_engine(CURATED_CONN_STR, fast_executemany=True)

    print("\nStep 2: Creating marts schema and tables...")
    ensure_schema_and_tables(mssql_engine)

    # read_only=True per CLAUDE.md gotcha #6 — this script never writes to the
    # dbt-duckdb working file, only reads the already-built, already-tested marts.
    duck_con = duckdb.connect(str(DUCKDB_FILE), read_only=True)

    minio_client = get_minio_client()
    ensure_bucket(minio_client, BUCKET_STAGING)

    run_date = date.today()

    print("\nStep 3: Exporting marts to Parquet (MinIO staging-zone audit copy)...")
    for table_name in MART_TABLES:
        export_to_parquet_and_upload(duck_con, minio_client, table_name, run_date)

    print("\nStep 4: Loading marts into MSSQL...")
    start = time.time()
    for table_name in MART_TABLES:
        load_table_to_mssql(duck_con, mssql_engine, table_name)
    elapsed = time.time() - start

    duck_con.close()
    mssql_engine.dispose()

    print(f"\nDone in {elapsed:.1f}s. Curated marts are in MSSQL under "
          f"'{MSSQL_CURATED_DATABASE}.{MARTS_SCHEMA}'.")
    print("Next step: Power BI connects to these tables directly.")


if __name__ == "__main__":
    main()
