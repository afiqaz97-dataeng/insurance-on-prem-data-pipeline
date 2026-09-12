"""
Load mock CSVs directly into local MSSQL — raw landing zone.
Run this AFTER generate_mock_data.py has produced the 5 CSVs in mock_data/output/.

This script:
  1. Creates a `raw` schema in MSSQL (if not exists)
  2. Creates 5 raw tables matching the CSV structure exactly (no transformation yet)
  3. Bulk-loads each CSV into its corresponding raw table

The ETL step (DuckDB + dbt transform, writing into a `marts` schema) happens LATER,
reading FROM these raw tables — not from CSV/MinIO directly.

Requirements:
    pip install pyodbc pandas sqlalchemy
    ODBC Driver 17 (or 18) for SQL Server must be installed on your machine.

Environment variables expected (set in .env or export before running):
    MSSQL_SERVER    e.g. "localhost,1433"
    MSSQL_DATABASE  e.g. "TakafulPOC"
    MSSQL_USER      e.g. "sa"
    MSSQL_PASSWORD  e.g. "YourStrong!Passw0rd"
"""

import os
import time
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MSSQL_SERVER = os.environ.get("MSSQL_SERVER", "localhost,1433")
MSSQL_DATABASE = os.environ.get("MSSQL_DATABASE", "TakafulPOC")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_PASSWORD", "YourStrong!Passw0rd")
ODBC_DRIVER = os.environ.get("MSSQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

DATA_DIR = Path(__file__).parent / "Data"
RAW_SCHEMA = "takaful"

# Connection string — connects to `master` first so we can create the target DB if missing
# Note: TrustServerCertificate=yes + Encrypt=yes matches a server configured for
# "Mandatory" encryption with a self-signed/trusted local certificate (typical for a
# local dev install). Without this, pyodbc will fail with an SSL/certificate error.
#
# IMPORTANT: the password is URL-encoded via quote_plus() because SQLAlchemy connection
# URLs use special characters (@, :, /, etc.) as delimiters. If your password contains
# any of these characters (e.g. "Suede@3245"), building the string with an f-string
# directly will silently corrupt the parsed username/password/host — always encode.
CONN_EXTRA = "&Encrypt=yes&TrustServerCertificate=yes"

_encoded_user = quote_plus(MSSQL_USER)
_encoded_password = quote_plus(MSSQL_PASSWORD)

MASTER_CONN_STR = (
    f"mssql+pyodbc://{_encoded_user}:{_encoded_password}@{MSSQL_SERVER}/master"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}{CONN_EXTRA}"
)
DB_CONN_STR = (
    f"mssql+pyodbc://{_encoded_user}:{_encoded_password}@{MSSQL_SERVER}/{MSSQL_DATABASE}"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}{CONN_EXTRA}"
)

# ---------------------------------------------------------------------------
# Table definitions — explicit DDL so types are sensible (not just pandas guesses)
# ---------------------------------------------------------------------------
TABLE_DDL = {
    "participants": """
        CREATE TABLE takaful.participants (
            participant_id   VARCHAR(20)  PRIMARY KEY,
            full_name        NVARCHAR(200),
            ic_number        VARCHAR(20),
            date_of_birth    DATE,
            gender           CHAR(1),
            state            NVARCHAR(50),
            join_date        DATE
        )
    """,
    "policies": """
        CREATE TABLE takaful.policies (
            policy_id            VARCHAR(20) PRIMARY KEY,
            participant_id       VARCHAR(20),
            product_name         NVARCHAR(100),
            product_category     NVARCHAR(20),
            has_pif              BIT,
            start_date           DATE,
            end_date             DATE,
            term_years           INT,
            payment_mode         NVARCHAR(20),
            contribution_amount  DECIMAL(12,2),
            status               NVARCHAR(20),
            agent_id             VARCHAR(20),
            branch               NVARCHAR(50)
        )
    """,
    "contributions": """
        CREATE TABLE takaful.contributions (
            contribution_id                 VARCHAR(20) PRIMARY KEY,
            policy_id                       VARCHAR(20),
            contribution_date               DATE,
            gross_amount                    DECIMAL(12,2),
            wakalah_fee_shareholders_fund   DECIMAL(12,2),
            prf_amount                      DECIMAL(12,2),
            pif_amount                      DECIMAL(12,2),
            payment_method                  NVARCHAR(30)
        )
    """,
    "claims": """
        CREATE TABLE takaful.claims (
            claim_id        VARCHAR(20) PRIMARY KEY,
            policy_id       VARCHAR(20),
            claim_type      NVARCHAR(50),
            claim_date      DATE,
            claim_amount    DECIMAL(12,2),
            fund_type       VARCHAR(10),
            status          NVARCHAR(20),
            approval_date   DATE NULL
        )
    """,
    "agency_transactions": """
        CREATE TABLE takaful.agency_transactions (
            transaction_id                          VARCHAR(20) PRIMARY KEY,
            agent_id                                VARCHAR(20),
            policy_id                               VARCHAR(20),
            transaction_type                        NVARCHAR(20),
            transaction_date                        DATE,
            commission_rate                         DECIMAL(5,2),
            commission_amount_shareholders_fund     DECIMAL(12,2)
        )
    """,
}

CSV_FILES = {
    "participants": "participants.csv",
    "policies": "policies.csv",
    "contributions": "contributions.csv",
    "claims": "claims.csv",
    "agency_transactions": "agency_transactions.csv",
}

DATE_COLUMNS = {
    "participants": ["date_of_birth", "join_date"],
    "policies": ["start_date", "end_date"],
    "contributions": ["contribution_date"],
    "claims": ["claim_date", "approval_date"],
    "agency_transactions": ["transaction_date"],
}


def ensure_database_exists():
    """Connects to master and creates the target database if it doesn't exist yet."""
    engine = sa.create_engine(MASTER_CONN_STR, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT database_id FROM sys.databases WHERE name = :name"),
            {"name": MSSQL_DATABASE},
        ).fetchone()
        if result is None:
            print(f"Database '{MSSQL_DATABASE}' not found — creating it...")
            conn.execute(text(f"CREATE DATABASE [{MSSQL_DATABASE}]"))
        else:
            print(f"Database '{MSSQL_DATABASE}' already exists.")
    engine.dispose()


def ensure_schema_and_tables(engine):
    """Creates the `raw` schema and drops/recreates each raw table for a clean reload."""
    with engine.connect() as conn:
        conn.execute(text(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{RAW_SCHEMA}') "
            f"EXEC('CREATE SCHEMA {RAW_SCHEMA}')"
        ))
        conn.commit()

        for table_name, ddl in TABLE_DDL.items():
            conn.execute(text(
                f"IF OBJECT_ID('{RAW_SCHEMA}.{table_name}', 'U') IS NOT NULL "
                f"DROP TABLE {RAW_SCHEMA}.{table_name}"
            ))
            conn.execute(text(ddl))
            print(f"  Created table: {RAW_SCHEMA}.{table_name}")
        conn.commit()


def load_csv_to_table(engine, table_name, csv_filename):
    """Reads a CSV and bulk-loads it into the corresponding raw table via pandas.to_sql."""
    csv_path = DATA_DIR / csv_filename
    df = pd.read_csv(csv_path)

    for col in DATE_COLUMNS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    start = time.time()
    df.to_sql(
        table_name,
        con=engine,
        schema=RAW_SCHEMA,
        if_exists="append",   # table already created explicitly above with correct types
        index=False,
        chunksize=1000,
        # NOTE: do NOT use method="multi" here. pandas builds one multi-row
        # INSERT ... VALUES (...), (...), ... statement per chunk, and SQL Server's
        # ODBC driver caps out at ~2,100 parameters per statement. A 1000-row chunk
        # on a 7-column table alone is 7,000 parameters -> "COUNT field incorrect".
        # fast_executemany=True (set on the engine) already batches inserts
        # efficiently without building one giant multi-row statement.
    )
    elapsed = time.time() - start
    print(f"  Loaded {len(df):>6} rows into {RAW_SCHEMA}.{table_name}  ({elapsed:.1f}s)")


def main():
    print(f"Target: {MSSQL_SERVER} / database '{MSSQL_DATABASE}'\n")

    print("Step 1: Ensuring database exists...")
    ensure_database_exists()

    engine = sa.create_engine(DB_CONN_STR, fast_executemany=True)

    print("\nStep 2: Creating raw schema and tables...")
    ensure_schema_and_tables(engine)

    print("\nStep 3: Loading CSVs into raw tables...")
    for table_name, csv_filename in CSV_FILES.items():
        load_csv_to_table(engine, table_name, csv_filename)

    print(f"\nDone. Data is now in MSSQL under the '{RAW_SCHEMA}' schema.")
    print("Next step: run the ETL (DuckDB/dbt) reading FROM these raw tables,")
    print("writing transformed marts back into a separate 'marts' schema.")

    engine.dispose()


if __name__ == "__main__":
    main()