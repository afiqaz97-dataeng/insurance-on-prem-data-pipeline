# Custom Airflow image: base image + MS ODBC Driver 17 (for load.py's pyodbc/
# MSSQL access) + our Python stack (dbt-core, dbt-duckdb, duckdb, boto3, etc.).
#
# Pinned to an explicit python-tagged base (not just apache/airflow:3.3.1) so the
# constraints file URL below is unambiguous — see plan.md/action.md Phase 8.
FROM apache/airflow:3.3.1-python3.12

USER root

# MS ODBC Driver 17 for SQL Server — required for pyodbc (scripts/load.py,
# Migration_Script/load_raw_to_mssql.py) to reach the native MSSQL instance via
# host.docker.internal. Standard Microsoft apt-repo install pattern.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 unixodbc-dev g++ \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install pandas/sqlalchemy/pyodbc/boto3/duckdb/python-dotenv into Airflow's own
# environment against its constraints file (Airflow's documented pattern for
# extending the image, not `_PIP_ADDITIONAL_REQUIREMENTS`, which is dev/test-only).
# dbt-core is DELIBERATELY excluded here — see below.
COPY requirements.txt /requirements.txt
RUN grep -v -E '^dbt-' /requirements.txt > /tmp/requirements-airflow-env.txt \
    && pip install --no-cache-dir \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
       -r /tmp/requirements-airflow-env.txt

# dbt-core's dependency tree (jinja2, protobuf, sqlparse, ...) conflicts with
# Airflow's pinned constraints when installed into the same environment — a
# well-known Airflow+dbt integration issue. Fix: isolate dbt in its own
# virtualenv, resolved independently with no Airflow constraints, and invoke it
# by full path from the DAG (dags/takaful_batch_pipeline.py) instead of relying
# on a bare `dbt` on PATH.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-duckdb
