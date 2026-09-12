"""
Takaful batch pipeline DAG — orchestrates extract -> transform -> test -> load.

Runs inside the custom Airflow image (docker/airflow.Dockerfile), which has our
Python stack (dbt-core, dbt-duckdb, pyodbc, boto3, etc.) installed. The whole
repo is volume-mounted at /opt/airflow/project (see docker-compose.yml) so this
DAG operates on the exact same DuckDB file and dbt project as manual host-side
runs (Phases 1-7) — not a separate copy.

`test` failing blocks `load`: Airflow does not run downstream tasks after an
upstream failure, so the `extract >> transform >> test >> load` dependency
chain IS the hard gate plan.md Section 6 calls for — no extra logic needed.
"""

import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_takaful"
SCRIPTS_DIR = f"{PROJECT_DIR}/scripts"
# dbt lives in its own virtualenv, isolated from Airflow's own dependencies —
# see docker/airflow.Dockerfile for why (a real, hit-in-practice pip conflict
# between dbt-core's deps and Airflow's constraints file).
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL_TO", "")],
}


def _run_dbt(subcommand: str) -> None:
    # profiles.yml's `path` (../duckdb_data/...) resolves relative to the
    # process's CWD, not --project-dir — cwd must be dbt_takaful/ or dbt looks
    # for the DuckDB file one level too high (found by running this for real).
    result = subprocess.run(
        [DBT_BIN, subcommand, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {subcommand} failed (exit code {result.returncode})")


@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # DuckDB is single-writer (CLAUDE.md gotcha #6) — two
    # concurrent runs both calling `dbt run` against the same file will fight
    # over its lock. Hit this for real: an auto-created catchup run and a
    # manual trigger raced and one got "Permission denied" from DuckDB.
    tags=["takaful", "batch"],
    default_args=default_args,
)
def takaful_batch_pipeline():

    @task
    def extract():
        sys.path.insert(0, SCRIPTS_DIR)
        from extract import run_extraction

        run_extraction()

    @task
    def transform():
        _run_dbt("run")

    @task
    def test():
        _run_dbt("test")

    @task
    def load():
        sys.path.insert(0, SCRIPTS_DIR)
        from load import main as load_main

        load_main()

    extract() >> transform() >> test() >> load()


takaful_batch_pipeline()
