# Takaful On-Prem Data Pipeline

A batch data pipeline proof-of-concept for a **Takaful (Islamic insurance) operator in Malaysia** — built to mirror real Shariah/regulatory requirements (fund segregation, SCD history, an audit trail enforced by automated tests) on synthetic data, as a template for a real on-prem deployment.

[![CI](https://github.com/afiqaz97-dataeng/insurance-on-prem-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/afiqaz97-dataeng/insurance-on-prem-data-pipeline/actions/workflows/ci.yml)

## Architecture

```
Mock CSVs → Python (extract.py) → MinIO (raw-zone) → DuckDB + dbt (transform + test)
    → Python (load.py) → MSSQL (CuratedTakafulPOC.marts) → Power BI
```

Orchestrated end-to-end by **Apache Airflow**, with `dbt test` acting as a hard gate — a failed test blocks the load into the serving warehouse.

**Deployment model:** MSSQL runs **natively** on Windows (not containerized — a deliberate on-prem design choice); MinIO and Airflow run in **Docker**, reaching native MSSQL via `host.docker.internal`. This is a data lake feeding a serving warehouse, not a lakehouse — no Iceberg/Delta/Hudi table format is in scope.

## Why this design

- **Fund segregation is enforced everywhere the money moves.** `contributions.gross_amount` splits into `prf_amount + pif_amount + wakalah_fee_shareholders_fund`, and `pif_amount` must be zero for policies without a PIF component. This invariant is checked by dbt tests, re-verified directly against MSSQL, and made visible on the Power BI dashboard — not just asserted once.
- **Slowly Changing Dimensions (Type 2)** on `dim_policies` and `dim_participants`, via dbt snapshots. Fact tables join dimensions on an effective date range (`valid_from`/`valid_to`), never the natural key alone — the classic SCD2 bug that silently collapses history to "current state."
- **Raw and curated data live in separate MSSQL databases** (`TakafulPOC` for raw landing, `CuratedTakafulPOC` for tested output) so they can carry different access controls.
- **Audit trail, not just a pipeline.** MinIO's date-partitioned raw files, dbt's lineage docs, and `dbt_run_started_at`/`etl_loaded_at` timestamps on every mart row together answer "what did we receive, what did we do to it, and when" — the actual audit story a Shariah committee or regulator would ask for.
- **The pipeline proves it blocks bad data, on demand.** A validation exercise deliberately breaks a dbt test through the real Airflow DAG and confirms `load` never runs and the serving warehouse stays untouched.

## Stack

| Layer | Tool |
|---|---|
| Raw storage | MinIO (S3-compatible), in Docker |
| Extraction | Python (`boto3`) |
| Transformation | DuckDB + dbt (`dbt-duckdb`) |
| Serving warehouse | MSSQL (native), two databases |
| Orchestration | Apache Airflow 3.3.1 (LocalExecutor), in Docker |
| CI | GitHub Actions |
| Reporting | Power BI |

## Repository layout

```
Data/                   mock source CSVs
Migration_Script/       initial raw-load script (superseded by extract.py, kept for reference)
scripts/                extract.py, load.py
dbt_takaful/             dbt project — staging models, SCD2 snapshots, mart models, tests
dags/                   Airflow DAG
docker/                 custom Airflow image (MS ODBC driver + isolated dbt venv)
docker-compose.yml      MinIO + Airflow (LocalExecutor) services
.github/workflows/      CI pipeline
notebooks/              read-only debug/validation queries
plan.md                 full architecture, data model, and design rationale
action.md               phased build log — what was built, verified, and how
```

## Getting started

Prerequisites: Docker Desktop, a native MSSQL instance (TCP/IP enabled, mixed-mode auth), Python 3.12+.

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in your MSSQL credentials
docker compose up -d minio
python scripts/extract.py
cd dbt_takaful && dbt deps && dbt run --select staging.* && dbt snapshot && dbt run && dbt test
python ../scripts/load.py
```

For the full orchestrated pipeline (Airflow) and email alerting, see `action.md` Phase 8 and `plan.md` Section 6.

## Project status

All 11 build phases complete — see `action.md` for the full log, including three real bugs found and fixed during verification (not just "it ran"): a DuckDB CWD path issue, a missing `dbt snapshot` step in the orchestrated DAG, and a dead Docker Hub image dependency caught before it ever reached CI.

`plan.md` is the full architecture reference; `action.md` is the phase-by-phase build and verification log.
