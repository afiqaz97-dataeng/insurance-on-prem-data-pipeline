# Action Checklist
### Phased build plan for the Takaful ETL pipeline — see `plan.md` for full architecture/design detail

Status legend: ✅ done · 🚧 in progress · ⬜ not started

---

## Phase 1 — Repo scaffolding + extract.py + MinIO ✅
*plan.md §6 Step 1, §8, §10*

- [x] `git init`, `.gitignore`
- [x] `requirements.txt` (pandas, sqlalchemy, pyodbc, boto3, python-dotenv, duckdb, dbt-core, dbt-duckdb)
- [x] `.env.example` (MSSQL_* + MINIO_*)
- [x] `docker-compose.yml` — MinIO service (raw-zone / staging-zone buckets)
- [x] `scripts/extract.py` — mock CSVs → MinIO `raw-zone/{table}/{yyyy-mm-dd}/{table}.csv`
- [x] Verified end-to-end: `docker compose up -d minio` + `python scripts/extract.py` + confirmed 5 partitioned files in MinIO, row counts match source CSVs, re-run is idempotent

## Phase 2 — dbt project scaffolding ✅
*plan.md §2 (transform layer), §6 Step 2*

- [x] `dbt_takaful/` project, `dbt-duckdb` adapter (dbt-core 1.12.4, dbt-duckdb 1.11.0)
- [x] `profiles.yml` (project-local, not `~/.dbt/`) with separate `dev` and `ci` targets — own DuckDB file paths per CLAUDE.md gotcha #7, all values via `env_var()` with local-dev defaults so the file is safe to commit
- [x] `httpfs` extension configured to read MinIO raw-zone (`s3_endpoint`, path-style, no SSL for local)
- [x] Sources defined (`models/staging/_sources.yml`) against raw-zone CSVs via dbt-duckdb `external_location` + glob over date partitions
- [x] Verified: `dbt debug` passes (DuckDB + httpfs + MinIO connection OK); `dbt parse` succeeds; direct `read_csv_auto` over each source path returns row counts matching source CSVs (3000/5000/20000/1200/6000)

Note: `dbt` console script isn't on PATH by default on this machine (pip installed it to `%APPDATA%\Python\Python313\Scripts`, which isn't on PATH) — add that directory to PATH, or call the full path to `dbt.exe` there, if `dbt` isn't found.

## Phase 3 — Staging models ⬜
*plan.md §6 Step 2, CLAUDE.md coding conventions*

- [ ] `stg_participants`, `stg_policies`, `stg_contributions`, `stg_claims`, `stg_agency_transactions`
- [ ] 1:1 with raw tables, light cleanup only — no business logic

## Phase 4 — SCD snapshots ⬜
*plan.md §5*

- [ ] `policies_snapshot` (Type 2 on `status`, `agent_id`, `contribution_amount`)
- [ ] `participants_snapshot` (Type 2 on `state` only)
- [ ] `agents_snapshot` (Type 2 on branch/status fields)

## Phase 5 — Mart models ⬜
*plan.md §5, §6 Step 2*

- [ ] `dim_policies`, `dim_participants`, `dim_agents` built on snapshots (surrogate keys, `valid_from`/`valid_to`/`is_current`)
- [ ] `dim_product` (Type 1, simple overwrite), `dim_date`
- [ ] `fct_contributions`, `fct_claims`, `fct_agency_commissions` — **joined to SCD2 dims on effective date range, never natural key alone**

## Phase 6 — dbt tests ⬜
*plan.md §5 (SCD tests), §6 Step 3*

- [ ] `not_null` on all primary keys
- [ ] `accepted_values` on `fund_type`
- [ ] Custom test: `pif_amount = 0` where `has_pif = false`
- [ ] Custom test: `gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund` (rounding tolerance)
- [ ] Relationship tests (`policy_id` in claims/contributions → policies)
- [ ] SCD2 gap/overlap test on `valid_from`/`valid_to` per natural key
- [ ] `dbt docs generate`

## Phase 7 — load.py ⬜
*plan.md §6 Step 4*

- [ ] Export dbt marts → Parquet → MinIO `staging-zone` (audit copy)
- [ ] Bulk-load into MSSQL `marts` schema (truncate + reload)
- [ ] Reuse connection pattern from `Migration_Script/load_raw_to_mssql.py` (URL-encoded creds, `fast_executemany=True`, no `method="multi"`, encryption flags)

## Phase 8 — Airflow ⬜
*plan.md §6 Step 5–6*

- [ ] Airflow added to `docker-compose.yml` (containers reach native MSSQL via `host.docker.internal`)
- [ ] DAG: `extract >> transform >> test >> load` (dbt test is a hard gate before load)
- [ ] `email_on_failure`, sensible `retries`/`retry_delay`

## Phase 9 — Validation exercise ⬜
*plan.md §10*

- [ ] Deliberately break a test (bad `fund_type`) — confirm the DAG blocks the load into MSSQL

## Phase 10 — CI workflow ⬜
*CLAUDE.md gotchas #7, #8*

- [ ] `.github/workflows/ci.yml` — extract → transform → `dbt test` against a live MinIO service container
- [ ] Use `bitnami/minio` (not plain `minio/minio`) for the `services:` block
- [ ] CI dbt target uses its own DuckDB file + CI MinIO credentials, never touches local dev files

## Phase 11 — Power BI ⬜
*plan.md §3, §6*

- [ ] Connect Power BI to MSSQL `marts.*` tables
- [ ] Build validation dashboard

---

## Explicitly out of scope for now

- Iceberg/Delta/Hudi table format (this is a data lake feeding a warehouse, not a lakehouse — see plan.md §2)
- Terraform / IaC
- Teams/Slack alerting (email only, via Airflow's built-in `email_on_failure`)
- Containerizing MSSQL (stays native — deliberate on-prem deployment choice, see CLAUDE.md)
- Federated queries
