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

## Phase 3 — Staging models ✅
*plan.md §6 Step 2, CLAUDE.md coding conventions*

- [x] `stg_participants`, `stg_policies`, `stg_contributions`, `stg_claims`, `stg_agency_transactions`
- [x] 1:1 with raw tables, light cleanup only (explicit type casts to match MSSQL raw DDL precision; no business logic, no joins, fund-segregation columns passed through untouched)
- [x] **Fixed a latent bug before it could bite:** `_sources.yml` reads raw-zone across *all* date partitions ever written (for audit history), but the mock CSVs are static — so a second day's `extract.py` run would have silently duplicated every row in staging. Added `macros/latest_extract.sql` (`latest_partition_only()`), used via `QUALIFY` in every staging model to keep only the most recent partition. Verified by manually copying `participants.csv` into a second, earlier-dated partition and confirming `stg_participants` still returned exactly 3000 rows (no dupes) before cleaning the test partition back up.
- [x] Verified: `dbt run --select staging.*` builds all 5 views; row counts match source CSVs exactly (3000/5000/20000/1200/6000); `pif_amount = 0` where `has_pif = false` holds with 0 violations; column types (DATE, BOOLEAN, DECIMAL(12,2)/(5,2)) cast correctly

## Phase 4 — SCD snapshots ✅
*plan.md §5*

- [x] `policies_snapshot` (Type 2 on `status`, `agent_id`, `contribution_amount`) — sources from `ref('stg_policies')`, not raw, to avoid the multi-partition duplicate-key problem
- [x] `participants_snapshot` (Type 2 on `state` only) — sources from `ref('stg_participants')`
- [x] ~~`agents_snapshot`~~ — **dropped.** No raw agents table and no agent-level attribute exists in the raw data; empirically every agent spans ~16 different branches, so `policies.branch` isn't a stable per-agent attribute and can't be a working SCD2 unique key. See plan.md §5 / CLAUDE.md SCD section for full reasoning.
- [x] Verified real Type-2 behavior end-to-end: simulated a status change (`POL0000001` `Lapsed`→`Active`) via a new raw-zone partition, confirmed the snapshot closed the old row (`dbt_valid_to` set) and opened a new current row (5000→5001 rows, all other policies untouched), then reset local snapshot state back to clean (1 row per key) after the test

## Phase 5 — Mart models ✅
*plan.md §5, §6 Step 2*

- [x] `dim_policies`, `dim_participants` built on snapshots (surrogate keys via `dbt_utils.generate_surrogate_key`, `valid_from`/`valid_to`/`is_current`)
- [x] `dim_product` (Type 1, derived from `stg_policies` — verified `product_name`→`product_category` is a stable 1:1 mapping, 7 products, no raw catalog table exists), `dim_date` (via `dbt_utils.date_spine`, 2020-01-01 to 2048-01-01)
- [x] `fct_contributions`, `fct_claims`, `fct_agency_commissions` — joined to `dim_policies` on effective date range, never natural key alone; `fct_agency_commissions` references `agent_id` directly (no `dim_agents` — see Phase 4)
- [x] Added `dbt_utils` package (`packages.yml` + `dbt deps`)
- [x] **Fixed two real correctness bugs found while verifying, not just "it ran":**
  - Sentinel start date (`1900-01-01`) on each key's earliest snapshot version — without it, every fact dated before the pipeline's first snapshot run (i.e. basically everything, on a fresh POC) would fail to join to any dimension version at all (0 rows matched initially, traced to `valid_from` = snapshot capture time, not real history start).
  - `dim_participants` joins back to `stg_participants` for `full_name`/`ic_number` instead of reading them off the snapshot — the `check` strategy freezes non-tracked columns on unchanged rows, so reading them straight from the snapshot would silently break the "Type 1, always current" requirement.
- [x] Verified real SCD2-correct fact behavior end-to-end (not just build success): simulated `POL0000001` `Lapsed`→`Active`, rebuilt everything, confirmed all 4 of its historical contributions (dated 2022–2026) still joined to the *old* `Lapsed` dimension version and showed `Lapsed`, not `Active`. Reset to clean single-version state after.
- [x] Row counts verified: dim_policies 5000, dim_participants 3000, dim_product 7, dim_date 10227, fct_contributions 20000, fct_claims 1200, fct_agency_commissions 6000; 0 unmatched `policy_sk` across all 3 fact tables

## Phase 6 — dbt tests ✅
*plan.md §5 (SCD tests), §6 Step 3*

- [x] `not_null`/`unique` on all primary keys (staging + marts) — `models/staging/_staging.yml`, `models/marts/_marts.yml`
- [x] `accepted_values` on `fund_type` (`PRF`, `PIF`), policy `status`, claim `status`
- [x] Custom test: `pif_amount = 0` where `has_pif = false` — `tests/assert_no_pif_leakage.sql`, tested at the mart layer (fct_contributions + dim_policies), not just staging
- [x] Custom test: `gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund` (rounding tolerance) — `tests/assert_fund_split_invariant.sql`
- [x] Relationship tests: `policy_id` in claims/contributions/agency_transactions → policies; `participant_id` in policies → participants
- [x] SCD2 gap/overlap test — custom generic tests `no_scd_overlap` + `exactly_one_current_version` (`macros/generic_tests.sql`), applied to `dim_policies`/`dim_participants`
- [x] `dbt docs generate` — clean run, catalog built
- [x] All 50 tests pass on clean data. **Proved 3 of them actually catch bad data** (not just "ran without error"), same discipline as Phases 3-5:
  - Corrupted one contribution's fund split via a new raw partition → `assert_fund_split_invariant` caught exactly 1 violation
  - Injected PIF leakage on a no-PIF policy's contribution → `assert_no_pif_leakage` caught it
  - Hand-inserted a manufactured overlapping SCD version into `dim_policies` → `no_scd_overlap` caught it (while `dbt_utils.unique_combination_of_columns` on `policy_id, valid_from` correctly did NOT, since it's checking a different thing — confirms the custom overlap test is doing real, non-redundant work)
  - All test data reverted / tables rebuilt clean afterward

## Phase 7 — load.py ✅
*plan.md §6 Step 4*

- [x] Export dbt marts → Parquet → MinIO `staging-zone` (audit copy), partitioned by date same as raw-zone
- [x] `CREATE DATABASE CuratedTakafulPOC` if not exists (same pattern as `Migration_Script/load_raw_to_mssql.py`'s `ensure_database_exists()` for `TakafulPOC`) — confirmed working, database didn't exist before first run
- [x] `marts` schema + explicit DDL for all 7 tables (drop/recreate each run — truncate + reload)
- [x] Bulk-load into `CuratedTakafulPOC.marts` — reused connection pattern from `Migration_Script/load_raw_to_mssql.py` (URL-encoded creds — real password contains `@`, confirming the gotcha is real not theoretical — `fast_executemany=True`, no `method="multi"`, encryption flags)
- [x] Verified end-to-end: all 7 tables load with row counts matching the local DuckDB marts exactly (5000/3000/7/10227/20000/1200/6000); fund-split invariant re-checked directly in MSSQL (0 violations, not just trusted from dbt); re-ran the whole script a second time to confirm idempotent truncate+reload (identical counts, no duplicates, no errors)
- [x] **Added audit/lineage columns (raised during Phase 8, worth doing regardless of orchestration):** there was no way to tell when a mart row was produced. `dbt_run_started_at` (every mart model, `macros/audit_columns.sql`) says which transform run produced the row; `etl_loaded_at` (stamped in `load.py` at write time) says when it actually landed in MSSQL — two different timestamps since transform and load are separate steps that can run apart. Verified both are single consistent values per run via `SELECT DISTINCT`, and confirmed `dbt test` (all 50) still passes after the schema change.

## Phase 8 — Airflow ✅
*plan.md §6 Step 5–6*

- [x] Airflow 3.3.1 added to `docker-compose.yml` — LocalExecutor (postgres, airflow-init, airflow-scheduler, airflow-dag-processor, airflow-apiserver; no redis/worker/flower, right-sized for this data volume)
- [x] Custom image (`docker/airflow.Dockerfile`): MS ODBC Driver 17 + our Python stack. dbt-core/dbt-duckdb isolated in their own venv (`/home/airflow/dbt_venv`) — installing them into Airflow's own environment conflicts with Airflow's pinned constraints (a real, hit-in-practice pip resolution failure, not theoretical)
- [x] Whole repo volume-mounted at `/opt/airflow/project` so Airflow's `dbt run` and host-side manual runs/`debug_queries.ipynb` share the exact same `dbt_takaful/` project and `duckdb_data/` file
- [x] Network addressing overrides on the Airflow services only (`MINIO_ENDPOINT=http://minio:9000`, `MINIO_ENDPOINT_HOST=minio:9000`, `MSSQL_SERVER=host.docker.internal,1433`) — host `.env` keeps its `localhost` defaults for manual runs, unchanged
- [x] DAG (`dags/takaful_batch_pipeline.py`): `extract >> transform >> test >> load`, `max_active_runs=1` (added after hitting real lock contention between an auto-created catchup run and a manual trigger — DuckDB is single-writer, CLAUDE.md gotcha #6)
- [x] `retries=2`, `retry_delay=5min`; `smtp_default` Airflow Connection (Airflow 3.2+ requires an actual Connection, not just `AIRFLOW__SMTP__*` config — found this the hard way, first attempt failed with "conn_id `smtp_default` isn't defined")
- [x] **Readable failure emails** — plain `email_on_failure=True` sends an unreadable dump of the raw `TaskInstance` object (confirmed via the first real test — user flagged it as not understandable). Replaced with `on_failure_callback=[send_smtp_notification(...)]` using a custom Jinja `subject`/`html_content` (task, DAG, run ID, attempt count, the actual error, a log link). Also fixed `_run_dbt()` to include the tail of dbt's own output in the raised exception — the old message was just `"dbt test failed (exit code 1)"` with no indication of *which* test failed, which the email would have inherited verbatim.
  - Hit `'try_number' is undefined` on the first attempt — bare `{{ try_number }}`/`{{ max_tries }}` aren't in Airflow 3.x's `SmtpNotifier` template context (despite docs search results suggesting otherwise); fixed to `{{ ti.try_number }}`/`{{ ti.max_tries }}` and reverified with a second real failure/email round-trip before applying it to the real pipeline DAG
- [x] **Found and fixed 3 real bugs while verifying, not just "it built":**
  - dbt couldn't find the DuckDB file (`profiles.yml`'s relative path resolves against the process's CWD, not `--project-dir`) — fixed with `cwd=DBT_PROJECT_DIR` on the subprocess call
  - Two DAG runs racing for the same DuckDB file lock, surfacing as `Permission denied` — root-caused to a stale host-side Jupyter kernel still holding a `read_only` connection open; killed it, and added `max_active_runs=1` so this can't happen from Airflow's own side either
  - Failure emails silently went to a blank recipient because `airflow-scheduler` was started before `.env` had real `SMTP_*`/`ALERT_EMAIL_TO` values — env vars are baked in at container creation, not live-reloaded; fixed by recreating the containers
- [x] Verified end-to-end via the real Airflow UI/CLI (not just dbt/scripts in isolation): triggered the DAG, all 4 tasks succeeded in ~47s, `CuratedTakafulPOC.marts` row counts re-confirmed identical to the manual Phase 7 run
- [x] **Real failure/email test**: throwaway test DAG (`_test_email_alert`, deleted after) that fails on purpose — confirmed the alert email actually arrived in the inbox with full failure context (task_id, dag_id, run_id, traceback context), not just "no error in the logs"

## Phase 9 — Validation exercise ✅
*plan.md §10*

- [x] Deliberately broke a test for real, via the real pipeline DAG (not a script in isolation): injected `fund_type='INVALID_FUND'` into `claims.csv`, uploaded it to a future-dated raw-zone partition (`2026-09-14`) so it survived past the DAG's own `extract` step and stayed "latest"
- [x] Triggered `takaful_batch_pipeline` — `extract` and `transform` succeeded (the bad row flows through, as it should — that's staging's job, not business-logic filtering), `test` failed on `accepted_values_stg_claims_fund_type__PRF__PIF` exactly as expected, retried twice (5min apart, deterministic failure didn't change), then Airflow marked it `failed` and `load` went `upstream_failed` — **never ran**
- [x] Confirmed `CuratedTakafulPOC.marts.fct_claims` was untouched: 1200 rows before and after, 0 rows with the bad `fund_type` — the hard gate held
- [x] Confirmed the failure email fired for this real data-quality failure (no SMTP errors logged, same silent-success signature as the two prior confirmed email deliveries)
- [x] Cleaned up: removed the injected partition, rebuilt `stg_claims`/`fct_claims`, reconfirmed all 50 dbt tests pass clean. Left the failed DAG run in Airflow's history on purpose — it's real audit evidence this exercise happened, fitting the project's own audit-trail story

## Phase 10 — CI workflow ⬜
*CLAUDE.md gotchas #7, #8*

- [ ] `.github/workflows/ci.yml` — extract → transform → `dbt test` against a live MinIO service container
- [ ] Use `bitnami/minio` (not plain `minio/minio`) for the `services:` block
- [ ] CI dbt target uses its own DuckDB file + CI MinIO credentials, never touches local dev files

## Phase 11 — Power BI ⬜
*plan.md §3, §6*

- [ ] Connect Power BI to `CuratedTakafulPOC.marts.*` tables
- [ ] Build validation dashboard

---

## Explicitly out of scope for now

- Iceberg/Delta/Hudi table format (this is a data lake feeding a warehouse, not a lakehouse — see plan.md §2)
- Terraform / IaC
- Teams/Slack alerting (email only, via Airflow's built-in `email_on_failure`)
- Containerizing MSSQL (stays native — deliberate on-prem deployment choice, see CLAUDE.md)
- Federated queries
