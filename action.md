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

## Phase 7 — load.py ⬜
*plan.md §6 Step 4*

- [ ] Export dbt marts → Parquet → MinIO `staging-zone` (audit copy)
- [ ] `CREATE DATABASE CuratedTakafulPOC` if not exists (same pattern as `Migration_Script/load_raw_to_mssql.py`'s `ensure_database_exists()` for `TakafulPOC`), then create `marts` schema inside it
- [ ] Bulk-load into `CuratedTakafulPOC.marts` (truncate + reload) — a separate database from `TakafulPOC` (raw landing), not a schema alongside `takaful`
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

- [ ] Connect Power BI to `CuratedTakafulPOC.marts.*` tables
- [ ] Build validation dashboard

---

## Explicitly out of scope for now

- Iceberg/Delta/Hudi table format (this is a data lake feeding a warehouse, not a lakehouse — see plan.md §2)
- Terraform / IaC
- Teams/Slack alerting (email only, via Airflow's built-in `email_on_failure`)
- Containerizing MSSQL (stays native — deliberate on-prem deployment choice, see CLAUDE.md)
- Federated queries
