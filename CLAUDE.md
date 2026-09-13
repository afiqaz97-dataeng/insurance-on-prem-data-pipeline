# CLAUDE.md
### Project context for Claude Code / coding agents working on this repo

This file briefs an agent on what this project is, what's already decided, what's already built, and what conventions to follow. Read this before writing any code. Also read `plan.md` (full architecture) and `action.md` (phased build checklist) before starting any phase.

---

## What this project is

A batch data pipeline for a **Takaful (Islamic insurance) operator in Malaysia** — a local proof-of-concept meant to be later proposed to a real client. It is **not** a toy exercise; the design choices below (fund segregation, SCD history, audit trail via dbt tests) are meant to mirror real Shariah/regulatory requirements, even though the data is synthetic.

**Confirmed architecture:**
```
Mock CSVs → Python (extract.py) → MinIO (raw-zone) → DuckDB + dbt (transform + test) → Python (load.py) → MSSQL (CuratedTakafulPOC.marts) → Power BI
```
Orchestrated by **Airflow**. MinIO + Airflow run in **Docker**; **MSSQL runs natively** on Windows (already set up and working — do not containerize it, see "Deployment model" below).

Full detail lives in `plan.md`. Build sequence and checklists live in `action.md`. This file is a supplement to both, not a replacement — always check those two files for the authoritative current state before assuming anything.

---

## Deployment model — do not change without asking

- **MSSQL is native**, not Dockerized. This was a deliberate choice (mirrors realistic on-prem deployment) and involved real setup work (TCP/IP, auth mode, encryption) — do not suggest moving it into a container unless explicitly asked.
- **MinIO and Airflow run in Docker.** Containers reach the native MSSQL instance via `host.docker.internal`, never `localhost` or the machine's hostname.
- This is **not a lakehouse**. It's a data lake (MinIO, plain files) feeding a serving warehouse (MSSQL) — two physical copies of the data, kept in sync by `load.py`. Do not describe it as a lakehouse in generated docs/comments. No Iceberg/Delta/Hudi table format is in scope currently.

---

## Database schema — already built, do not rename

Raw and curated data live in **two separate MSSQL databases on the same server** — a deliberate split so raw and curated data can carry different access controls, not an oversight. Do not merge them back into one database without asking.

| Database | Schema | Purpose | Status |
|---|---|---|---|
| **`TakafulPOC`** | `takaful` | Raw landing zone — 5 tables, already loaded from mock CSVs | ✅ Done |
| **`CuratedTakafulPOC`** | `marts` | Transformed, tested output (dbt marts land here) | Not yet built |

Raw tables (`TakafulPOC.takaful`): `participants`, `policies`, `contributions`, `claims`, `agency_transactions`. Exact column definitions are in `Migration_Script/load_raw_to_mssql.py` (already written) — treat that file as the source of truth for raw schema, don't redefine it elsewhere.

`CuratedTakafulPOC` doesn't exist yet — `load.py` (Phase 7) must create it the same way `Migration_Script/load_raw_to_mssql.py` already creates `TakafulPOC` (`CREATE DATABASE` if not exists), then create the `marts` schema inside it.

---

## Fund-segregation logic — this is the core business rule, must be preserved everywhere

- `policies.has_pif` distinguishes **Family Takaful** (has a PIF/investment component) from **General/Medical Takaful** (PRF only).
- `contributions.gross_amount` splits into three parts that must always sum back to the gross amount:
  ```
  gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund
  ```
- **Invariant to test everywhere this data passes through:** `pif_amount = 0` wherever `has_pif = false`. Any dbt model or script touching contributions must preserve this — never silently collapse or drop these columns.
- `claims.fund_type` is almost always `PRF`.
- `agency_transactions.commission_amount_shareholders_fund` is paid from the Shareholders' Fund, not PRF/PIF.

Any transformation logic that touches money must keep these three funds distinguishable. This is the actual "audit trail" story for this project — don't take shortcuts here even in a POC.

---

## SCD (Slowly Changing Dimensions) — required on key dimensions

Full design is in `plan.md` Section 5. Summary:

| Dimension | SCD Type | Tracked via dbt snapshot on |
|---|---|---|
| `dim_policies` | Type 2 | `status`, `agent_id`, `contribution_amount` |
| `dim_participants` | Type 2 on `state` only; Type 1 (overwrite) on name/IC | `state` |
| `dim_product` | Type 1 | N/A — simple overwrite |

**No `dim_agents`.** Dropped in Phase 4 — there's no raw agents table and no agent-level attribute in the raw data at all (`agent_id`/`branch` only exist per-policy, and every agent spans ~16 different branches, so `branch` isn't a stable agent attribute). Fact tables reference `agent_id` directly with no dimension enrichment. See `plan.md` §5 for the full reasoning; don't reintroduce this without a real agent master source.

**Critical rule:** fact tables must join to SCD2 dimensions on an **effective date range** (`valid_from`/`valid_to`), never on the natural key alone. Joining on natural key alone silently collapses history back to "current state" — this is the most common SCD2 bug and must be caught in review, not just at test time.

---

## Known gotchas already discovered — don't rediscover these

These cost real debugging time during setup. Apply the fixes proactively rather than waiting to hit the same errors:

1. **SQLAlchemy connection strings + special characters in passwords** — always URL-encode username/password with `urllib.parse.quote_plus()`. A password containing `@` (e.g., `YourStr0ng!Pass@word`) will silently break connection parsing if not encoded.
2. **`pandas.to_sql()` with `method="multi"` on wide tables** — SQL Server's ODBC driver caps out around ~2,100 parameters per statement. `method="multi"` builds one large multi-row INSERT and will fail with `COUNT field incorrect or syntax error` on anything but narrow tables/small chunksizes. Use `fast_executemany=True` on the engine instead, and do **not** pass `method="multi"`.
3. **MSSQL connection needs `Encrypt=yes&TrustServerCertificate=yes`** in the connection string if the server enforces mandatory encryption (it does here).
4. **TCP/IP must be explicitly enabled** on the native SQL Server instance, and the service restarted afterward, or connections fail with `Named Pipes Provider: Could not open a connection` even though the engine is running.
5. **PowerShell vs cmd.exe** — `$env:VAR=value` is PowerShell-only; `set VAR=value` is cmd.exe. Scripts/docs should not assume one or the other without saying which shell.
6. **DuckDB is single-writer** — a `dbt run` holding a write lock on the `.duckdb` file will conflict with a concurrent read unless the reader explicitly opens `read_only=True`. Any ad hoc query script against the local DuckDB file must use `read_only=True`.
7. **CI's dbt target must be separate from local dev** — `profiles.yml` needs a distinct `ci` target (own DuckDB file path, CI MinIO credentials) so GitHub Actions runs never touch a developer's local working file.
8. **GitHub Actions `services:` containers can't override `command:`** — plain `minio/minio` requires an explicit `server /data` command that can't be injected via the `services:` block. `bitnami/minio` used to be the fix (auto-starts from env vars) but was deleted from Docker Hub in Bitnami's Aug 2025 free-image deprecation — use `bitnamilegacy/minio` instead (the frozen backup repo, still free, same auto-start behavior). Verified locally before adopting: pulls fine, starts with just `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` env vars, health endpoint responds.
9. **`dbt run` never builds snapshots** — separate resource type, separate command (`dbt snapshot`). A DAG/CI pipeline that only calls `dbt run` will never capture SCD2 history, and on a *fresh* database, any model that `ref()`s a snapshot will fail outright with a missing-schema error. Correct bootstrap order on a fresh database: `dbt run --select staging.*` (snapshots `ref()` staging) → `dbt snapshot` → `dbt run` (full, marts now resolve). Found this by simulating the CI workflow locally — it also revealed the real Airflow DAG's `transform` task had never called `dbt snapshot` at all; it only "worked" because snapshots already existed on the shared dev file from earlier manual runs.

---

## Workflow discipline — investigate read-only, fix via code

Never hand-patch data directly in DuckDB or MSSQL to "fix" a bad value. The required flow (see `plan.md` Section 9):
1. Investigate using **read-only** connections only
2. Root-cause the issue
3. Fix it by editing the actual dbt model / extraction / load script
4. Add a dbt test that would have caught this specific bug
5. Commit → PR → CI (`dbt test` must pass) → merge → next scheduled Airflow run applies the fix

If asked to "just fix this value in the database," push back and route the fix through this flow instead — this matters for the project's audit-trail credibility, not just style preference.

---

## Coding conventions

- Python: `pandas`, `sqlalchemy`, `pyodbc`, `boto3`, `duckdb` — already the established stack, don't introduce alternatives (e.g., don't swap in `psycopg2`-style patterns, this isn't Postgres) without a clear reason.
- dbt: `dbt-duckdb` adapter. Staging models are 1:1 with raw tables, light cleanup only — no business logic. Business logic (fund splits, joins, SCD-aware joins) belongs in mart models.
- Any new script that writes to MSSQL should reuse the connection-string pattern (URL-encoding, `fast_executemany`, encryption flags) already established in `scripts/load_raw_to_mssql.py` rather than reinventing it.
- Keep scripts idempotent where practical (e.g., raw table load does a clean drop/recreate; mart loads should support truncate + reload at minimum).

---

## Where to look for more detail

- **`plan.md`** — full architecture, data model, SCD design, MSSQL setup notes, storage layout, troubleshooting workflow, and the reasoning behind each major decision (including why this isn't a lakehouse, and why MSSQL stays native).
- **`action.md`** — the phased build checklist (Phase 1 through Phase 14), current status, and what's explicitly out of scope for now (Iceberg/Delta, Terraform, Teams/Slack alerting, containerizing MSSQL, federated queries).
- **`.github/workflows/ci.yml`** — the CI pipeline; read this before changing anything about the dbt project structure, since CI exercises the real extract → transform → test sequence against a live MinIO service container.

When in doubt about a decision that isn't covered above, check `plan.md` first — if it's genuinely not addressed there, ask rather than assuming, especially for anything touching the fund-segregation logic or the MSSQL/Docker deployment split.
