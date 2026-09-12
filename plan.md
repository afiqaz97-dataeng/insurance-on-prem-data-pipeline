# Data Pipeline Plan
### Takaful Malaysia POC — MinIO + Python + DuckDB + dbt + MSSQL + Airflow

---

## 1. Objective

Build an end-to-end batch data pipeline that:
1. **Extracts** raw data via a Python script into **MinIO** (object storage, raw landing zone)
2. **Transforms** the data using **DuckDB** as the local analytical/transformation engine
3. **Tests/validates** the transformed data using **dbt** (data quality gates, lineage documentation)
4. **Loads** the validated, transformed output back into **MSSQL** (the serving/analyst-facing warehouse)
5. **Orchestrates** all of the above using **Airflow**

This supersedes the earlier "load CSVs directly into MSSQL" approach used for initial local testing — that was a useful shortcut to validate MSSQL connectivity early, but the confirmed target architecture routes raw data through MinIO first, consistent with the original production design.

---

## 2. Architecture

### Deployment target assumption

This architecture assumes an **on-prem deployment** (consistent with the project scope). Accordingly, the local POC mirrors that split deliberately:

- **MSSQL runs natively** on Windows — not containerized. This matches how on-prem SQL Server deployments are typically run in practice (native install, OS-level DBA control, native backup/HA tooling), including in regulated on-prem insurance/Takaful environments.
- **MinIO and Airflow run in Docker** — newer OSS tooling commonly gets containerized even in on-prem shops, while the legacy/core database stays native. Containers reach the native MSSQL instance via Docker Desktop's `host.docker.internal` DNS name rather than `localhost`.

If the target environment shifts to cloud or a greenfield Linux deployment, MSSQL (via its Linux container support) could be containerized too — that's an explicit fork point to revisit if the client's direction changes, not a default assumption now.

### Is this a "lakehouse"? — be precise about the term

This architecture is best described as a **data lake feeding a serving warehouse**, not a formal "lakehouse." True lakehouse architecture (as the term is generally used) requires an **open table format** — Apache Iceberg, Delta Lake, or Hudi — sitting on top of object storage, providing ACID transactions, time travel, and schema evolution directly at the storage layer. MinIO's date-partitioned folders here give a manual approximation of versioning, but not those transactional guarantees.

This is the right amount of architecture for current scale — adding a table format layer now would be premature complexity, same reasoning that ruled out Spark/HDFS. If data volume or concurrency needs grow, Iceberg on MinIO (DuckDB has a native Iceberg extension) is a clean upgrade path, not a rebuild. **Recommendation: avoid calling this a "lakehouse" in client-facing material unless a table format is actually added** — a technically sharp stakeholder may reasonably push back on the term otherwise.

```
Source data (mock CSVs for POC; real source systems in production)
        │
        ▼
Python script (EXTRACTION)
   → writes raw files to MinIO, partitioned by date
        │
        ▼
MinIO — raw-zone bucket
   raw-zone/{table}/{yyyy-mm-dd}/{table}.csv
        │
        ▼
DuckDB (TRANSFORMATION)
   reads from MinIO via httpfs extension
   staging models → mart models (fund segregation: PRF / PIF / Wakalah fee)
        │
        ▼
dbt (TESTING / VALIDATION)
   dbt run   → builds models
   dbt test  → validates data quality — MUST PASS before proceeding
   dbt docs  → auto-generated lineage graph (audit trail evidence)
        │
        ▼
Python script (LOAD)
   → exports validated marts to Parquet (audit copy → MinIO staging-zone)
   → bulk-loads into MSSQL
        │
        ▼
MSSQL — TakafulPOC database
   Schema: takaful   (raw landing — already populated for initial testing)
   Schema: marts      (transformed, tested output — analyst/Power BI facing)
        │
        ▼
Analysts query via SSMS / Power BI connects natively
```

**Orchestration:** Apache Airflow sequences extract → transform → test → load, with the `test` step acting as a hard gate — a failed dbt test blocks the load into MSSQL.

---

## 3. Component Roles

| Layer | Tool | Role |
|---|---|---|
| Raw storage | **MinIO** | S3-compatible object storage; raw-zone (extracted data) + staging-zone (pre-load audit copies) |
| Extraction | **Python script** (`extract.py`) | Reads source data (mock CSVs for POC), writes partitioned files into MinIO |
| Transformation engine | **DuckDB** | Fast, single-node, embedded analytical engine; reads directly from MinIO via `httpfs` |
| Transformation framework | **dbt (dbt-duckdb adapter)** | Version-controlled SQL models, tests, auto-generated lineage documentation |
| Serving warehouse | **MSSQL** (`TakafulPOC` database) | Analyst/Power BI-facing; hosts both raw (`takaful` schema) and transformed (`marts` schema) data |
| Load | **Python script** (`load.py`) | Exports dbt mart tables → Parquet (MinIO audit copy) → bulk-load into MSSQL `marts` schema |
| Orchestration | **Apache Airflow** | Sequences extract → transform → test → load; retries, alerting (email), run history |
| Reporting | **Power BI** | Connects natively to MSSQL `marts.*` tables |

---

## 4. Data Model (confirmed from POC)

Five core tables, already validated in MSSQL under `takaful.*`:

| Table | Key fields | Notes |
|---|---|---|
| `participants` | participant_id, full_name, ic_number, date_of_birth, gender, state, join_date | Policyholders |
| `policies` | policy_id, participant_id, product_name, product_category, has_pif, status, agent_id | `has_pif` distinguishes Family (PIF-bearing) vs General/Medical Takaful |
| `contributions` | contribution_id, policy_id, gross_amount, wakalah_fee_shareholders_fund, prf_amount, pif_amount | Fund segregation logic: `pif_amount = 0` where `has_pif = false` |
| `claims` | claim_id, policy_id, claim_type, claim_amount, fund_type, status | `fund_type` almost always `PRF` |
| `agency_transactions` | transaction_id, agent_id, policy_id, commission_amount_shareholders_fund | Commission paid from Shareholders' Fund |

**Fund segregation invariant to preserve through the pipeline:**
`gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund` (validated in SSMS during POC; must become a dbt test).

---

## 5. Star Schema Design — Slowly Changing Dimensions (SCD)

The mart layer uses a star schema. Since policy status, agent assignment, and participant state can change over time, dimensions need explicit SCD handling — otherwise fact tables silently show only "current state" instead of "state as it was when the transaction happened." This matters for Takaful specifically: a claim filed while a policy was `Active` should still show `Active` in historical reporting, even if the policy later lapses.

### SCD type per dimension

| Dimension | SCD Type | Why |
|---|---|---|
| `dim_policies` | **Type 2** | Status, agent reassignment, contribution amount changes — need full history for audit-correct reporting |
| `dim_participants` | **Type 2** on `state`; **Type 1** on `full_name`/`ic_number` | State changes are meaningful history; name/IC changes are corrections, not real history |
| `dim_agents` | **Type 2** | Branch reassignment affects historical commission attribution |
| `dim_product` | **Type 1** | Product catalog is close to static — simple overwrite |
| `dim_date` | N/A | Standard static date dimension |

### Implementation — dbt snapshots (native SCD2 support)

```sql
-- snapshots/policies_snapshot.sql
{% snapshot policies_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='policy_id',
      strategy='check',
      check_cols=['status', 'agent_id', 'contribution_amount'],
    )
}}
select * from {{ source('raw', 'policies') }}
{% endsnapshot %}
```

```sql
-- snapshots/participants_snapshot.sql
{% snapshot participants_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='participant_id',
      strategy='check',
      check_cols=['state'],   -- only track state as history; name/IC stay Type 1
    )
}}
select * from {{ source('raw', 'participants') }}
{% endsnapshot %}
```

Running `dbt snapshot` on each scheduled run adds `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id` automatically — a new row is only created when a `check_cols` field actually changes.

### Building the dimension table on top of the snapshot

```sql
-- models/marts/dim_policies.sql
select
    {{ dbt_utils.generate_surrogate_key(['policy_id', 'dbt_valid_from']) }} as policy_sk,
    policy_id,
    product_name,
    product_category,
    has_pif,
    status,
    agent_id,
    contribution_amount,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, '9999-12-31') as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('policies_snapshot') }}
```

### Critical: fact tables must join on the effective dimension version, not the current one

The most common SCD2 mistake is building history correctly in the dimension, then joining facts to it on the natural key alone — which silently collapses everything back to "current state." Facts must join on an effective date range instead:

```sql
-- models/marts/fct_contributions.sql
select
    c.contribution_id,
    c.policy_id,
    dp.policy_sk,               -- surrogate key of the policy AS IT WAS on this date
    c.contribution_date,
    c.gross_amount,
    c.prf_amount,
    c.pif_amount,
    c.wakalah_fee_shareholders_fund
from {{ ref('stg_contributions') }} c
left join {{ ref('dim_policies') }} dp
    on c.policy_id = dp.policy_id
    and c.contribution_date >= dp.valid_from
    and c.contribution_date <  dp.valid_to
```

### SCD-specific dbt tests

```yaml
models:
  - name: dim_policies
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [policy_id, valid_from]
    columns:
      - name: policy_sk
        tests: [unique, not_null]
```

Also add a custom test confirming no gaps or overlaps exist in `valid_from`/`valid_to` ranges per natural key — a common SCD2 bug that silently corrupts historical fact joins without raising an obvious error.

---

## 6. Pipeline Steps in Detail

### Step 1 — Extraction (`extract.py`)
- Reads mock CSVs (or, in production, connects to actual source systems)
- Writes to MinIO `raw-zone` bucket, partitioned by date: `raw-zone/{table}/{yyyy-mm-dd}/{table}.csv`
- Uses `boto3` (S3-compatible client) pointed at MinIO's local endpoint

### Step 2 — Transformation (DuckDB + dbt)
- DuckDB's `httpfs` extension reads directly from MinIO:
  ```sql
  INSTALL httpfs; LOAD httpfs;
  SET s3_endpoint='localhost:9000';
  SET s3_access_key_id='<minio access key>';
  SET s3_secret_access_key='<minio secret key>';
  SET s3_url_style='path';
  SET s3_use_ssl=false;
  ```
- **Staging models** (1:1 with raw tables, light cleanup): `stg_participants`, `stg_policies`, `stg_contributions`, `stg_claims`, `stg_agency_transactions`
- **Mart models**: `fct_contributions`, `fct_claims`, `fct_agency_commissions`, `dim_participants`, `dim_policies` (see Section 5 for SCD Type 2 design on dimensions — facts join to dimensions on effective date range, not natural key alone)

### Step 3 — Testing (dbt)
- `dbt test` runs before any data reaches MSSQL. Required tests:
  - `not_null` on all primary keys
  - `accepted_values` on `fund_type` (`PRF`, `PIF`)
  - Custom test: `pif_amount = 0` where `has_pif = false`
  - Custom test: `gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund` (within rounding tolerance)
  - Relationship tests: `policy_id` in `claims`/`contributions` must exist in `policies`
- `dbt docs generate` — lineage graph doubles as audit-trail documentation

### Step 4 — Load (`load.py`)
- Exports each dbt mart table to Parquet
- Writes a copy to MinIO `staging-zone` (point-in-time audit artifact)
- Bulk-loads into MSSQL `marts` schema (truncate + reload for POC scale)
- **Important (lesson learned during setup):** use `fast_executemany=True` on the SQLAlchemy engine and avoid `method="multi"` in `pandas.to_sql()` — SQL Server's ODBC driver caps out around ~2,100 parameters per statement, and `method="multi"` easily exceeds this on wide tables with normal chunk sizes.
- **Important (lesson learned during setup):** URL-encode username/password with `urllib.parse.quote_plus()` when building the SQLAlchemy connection string — passwords containing `@`, `:`, or `/` will otherwise be misparsed.

### Step 5 — Orchestration (Airflow)
```python
from airflow.decorators import dag, task

@dag(schedule="0 2 * * *", catchup=False, tags=["takaful", "batch"])
def takaful_batch_pipeline():

    @task
    def extract():
        from scripts.extract import run_extraction
        run_extraction()

    @task
    def transform():
        import subprocess
        subprocess.run(["dbt", "run", "--project-dir", "dbt_takaful"], check=True)

    @task
    def test():
        import subprocess
        subprocess.run(["dbt", "test", "--project-dir", "dbt_takaful"], check=True)

    @task
    def load():
        from scripts.load import export_and_load_mssql
        export_and_load_mssql()

    extract() >> transform() >> test() >> load()
```

### Step 6 — Alerting
- Airflow's built-in `email_on_failure` in `default_args` — no webhook needed
- Include the failed task's error/log link in the email body
- Set sensible `retries`/`retry_delay` to avoid alert fatigue on transient issues

---

## 7. MSSQL Setup Notes (from POC — apply to any fresh environment)

These were real blockers during local setup and are worth documenting so they aren't rediscovered each time:

- [ ] **TCP/IP protocol** must be explicitly enabled (SQL Server Configuration Manager, or registry if Config Manager isn't installed: `HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQLxx.MSSQLSERVER\MSSQLServer\SuperSocketNetLib\Tcp`) — this is not on by default even when SQL Server itself is running
- [ ] **SQL Server service must be restarted** after enabling TCP/IP or changing auth mode — the setting doesn't take effect until restart
- [ ] **Mixed Mode Authentication** must be enabled (Server Properties → Security) if using SQL logins (`sa`) rather than Windows Authentication only
- [ ] **`sa` login must be explicitly enabled** with a password set (Security → Logins → sa → Status: Enabled)
- [ ] **Encryption settings** — if the server enforces `Encryption: Mandatory`, connection strings need `Encrypt=yes&TrustServerCertificate=yes` or `pyodbc` will fail with an SSL error
- [ ] **Verify connectivity with `Test-NetConnection -ComputerName localhost -Port 1433`** before touching Python — isolates network/protocol issues from application-level issues
- [ ] **PowerShell vs cmd.exe** — `$env:VAR="value"` only works in PowerShell; use `set VAR=value` in cmd.exe

---

## 8. Storage Layout (MinIO)

```
raw-zone/
  participants/{yyyy-mm-dd}/participants.csv
  policies/{yyyy-mm-dd}/policies.csv
  contributions/{yyyy-mm-dd}/contributions.csv
  claims/{yyyy-mm-dd}/claims.csv
  agency_transactions/{yyyy-mm-dd}/agency_transactions.csv

staging-zone/
  fct_contributions/{yyyy-mm-dd}/fct_contributions.parquet
  fct_claims/{yyyy-mm-dd}/fct_claims.parquet
  fct_agency_commissions/{yyyy-mm-dd}/fct_agency_commissions.parquet
```

Date-partitioned paths (not overwritten in place) give a natural version history — useful both for debugging and for the Shariah/regulatory audit trail story.

---

## 9. Troubleshooting Workflow — Read-Only Investigate, Fix via Git/CI

To avoid ad hoc edits ever silently changing production data (a real audit-trail risk for Takaful fund calculations), troubleshooting and fixing are kept as two clearly separate activities.

### Step 1 — Investigate (read-only, no risk)

Anyone can freely inspect data without any risk of locking conflicts or accidental changes:

- Query the DuckDB working file in **explicit read-only mode**:
  ```python
  import duckdb
  con = duckdb.connect('duckdb_data/takaful_transform.duckdb', read_only=True)
  con.sql("SELECT * FROM fct_contributions WHERE contribution_id = 'CTB00001234'")
  ```
- Note: this file is only briefly locked while Airflow's `dbt run`/`dbt test` executes (typically seconds to a couple of minutes). Read-only connections avoid most conflicts; if one does occur, just retry after the scheduled run completes.
- Also inspect the raw MinIO files directly if the issue might originate upstream of the transform layer (bad source data vs. bad transformation logic).
- For routine/day-to-day analysis (not debugging), query **MSSQL directly** instead of DuckDB — that's the intended multi-reader destination, built for exactly this.

### Step 2 — Fix (always through code, never a direct patch)

Once a root cause is identified, the fix goes through the normal engineering workflow — never a manual edit to the DuckDB file or a direct `UPDATE` on MSSQL:

1. Edit the actual dbt model (`.sql` file) or extraction/load script
2. Add a dbt test that would have caught this specific bug, so it can't silently regress
3. Commit, push, open a PR
4. CI runs `dbt test` against the change
5. Merge → next scheduled Airflow run rebuilds the data correctly using the fixed model

### Why this separation matters for this client

This is a meaningful governance story to tell a Shariah committee or auditor: **no one can quietly hand-edit a PRF/PIF/Wakalah fee number.** Every correction is a traceable, reviewed, tested code change — not a one-off manual patch with no audit trail.

### Starter debug queries (suggested: `notebooks/debug_queries.py`)

Keep a small set of pre-written, read-only diagnostic queries so investigation is repeatable rather than reinvented each time:

```python
import duckdb

con = duckdb.connect('duckdb_data/takaful_transform.duckdb', read_only=True)

# Trace a single contribution through the fund-split invariant
con.sql("""
    SELECT contribution_id, policy_id, gross_amount, prf_amount, pif_amount,
           wakalah_fee_shareholders_fund,
           gross_amount - (prf_amount + pif_amount + wakalah_fee_shareholders_fund) AS diff
    FROM fct_contributions
    WHERE contribution_id = ?
""", params=['CTB00001234'])

# Find all contributions where the fund-split invariant doesn't hold
con.sql("""
    SELECT *
    FROM fct_contributions
    WHERE ABS(gross_amount - (prf_amount + pif_amount + wakalah_fee_shareholders_fund)) > 0.01
""")

# Find PIF leakage into non-Family products (should never happen)
con.sql("""
    SELECT c.*
    FROM fct_contributions c
    JOIN dim_policies p ON c.policy_sk = p.policy_sk
    WHERE p.has_pif = false AND c.pif_amount != 0
""")
```

---

## 10. Open Items / Next Steps

- [ ] Stand up MinIO locally (Docker container or native binary) — buckets: `raw-zone`, `staging-zone`
- [ ] Write `extract.py` — mock CSVs → MinIO raw-zone
- [ ] Set up `dbt_takaful` project with `dbt-duckdb` adapter, configure `httpfs` to read from MinIO
- [ ] Build dbt snapshots for `dim_policies`, `dim_participants`, `dim_agents` (SCD Type 2 — see Section 5)
- [ ] Build staging + mart models, write dbt tests (including the fund-segregation invariant and SCD gap/overlap tests)
- [ ] Write `load.py` — dbt marts → Parquet (MinIO staging-zone) → MSSQL `marts` schema
- [ ] Build the Airflow DAG, test the full extract → transform → test → load sequence
- [ ] Deliberately break a test (bad `fund_type`) to confirm the pipeline correctly blocks the load
- [ ] Connect Power BI to `marts.*` tables, build a validation dashboard
- [ ] Set up email alerting on task failure

---

## 11. Why This Architecture

- **Audit-ready:** dbt tests gate every load into MSSQL; dbt's lineage docs + MinIO's date-partitioned raw files together provide a defensible trail for Shariah committee and regulatory review
- **Cost-efficient:** MinIO, DuckDB, dbt-core, and Airflow are all open source; only MSSQL/Power BI carry licensing cost
- **No distributed compute needed:** DuckDB handles this data volume comfortably on a single node — no Spark, no HDFS
- **Reproducible:** raw data preserved in MinIO independent of the transformation logic, so any mart can be rebuilt from scratch if dbt models change
