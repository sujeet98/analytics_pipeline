# Repo Structure

ANALYTICS_PIPELINE/
├── ingestion/
│   └── thelook_ingest/
│       ├── __init__.py
│       ├── config.py
│       ├── ingest_bigquery_to_raw.py
│       └── raw_to_bronze_autoloader.py
├── look_dbt/
│   ├── analyses/
│   ├── dbt_packages/
│   ├── logs/
│   ├── macros/
│   │   ├── cleaning.sql
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── intermediate/
│   │   │   └── commerce/
│   │   │       ├── _commerce__models.yml
│   │   │       ├── int_order_items_enriched.sql
│   │   │       └── int_orders_aggregated_from_items.sql
│   │   ├── marts/
│   │   │   └── core/
│   │   │       ├── _core__models.yml
│   │   │       ├── distribution_centers_dim.sql
│   │   │       ├── events.sql
│   │   │       ├── order_items.sql
│   │   │       ├── orders.sql
│   │   │       ├── products_dim.sql
│   │   │       ├── users_dim.sql
│   │   │       └── README.md
│   │   ├── staging/
│   │   │   └── look/
│   │   │       ├── _look__models.yml
│   │   │       ├── _look__sources.yml
│   │   │       ├── stg_look__distribution_centers.sql
│   │   │       ├── stg_look__events.sql
│   │   │       ├── stg_look__inventory_items.sql
│   │   │       ├── stg_look__order_items.sql
│   │   │       ├── stg_look__orders.sql
│   │   │       ├── stg_look__products.sql
│   │   │       └── stg_look__users.sql
│   │   └── README.md
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/
│   ├── README.md
│   ├── dbt_project.yml
│   ├── package-lock.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── .user.yml
├── venv/
├── .dbt_env
├── .env
├── .gitignore
├── README.md
└── requirements.txt

# `ingestion/thelook_ingest/config.py` — What it is

A tiny **configuration layer** for the ingestion jobs in `ingestion/thelook_ingest/`. It centralizes how the job discovers things like GCP project, cloud storage locations, auth, and the Unity Catalog so the rest of the code can just call simple functions (e.g., `get_project()`) instead of reading env vars everywhere.

---

## Why it exists

- **Single source of truth:** keeps all runtime settings in one place so other scripts don’t duplicate logic.
- **Environment-aware:** supports running the same code in:
  - Databricks Jobs (env vars + Secrets),
  - local development (sensible defaults),
  - service-account or OIDC auth.
- **Safe fallbacks:** if something isn’t set, the job still works with documented defaults.

---

## How it works (behavior)

1. **Config priority**
   1) **Environment variables** (recommended in Databricks Jobs)  
   2) **Databricks Secrets** (optional fallback via `dbutils.secrets`)  
   3) **Sensible defaults** (good for local/dev)

2. **Public getters used by ingestion code**
   - `get_project()` → GCP project ID (default: `analyticsproject-468700`)
   - `get_bucket()` → RAW bucket name (default: `analyticsbucketdev-sk`)
   - `get_raw_prefix()` → RAW path prefix inside the bucket (default: `raw/thelook`)
   - `get_bronze_prefix()` → (for dbt later) bronze path prefix (default: `bronze/thelook/delta`)
   - `get_uc_catalog()` → Unity Catalog name for external tables (default: `sujeet_data_analytics_workspace`)
   - `get_bq_auth_options()` → dict for BigQuery connector auth:
     - If `GOOGLE_APPLICATION_CREDENTIALS_B64` is set (base64 of SA JSON), returns `{"credentials": <b64>}`.
     - Otherwise tries the same key from Databricks Secrets (`DATABRICKS_SECRETS_SCOPE`, default `analyticsProject`).
     - If neither is present, returns `{}` so the connector uses cluster auth (OIDC/ADC).

3. **Databricks-only helpers**
   - `_get_dbutils()` and `_get_secret(scope, key)` safely access secrets **only if** `dbutils` exists; otherwise they no-op.

---

## Why it’s important

- **Portable & reproducible jobs:** the ingestion scripts can run the same way across dev/stage/prod without code changes.
- **Least-surprise auth:** first use explicit credentials; else fall back to managed identity/OIDC if the platform provides it.
- **Clear defaults:** new contributors can run locally without setting everything up.

# `ingestion/thelook_ingest/ingest_bigquery_to_raw.py` — Purpose & How it Works

A **Spark job** that **incrementally pulls** tables from the BigQuery public dataset `bigquery-public-data.thelook_ecommerce` and writes **append-only Parquet files** into a **Unity Catalog External Volume** (your RAW landing zone at `/Volumes/.../raw_thelook_files`, which maps to `s3://.../raw/thelook`).

The output is **files only** (no UC tables). Another job (Auto Loader) later reads these RAW files into Bronze tables.

---

## Why this script exists

- **Decouple ingestion from warehousing:** produce raw, immutable files partners can also drop into, while your pipelines read from a stable UC Volume path.
- **Reliable incrementals:** use per-table **cursors** and **grace windows** to avoid missing late-arriving data.
- **Serverless-friendly:** UC **External Volumes** allow secure S3 access without managing VPCs/instance profiles.
- **Small-file control:** configurable coalesce & partitions so storage layout is healthy for downstream jobs.

---

## What it does (high level)

1. **Sets runtime context**
   - Resolves the RAW **Volume path** (default `/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files`).
   - Gets GCP **project** and **BigQuery auth options** from `config.py`.
   - Stamps **ingest_date** (`YYYY-MM-DD`, UTC) and **run_ts** (`HHMMSS`, UTC).

2. **Defines table configs**
   - `TABLES` lists each source table with:
     - `change_cols`: columns to detect changes (created/updated/shipped/etc.).
     - `grace_minutes`: re-read window to catch late updates.
     - `run_every_minutes`: cadence gate to avoid too-frequent reads.
     - `is_dim`: dimensions with **no change columns** → do **full snapshot** on schedule.

3. **Maintains state per table (in the Volume)**
   - Stores JSON at `{RAW_VOLUME}/_state/{table}.json` with:
     - `cursor` (high-watermark),
     - `last_run_utc`,
     - last batch row count.
   - Uses `dbutils.fs` to **atomically write** state (`tmp + mv`).

4. **Builds the incremental window**
   - On first run: lower bound = **epoch** (1970-01-01 UTC), optionally include `NULL` change timestamps.
   - On later runs: **lower bound** = `cursor - grace_minutes` (capped at now).

5. **Reads from BigQuery via Spark connector**
   - `spark.read.format("bigquery")` with `parentProject` and auth options.
   - Applies a **pushdownable filter**: `to_timestamp(col) >= lower_bound` across all `change_cols` (`OR`-ed).  
     - If a table has no `change_cols`, it reads the **full table** (for dims).

6. **Writes RAW Parquet files**
   - Appends to `{RAW_VOLUME}/{table}/ingest_date=YYYY-MM-DD/run_ts=HHMMSS/part-*.snappy.parquet`.
   - Ensures the 4 **system columns** exist/stable:
     - `ingest_date` *(STRING, partition)*
     - `run_ts` *(STRING, partition)*
     - `ingest_ts_utc` *(TIMESTAMP)*
     - `source_table` *(STRING)*
   - Controls file count via `WRITE_PARTS` (default `16`).

7. **Advances the cursor**
   - Computes the **max observed change timestamp** across `change_cols` in the current batch.
   - **Caps at now** to avoid saving a future cursor.
   - Writes updated state JSON (or stamps `last_run_utc` for dim snapshots/no data).

8. **Cadence gating**
   - Skips a table if it hasn’t been at least `run_every_minutes` since the last run.  
     (Keeps noisy facts more frequent than slow-moving dims.)

9. **Optional visibility check**
   - Reads today’s `users` partition to print a simple row count for sanity.

---

## Inputs & Outputs

**Inputs**
- BigQuery tables: `orders`, `order_items`, `events`, `inventory_items`, `users`, `products`, `distribution_centers`.
- Config/env: RAW Volume path, GCP project, BQ auth, `WRITE_PARTS`.

**Outputs**
- Append-only Parquet files per table, partitioned by `ingest_date` and `run_ts` under the RAW Volume.
- Per-table state JSON under `{RAW_VOLUME}/_state`.

---

## Operational model

- **Schedule:** every **10–30 min** (single concurrent run).
- **First run:** backfills from epoch with a grace window to catch late rows and `NULL` timestamps.
- **Subsequent runs:** fetch only **deltas** since last cursor (with grace).  
- **No table creation:** the job **does not** create UC tables — it just lands files.

**Prereqs (already done in your setup)**
1. External Location `raw_thelook` → `s3://analyticsbucketdev-sk/raw`
2. External Volume `raw_thelook_files` in catalog.schema `sujeet_data_analytics_workspace.raw`  
   Path: `/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files`
3. Grants: READ/WRITE FILES on the Volume for the job principal
4. Spark BigQuery connector available (e.g., DBR 12+ builtin)

---

## Key functions to know

- **State & cadence**
  - `_load_state_blob`, `load_cursor`, `save_cursor`, `mark_dim_snapshot`
  - `_get_last_run_utc`, `_is_due`
- **Incremental window**
  - `_parse_cursor_to_dt_utc`, `_compute_lower_bound`, `_build_pushdown_filter`
- **I/O**
  - `bq_read_filtered` → Spark DataFrame from BQ with predicate pushdown
  - `write_raw` → append Parquet with proper partitions & system columns
- **Cursor advancement**
  - `compute_max_ts` → find max change ts across `change_cols`

The `main()` loop ties this together per table: **gate → read → add system cols → count → write → advance cursor**.

# `ingestion/thelook_ingest/raw_to_bronze_autoloader.py` — Purpose & How it Works

Promote immutable **RAW Parquet files** (landed by your BQ→RAW job) into **Unity Catalog Delta tables** in a Bronze schema using **Databricks Auto Loader**. It reads from a **UC Volume** path so it works on Serverless without extra IAM/NAT setup, and it uses **exactly-once** semantics via checkpoints.

---

## Why this script exists

- **Bridge files → tables:** turn partner-/job-dropped files in RAW into queryable Delta tables.
- **Serverless-friendly IO:** UC **External Volumes** give secure access to S3/GCS/Azure without cluster networking fuss.
- **Reliable ingestion:** Auto Loader + checkpoints = idempotent, incremental loads with schema evolution.
- **Cost-aware runs:** one table at a time with `availableNow=True` (batch-like), optional pacing via `maxFilesPerTrigger`.

---

## What it does (high level)

1. **Config & targets**
   - Resolves **catalog** (default `sujeet_data_analytics_workspace`) and **bronze schema** (default `bronze_dev`).
   - Uses **RAW Volume** path (default `/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files`).
   - Tables: `orders`, `order_items`, `events`, `inventory_items`, `users`, `products`, `distribution_centers`.

2. **Auto Loader per table**
   - **Source**: `${RAW_VOLUME}/{table}/**` (partitioned by `ingest_date`, `run_ts` in folder names).
   - **Metadata storage** under the same Volume (Serverless-friendly):
     - Schema: `${RAW_VOLUME}/_autoloader/_schemas/bronze/{table}`
     - Checkpoint: `${RAW_VOLUME}/_autoloader/_checkpoints/bronze/{table}`
   - **First run detection**: if checkpoint path doesn’t exist ⇒ `includeExistingFiles=true` (backfill). Otherwise only new files.

3. **Type hygiene**
   - Recasts the 4 RAW system columns defensively so appends don’t break:
     - `ingest_date` → `string`
     - `run_ts` → `string`
     - `ingest_ts_utc` → `timestamp`
     - `source_table` → `string`
   - Adds missing ones as NULLs to keep schemas consistent across batches.

4. **Write path**
   - `writeStream.toTable(<CATALOG>.<BRONZE_SCHEMA>.<table>)` with:
     - `checkpointLocation` (exactly-once),
     - `mergeSchema=true` (schema-on-read evolution),
     - `trigger(availableNow=True)` (process available files then stop).

5. **Run strategy**
   - Sequentially processes listed tables — simple & safe for small/Serverless clusters.

---

## Inputs & Outputs

**Inputs**
- Parquet files in the RAW Volume produced by the upstream ingestion job.

**Outputs**
- UC Delta tables in `<CATALOG>.<BRONZE_SCHEMA>.<table>` with exactly-once semantics.
- Auto Loader schema + checkpoint directories under the RAW Volume.

---

## Operational model

- **First run** of a table: backfills existing files (`includeExistingFiles=true`), creates the Delta table if needed.
- **Subsequent runs**: processes only new files discovered since the last checkpoint.
- **Scheduling**: run as a Databricks Job; Serverless SQL Warehouse or small All-Purpose cluster both work.

**Prereqs**
1. External Volume exists and points to your RAW bucket.
2. Principal has **READ FILES / WRITE FILES** on the Volume and **CREATE/MODIFY** on `<CATALOG>.<BRONZE_SCHEMA>`.
3. RAW writer populated partitions `ingest_date`, `run_ts` in folder names (as in your BQ→RAW job).

---

## Key functions / pieces

- `normalize_system_cols(df)` — casts/creates the 4 system columns.
- `path_exists(p)` — quick existence check for schema/checkpoint folders.
- `run_one_table(table)` — builds the Auto Loader stream, sets `includeExistingFiles`, writes to Delta with `availableNow`.
- Main block — iterates `TABLES` and calls `run_one_table` sequentially.