# Repository Structure

```
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
```


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

# `look_dbt/dbt_project.yml` — Purpose & How It Works

A dbt **project manifest** that defines how your Look e-commerce warehouse is built and organized. It wires your **catalog & schemas**, **paths**, **materializations**, and **model-level settings** so `dbt run/test` behaves consistently across environments.

---

## Why it exists

- **Single source of truth** for dbt settings (paths, quoting, flags).
- **Environment portability** via top-level `vars` that map to Unity Catalog + schemas.
- **Clear layer semantics**: staging → intermediate → marts with sane defaults per layer.

---

## Key pieces

### Project metadata & paths
- `name`, `version`, `config-version`: standard dbt identifiers.
- `profile: 'look_dbt'`: matches credentials/profile in `profiles.yml`.
- Path blocks (`model-paths`, `macro-paths`, etc.) tell dbt where to find code.

### Quoting & flags
- `quoting.database/schema/identifier: false`  
  → Use bare identifiers (assumes names are valid without quoting).
- `flags.use_materialization_v2: true`  
  → Opt into dbt’s v2 materialization framework.
- `require_explicit_package_overrides_for_builtin_materializations: false`  
  → Don’t force overrides for built-ins.

### Top-level variables (drive UC locations)
```yaml
vars:
  catalog_name: 'sujeet_data_analytics_workspace'
  bronze_schema: 'bronze_dev'
  silver_schema: 'silver_dev'
  gold_schema: 'gold_dev'
```
These are referenced in models to decide where objects live. You can override them per-env (e.g., prod vs dev) without editing SQL.

### Global model configs
```yaml
models:
  +on_schema_change: append_new_columns
  +persist_docs: { relation: true, columns: true }
```
- **Schema evolution**: append new columns if upstream adds them.
- **Docs persistence**: push descriptions to relations & columns.

### Per-package / folder settings (`look_dbt` tree)
- **staging/**
  - `+schema: '{{ var("silver_schema", "silver_dev") }}'`
  - `+materialized: view` (lightweight, easy to refresh)
  - `look/` inherits same schema.
- **intermediate/**
  - `+materialized: ephemeral` (inlined CTEs for transformations between staging and marts)
  - `commerce/` stays in `silver_schema` but ephemeral means no persisted tables.
- **marts/**
  - `+schema: '{{ var("gold_schema", "gold_dev") }}'`
  - `+materialized: table` (stable, query-ready outputs)
  - `core/` inherits gold schema and persists as tables.

---

## Why it’s important

- **Consistent layer contract**: views in staging, ephemeral for interstitial transforms, tables in marts.
- **Safe evolution**: `append_new_columns` prevents breakage when sources gain columns.
- **Docs in warehouse**: `persist_docs` surfaces descriptions to the database/catalog.

# `models/staging/look/_look__sources.yml` — Purpose & How It Works

Declares the **dbt sources** that staging models read from — here, the **Bronze** layer created by Auto Loader. This lets dbt understand upstream lineage, render docs, and (optionally) run freshness tests against the raw Delta tables.

---

## Why it exists

- **Stable references** to raw tables using `source()` (not hard‑coding database/schema names in SQL).
- **Lineage & docs**: sources show up in the dbt DAG and documentation site with descriptions.
- **Environment portability**: centralizes the UC **database** (catalog) and **schema** used by staging.

---

## What it defines

```yaml
version: 2

sources:
  - name: bronze_dev
    database: sujeet_data_analytics_workspace
    schema: bronze_dev
    description: "Raw incremental ingests from Databricks Autoloader for TheLook sample data."
    tables:
      - name: orders
        description: "Raw orders; duplicates may exist across overlapping ingest partitions."
      - name: order_items
        description: "Raw order items; duplicates may exist across overlapping ingest partitions."
      - name: events
        description: "Raw web/app events; may be anonymous."
      - name: users
        description: "Raw users."
      - name: products
        description: "Raw products."
      - name: inventory_items
        description: "Raw inventory items; includes product attributes as landed."
      - name: distribution_centers
        description: "Raw distribution centers."
```

- **`version: 2`** — enables the modern yaml spec for dbt sources.
- **`sources[].name`** — a logical name (`bronze_dev`) you’ll reference in SQL.
- **`database` / `schema`** — Unity Catalog **catalog** and **schema** where Bronze tables live.
- **`tables[]`** — the physical table names and their documentation strings.

---

## How models use it

In staging SQL, refer to a Bronze table like this:

```sql
select * from {{ source('bronze_dev', 'orders') }}
```

This avoids hard-coding `catalog.schema.table` and gives you lineage and docs automatically.

---

## Why it’s important

- **Consistency**: a single declaration controls where staging pulls data from.
- **Observability**: dbt docs show source descriptions; you can add freshness checks later.
- **Refactor safety**: if the Bronze schema changes, you update it here instead of every model.

**Optional enhancements**
- Add `freshness:` blocks per table for SLA checks.
- Add `tests:` like `not_null` on primary keys if your Bronze enforces them.
- Parameterize `database`/`schema` via `var()` if you deploy to multiple environments.

# `models/staging/look/_look__models.yml` — Purpose & How It Works

Defines **documentation and tests** for all **staging models** in the Look project. Each model declares key columns and quality checks (uniqueness, FKs, accepted values, ranges), ensuring clean, consistent inputs for downstream intermediate/marts layers.

---

## Why it exists

- **Contracts for staging**: specify the shape and integrity rules for each table.
- **Automatic testing**: dbt runs these checks on `dbt test` to catch data drift early.
- **Discoverability**: descriptions populate the dbt docs site, improving lineage & context.

---

## What it defines (high level)

- One `models:` entry per staging model with:
  - **`description`** of the dataset after staging (deduped, typed, normalized).
  - **`columns:`** list, where each column can have:
    - `tests:` (e.g., `not_null`, `unique`, `relationships`, `accepted_values`).
    - Optional `description` and conditionals (e.g., `where:`) for targeted tests.

---

## Model-by-model highlights

### `stg_look__orders`
- Guarantees row-level identity and recency via `order_id` **unique + not_null**.
- Enforces `user_id` presence.
- Restricts `status` to one of: `Complete, Shipped, Returned, Cancelled, Processing, Unknown`.
- Requires `created_at` not null.

### `stg_look__order_items`
- Row identity on `order_item_id` (**unique + not_null**).
- **Foreign keys**:
  - `order_id` → `stg_look__orders.order_id`
  - `user_id` → `stg_look__users.user_id`
  - `product_id` → `stg_look__products.product_id`
  - `inventory_item_id` → `stg_look__inventory_items.inventory_item_id` (as **warn**, and optionally filtered to finalized statuses).
- `status` accepted values match the orders domain.
- `sale_price` must be **≥ 0** (nullable until priced).

### `stg_look__events`
- Identity: `event_id` **unique + not_null**.
- Optional FK: `user_id` may be null (anonymous) but, when present, must exist in `stg_look__users`.
- `event_type` constrained to `product, cart, home, cancel, purchase, department, unknown`.
- `created_at` required.

### `stg_look__users`
- Identity: `user_id` **unique + not_null**.
- `email` required.

### `stg_look__products`
- Identity: `product_id` **unique + not_null**.
- FK: `distribution_center_id` must exist in `stg_look__distribution_centers`.

### `stg_look__inventory_items`
- Identity: `inventory_item_id` **unique + not_null**.
- FKs:
  - `product_id` → `stg_look__products.product_id` (**not_null** and related).
  - `product_distribution_center_id` → `stg_look__distribution_centers.distribution_center_id`.

### `stg_look__distribution_centers`
- Identity: `distribution_center_id` **unique + not_null**.
- `name` required.

---

## Why it’s important

- **Data reliability**: invalid statuses, broken FKs, or negative prices are flagged before they reach marts.
- **Clear contracts**: downstream models can rely on deduped keys and typed fields.
- **Actionable tests**: the `warn` severity + `where` filter reduce alert noise for known edge cases (early-arriving items).

**Tips**
- Use `relationships` for **FK checks** across staging models.
- Add `dbt_utils.accepted_range` to validate numeric/temporal bounds.
- Consider `where:` to narrow tests to finalized records and avoid false positives.
- Keep descriptions succinct so dbt docs stay readable.

# `models/staging/look/stg_look__distribution_centers.sql` — Purpose & How it Works

Produces a **clean, deduplicated staging table** of distribution centers from the Bronze source, keeping the **latest version per `distribution_center_id`** based on the ingestion timestamp. It standardizes data types and trims strings so downstream models have a stable, typed dimension seed.

---

## Why it exists

- **Deduplicate** multiple RAW arrivals of the same distribution center (overlapping ingest batches).
- **Choose the freshest row** deterministically using an ingestion timestamp.
- **Normalize types** (e.g., `bigint`, `double`) and clean text for reliable joins and aggregations.

---

## How it works

### 1) `raw` CTE — select & construct ingestion timestamp
- Pulls from the Bronze source: `{{ source('bronze_dev','distribution_centers') }}`.
- Renames `id` → `distribution_center_id` and selects key attributes.
- Derives a **source ingestion timestamp** `src_ingest_ts`:
  - Prefer `ingest_ts_utc` if present.
  - Else synthesize one from partition columns: `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')`.

### 2) `ranked` CTE — latest-per-id
- Adds `row_number()` over `partition by distribution_center_id order by src_ingest_ts desc`.
- `rn = 1` marks the **newest record** for each id.

### 3) Final select — typing & cleanup
- Casts to consistent types:
  - `distribution_center_id` → **bigint**
  - `latitude`, `longitude` → **double**
- Trims text fields: `name`, `distribution_center_geom`.
- Returns only `rn = 1` rows (deduped, latest version).

---

## Why it’s important

- **Idempotent staging**: windowed dedupe ensures one record per key.
- **Stable schemas**: typed numeric and string fields prevent downstream cast issues.
- **Deterministic freshness**: ingestion timestamps avoid ambiguity when the same id arrives multiple times.

# `models/staging/look/stg_look__events.sql` — Purpose & How it Works

Builds a **clean, deduplicated events staging table** from Bronze, standardizing types and collapsing messy `event_type` values into a **stable enum**. It keeps only the **latest version per `event_id`** based on an ingestion timestamp.

---

## Why it exists

- **Unify timestamps** for deterministic dedupe and lineage.
- **Remove duplicates** created by overlapping RAW ingests.
- **Normalize event types** to a fixed set for downstream models and tests.

---

## How it works

### 1) `raw` CTE — select + ingestion timestamp
- Source: `{{ source('bronze_dev','events') }}`.
- Renames `id` → `event_id` and selects user/session, network, and clickstream attributes.
- Derives `src_ingest_ts`:
  - prefer `ingest_ts_utc` if present,
  - else synthesize from partitions: `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')`.

### 2) `ranked` CTE — latest-per-id
- `row_number()` over `partition by event_id order by src_ingest_ts desc`.
- `rn = 1` keeps only the most recent row for each event.

### 3) Final projection — typing + normalization
- **Type casts** (stable schema):
  - `event_id`, `user_id`, `sequence_number` → `bigint`
  - `session_id` → `string`
  - `created_at` → `timestamp`
- **Trim** common string fields (`ip_address`, `city`, `state`, `postal_code`, `browser`, `traffic_source`, `uri`).
- **Normalize `event_type`** to a fixed set (case-insensitive):
  - `product, cart, home, cancel, purchase, department`, else `unknown`.

---

## Why it’s important

- **Deterministic dedupe**: picks the freshest record for each event.
- **Stable semantics**: a controlled `event_type` enum powers accurate marts and tests.
- **Consistent typing**: avoids downstream casting issues and improves join performance.

**Tips**
- Treat `user_id` as nullable in tests and modeling (anonymous events).
- Keep normalization **explicit**; don’t rely on free-form values beyond the allowed set.
- Revisit the enum list when new event types are introduced upstream.

# `models/staging/look/stg_look__inventory_items.sql` — Purpose & How it Works

Creates a **deduplicated, typed staging table** for inventory items from Bronze, keeping the **latest record per `inventory_item_id`** and preserving useful landed **product attributes** (name/brand/category/etc.) for downstream convenience.

---

## Why it exists

- **Resolve duplicates** from overlapping RAW ingests.
- **Stabilize schema & types** for reliable joins and aggregates.
- **Carry through product attributes** that are frequently referenced with inventory rows.

---

## How it works

### 1) `raw` CTE — select + ingestion timestamp
- Source: `{{ source('bronze_dev','inventory_items') }}`.
- Renames `id` → `inventory_item_id`; selects key product and lifecycle fields.
- Derives `src_ingest_ts`:
  - Prefer `ingest_ts_utc` if present;
  - Else `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')` from partition fields.

### 2) `ranked` CTE — latest-per-id dedupe
- `row_number()` over `partition by inventory_item_id order by src_ingest_ts desc`.
- `rn = 1` → keep the freshest record for each item.

### 3) Final select — typing & cleanup
- Casts:
  - Keys & FKs → `bigint` (`inventory_item_id`, `product_id`, `product_distribution_center_id`)
  - Timestamps → `timestamp` (`created_at`, `sold_at`)
  - Money-like fields → `decimal(18,2)` (`cost`, `product_retail_price`)
- Trims strings: `product_category`, `product_name`, `product_brand`, `product_department`, `product_sku`.
- Emits `src_ingest_ts` for lineage/debugging.

---

## Why it’s important

- **Deterministic snapshots** of inventory rows (latest-only per id).
- **Consistent types** prevent downstream casting errors and aid performance.
- **Convenience columns** (product attributes) simplify joins in later layers.

**Tips**
- Keep a consistent numeric precision/scale for currency fields across staging models.
- If upstream adds columns, let dbt’s schema evolution (and your Bronze writer) handle them; trimming & typing here maintains stability.
- Retain `src_ingest_ts` to trace which batch produced a given row.

# `models/staging/look/stg_look__order_items.sql` — Purpose & How it Works

Builds a **deduplicated, typed staging table** for order items from Bronze. It keeps the **latest record per `order_item_id`**, normalizes `status` into a **controlled enum**, and maintains monetary fields as `DECIMAL(18,2)`.

---

## Why it exists

- **Deterministic dedupe** of overlapping RAW ingests (same id, multiple arrivals).
- **Stable semantics**: consistent `status` values and timestamps for downstream joins/tests.
- **Financial correctness**: cast `sale_price` to fixed precision for reliable aggregations.

---

## How it works

### 1) `raw` CTE — source + ingestion timestamp
- Source: `{{ source('bronze_dev','order_items') }}`.
- Selects core keys (order/item/product/user/inventory), lifecycle timestamps, and `sale_price`.
- Derives `src_ingest_ts` via:
  - `ingest_ts_utc` if present,
  - else `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')` from landing partitions.

### 2) `ranked` CTE — dedupe to latest
- `row_number()` over `partition by order_item_id order by src_ingest_ts desc`.
- Keep only `rn = 1` — the freshest item row.

### 3) Final projection — typing & normalization
- **Type casts**
  - IDs & FKs → `bigint`: `order_item_id`, `order_id`, `user_id`, `product_id`, `inventory_item_id`.
  - Lifecycle times → `timestamp`: `created_at`, `shipped_at`, `delivered_at`, `returned_at`.
  - Money → `decimal(18,2)`: `sale_price`.
- **Normalize `status`** (case-insensitive) to one of:
  - `Complete`, `Shipped`, `Returned`, `Cancelled`, `Processing`, else `Unknown`.
- Emits `src_ingest_ts` for lineage/debugging.

---

## Why it’s important

- **Reliable downstream logic**: marts/tests expect a tight `status` enum and consistent types.
- **Accurate finance**: fixed-precision `sale_price` prevents float rounding issues.
- **Idempotent pipeline**: latest-per-id selection aligns with RAW partition semantics.

---

**Tips**
- Keep the `status` mapping in sync with tests in `_look__models.yml`.
- If early-arriving inventory rows are common, keep FK relationship tests for `inventory_item_id` at `severity: warn`.
- Consider adding guards for negative prices in downstream tests.

# `models/staging/look/stg_look__orders.sql` — Purpose & How it Works

Produces a **deduplicated, typed staging table** for orders from Bronze. It derives a unified ingestion timestamp, keeps only the **latest row per `order_id`**, and normalizes `status` to a controlled set.

---

## Why it exists

- **Deterministic dedupe**: overlapping RAW ingests can land the same order multiple times.
- **Stable semantics**: controlled `status` values make downstream logic/tests reliable.
- **Typed schema**: consistent types for joins, analytics, and dbt tests.

---

## How it works

### 1) `raw` CTE — source + unified ingestion time
- Source: `{{ source('bronze_dev','orders') }}`.
- Selects identity, user, lifecycle timestamps, counts, and status.
- Builds `src_ingest_ts` as:
  - `ingest_ts_utc` **if present** (preferred true timestamp),
  - else `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')` from landing partitions.

### 2) `ranked` CTE — choose latest per order
- `row_number()` over `partition by order_id order by src_ingest_ts desc`.
- `rn = 1` keeps the **freshest version** of each order.

### 3) Final projection — typing & normalization
- Casts:
  - `order_id`, `user_id`, `num_of_item` → **bigint**.
  - Lifecycle fields `created_at`, `returned_at`, `shipped_at`, `delivered_at` → **timestamp**.
- **Normalize `status`** (case-insensitive) to:
  - `Complete`, `Shipped`, `Returned`, `Cancelled`, `Processing`, else `Unknown`.
- Emits `src_ingest_ts` for lineage/debugging.

---

## Why it’s important

- Ensures **one row per order** with the newest data across overlapping batches.
- Keeps **status vocabulary** aligned with tests and downstream marts.
- Preserves ingestion lineage for audits and troubleshooting.

**Tip**
- Keep the status mapping in sync with `_look__models.yml` tests to avoid drift.

# `models/staging/look/stg_look__products.sql` — Purpose & How it Works

Creates a **deduplicated, typed products staging table** from Bronze. It keeps the **latest row per `product_id`**, standardizes monetary/text types, and **carries the distribution_center_id** for downstream enrichment/joins.

---

## Why it exists

- **Resolve duplicates** caused by overlapping RAW partitions.
- **Provide stable types** for pricing fields and product attributes.
- **Retain DC linkage** so products can be joined to distribution center details without extra hops.

---

## How it works

### 1) `raw` CTE — source + ingestion timestamp
- Source: `{{ source('bronze_dev','products') }}`.
- Renames `id` → `product_id`; selects price/brand/category and DC id.
- Builds `src_ingest_ts`:
  - prefer `ingest_ts_utc` if present,
  - else parse from partitions: `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')`.

### 2) `ranked` CTE — dedupe latest-per-id
- `row_number()` over `partition by product_id order by src_ingest_ts desc`.
- Keep only `rn = 1` (freshest product row).

### 3) Final projection — typing & cleanup
- Casts:
  - `product_id`, `distribution_center_id` → **bigint**.
  - `cost`, `retail_price` → **decimal(18,2)**.
- Trims text fields: `category`, `name`, `brand`, `department`, `sku`.
- Emits `src_ingest_ts` for lineage/debugging.

---

## Why it’s important

- **Deterministic one-row-per-product** guarantees stable downstream dimensions.
- **Financial correctness** with fixed-precision decimals.
- **Easy enrichment** using `distribution_center_id` with the staged DC table.

**Tips**
- Keep currency precision/scale consistent across staging models.
- Consider adding tests for non-negative `retail_price` downstream.
- Use the DC id to build a product dimension enriched with DC attributes in intermediate/marts layers.

# `models/staging/look/stg_look__users.sql` — Purpose & How it Works

Builds a **deduplicated, typed users staging table** from Bronze, keeping the **latest record per `user_id`** and preserving PII fields so downstream layers can choose what to expose or hash.

---

## Why it exists

- **Resolve duplicates** from overlapping RAW ingests.
- **Provide stable typing & cleaned text** for reliable joins and analytics.
- **Retain PII at staging** to allow flexible downstream privacy choices (e.g., masking in marts).

---

## How it works

### 1) `raw` CTE — source + ingestion timestamp
- Source: `{{ source('bronze_dev','users') }}`.
- Renames `id` → `user_id`; selects demographics, address, geo, attribution, created time.
- Derives `src_ingest_ts`:
  - prefer `ingest_ts_utc` if present,
  - else synthesize from partitions: `to_timestamp(ingest_date || ' ' || run_ts, 'yyyy-MM-dd HHmmss')`.

### 2) `ranked` CTE — dedupe to latest per user
- `row_number()` over `partition by user_id order by src_ingest_ts desc`.
- Keep only `rn = 1` — the freshest record for each user id.

### 3) Final projection — typing & cleanup
- Casts:
  - `user_id`, `age` → **bigint**
  - `latitude`, `longitude` → **double**
  - `created_at` → **timestamp**
- Trims strings for names, contact, address, attribution fields.
- Retains `user_geom` (trimmed) and `src_ingest_ts` for lineage.

---

## Why it’s important

- **Deterministic latest-per-id** ensures one canonical row per user.
- **Consistent types** and cleaned text prevent downstream cast and join issues.
- **Privacy-aware design**: keep PII at staging; downstream marts can anonymize or limit columns.

**Tips**
- If you later mask PII, keep the original in staging and expose hashed/nullable versions in marts.
- Consider uniqueness + not_null tests on `user_id` and `email` (if business rules allow).

# `models/intermediate/commerce/_commerce__models.yml` — Purpose & How It Works

Documents and tests **intermediate-layer** commerce models. These sit between staging and marts: one enriches order items with product/DC attributes; the other aggregates items up to orders. The YAML defines **descriptions** and **data tests** so dbt can validate assumptions before marts.

---

## Why it exists

- **Data contracts** for the intermediate layer (grain, relationships, ranges).
- **Automated validation** on `dbt test` to catch data drift early (FKs, non-negative sums, etc.).
- **Clear lineage**: fields and constraints appear in dbt docs and DAG.

---

## Models

### `int_order_items_enriched`
- **Grain**: one row per `order_item_id`.
- **Purpose**: join staged order items to staged products and distribution centers for convenient attributes (e.g., product/DC columns alongside each item).
- **Key tests**
  - `order_item_id` → `not_null`, `unique` (row identity).
  - FKs:
    - `order_id` → `stg_look__orders.order_id`
    - `user_id` → `stg_look__users.user_id`
    - `product_id` → `stg_look__products.product_id`
    - `distribution_center_id` → `stg_look__distribution_centers.distribution_center_id`
  - `sale_price` → `dbt_utils.accepted_range(min_value=0, inclusive=true)` (nullable but never negative).

### `int_orders_aggregated_from_items`
- **Grain**: one row per `order_id`.
- **Purpose**: roll up item-level facts to orders (e.g., item count, gross revenue) while keeping canonical `user_id` from the orders staging table.
- **Key tests**
  - `order_id` → `not_null`, `unique`.
  - `user_id` → `relationships` with `stg_look__users.user_id`.
  - `item_count` → `dbt_utils.accepted_range(min_value=0, inclusive=true)`.
  - `items_gross_revenue` → `dbt_utils.accepted_range(min_value=0, inclusive=true)`.

---

## Why it’s important

- **Pre-mart integrity**: guarantees that enrichment and aggregation produce clean, joinable rows.
- **Performance**: centralizes joins/rollups once, so marts can reuse trusted intermediate results.
- **Documentation**: readable contracts for team members and the dbt docs site.

# `models/intermediate/commerce/int_order_items_enriched.sql` — Purpose & How It Works

An **incremental dbt model** that enriches **order items** with **inventory**, **product**, and **distribution center (DC)** attributes. The output grain is **one row per `order_item_id`** with convenient item, product, and DC fields ready for order-level rollups.

---

## Why it exists

- **Centralizes enrichment** so downstream models don’t repeat joins.
- **Keeps row grain stable** (item-level) while bringing in product/DC context.
- **Efficient incremental loads**: only new items append to the table.

---

## How it works

### Model config
```jinja
{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='append_new_columns'
) }}
```
- **`materialized='incremental'`** — appends new rows only.
- **`unique_key='order_item_id'`** — row identity for incremental logic.
- **`on_schema_change='append_new_columns'`** — tolerate upstream schema evolution.

### Inputs
- `stg_look__order_items` (`oi`) — canonical item facts and keys.
- `stg_look__inventory_items` (`inv`) — item lifecycle + landed product/DC link.
- `stg_look__products` (`prod`) — product attributes (name/brand/category/price/sku).
- `stg_look__distribution_centers` (`dc`) — DC attributes (name/lat/lon).

### Enrichment steps
1. **Join item → inventory → product** (`items_joined`)
   - Select item keys/facts (`order_item_id`, `order_id`, `user_id`, `product_id`, `inventory_item_id`, `item_status`, `sale_price`, `item_created_at`).
   - Bring inventory’s `product_distribution_center_id` and product attributes.
   - **Resolve `distribution_center_id`** with a fallback:
     ```sql
     coalesce(inv.product_distribution_center_id, prod.distribution_center_id)
     ```
     Use inventory’s DC first (most specific), else product’s DC.

2. **Attach DC details** (`items_with_dc`)
   - Left join to `dc` on `distribution_center_id` to add `distribution_center_name`, `distribution_center_latitude`, `distribution_center_longitude`.

3. **Final select**
   - `select * from items_with_dc` exposes the enriched, item-grain dataset.

### Incremental guard
```jinja
{% if is_incremental() %}
  where order_item_id not in (select order_item_id from {{ this }})
{% endif %}
```
- Prevents **reprocessing existing items** when upstream sources are append-only.
- New `order_item_id`s are appended; existing ids are skipped.

---

## Output (example fields)

- **Keys**: `order_item_id` (grain), `order_id`, `user_id`, `product_id`, `inventory_item_id`
- **Item facts**: `item_status`, `sale_price`, `item_created_at`
- **Product attrs**: `product_name`, `product_brand`, `product_category`, `product_department`, `retail_price`, `product_sku`
- **DC linkage & attrs**: `distribution_center_id`, `distribution_center_name`, `distribution_center_latitude`, `distribution_center_longitude`

---

## Why it’s important

- **Reusability**: marts (orders, revenue, fulfillment) can reuse a single, enriched item source.
- **Performance**: heavy joins happen once; downstream models stay lightweight.
- **Safety**: incremental append + unique key and schema evolution reduce fragility.

**Tips**
- If upstream **mutates** existing item rows, switch to an **incremental merge** pattern instead of `NOT IN` filtering.
- Keep the **grain and unique key aligned**; mismatches cause duplicate rows on append.
- Consider indexing/sorting by `order_id` or `distribution_center_id` in the target warehouse for query speed.

# `models/intermediate/commerce/int_orders_aggregated_from_items.sql` — Purpose & How It Works

Builds an **order-grain intermediate view** by aggregating enriched **order items**. It ensures a **single row per order**, carries the **canonical `user_id`** from the orders staging table, and computes common rollups for downstream marts.

---

## Why it exists

- **Consolidate item-level facts** into order-level metrics once (reuse everywhere).
- **Guarantee canonical keys**: choose `user_id` from the orders table to avoid conflicts from item joins.
- **Keep marts light**: expose a ready-to-use order summary (counts, revenue, first item time).

---

## How it works

### Model config
```jinja
{{ config(materialized='view') }}
```
Creates a view so results are always up to date with the underlying tables.

### Inputs
- `int_order_items_enriched` (`items`) — item-grain dataset with product/DC fields and `sale_price`, `item_created_at`.
- `stg_look__orders` (`orders`) — **one row per order** with canonical `user_id` and `created_at`.

### Aggregation
```sql
select
  i.order_id,
  o.user_id as user_id,                  -- canonical user id
  min(i.item_created_at) as order_first_item_at,
  count(*)               as item_count,
  sum(coalesce(i.sale_price, 0.0)) as items_gross_revenue
from items i
left join orders o on i.order_id = o.order_id
group by i.order_id, o.user_id
```
- **`min(item_created_at)`** — the earliest item timestamp for order timing.
- **`count(*)`** — number of items on the order.
- **`sum(coalesce(sale_price, 0))`** — total **gross revenue** from items (treats null as 0).

---

## Output fields

- `order_id` — order grain (primary key).
- `user_id` — canonical from orders staging.
- `order_first_item_at` — first item timestamp.
- `item_count` — count of items on the order.
- `items_gross_revenue` — sum of item `sale_price` (nullable items → 0).

---

## Why it’s important

- **Stable order identity** with consistent `user_id` selection.
- **Reusable KPIs** (count, revenue, timing) shared across marts (core, fulfillment, marketing).
- **Predictable null-handling** for revenue to prevent undercounting.

**Tips**
- If you later need net revenue, add discounts/returns logic here or in a sibling model.
- Consider casting revenue to a fixed precision/scale on your target platform.
- Index/cluster by `order_id` (and optionally `user_id`) if your warehouse supports it for faster joins.

# `models/marts/core/_core__models.yml` — Purpose & How It Works

Documents and tests the **Core marts**: a **line-item fact** (`order_items`) and an **order-grain fact** (`orders`). This YAML captures **business-level contracts** (grain, relationships, non-negativity) so dbt can validate metrics and keys before analytics consume them.

---

## Why it exists

- **Trustworthy marts**: enforce keys, foreign keys, and metric constraints close to the business layer.
- **Clear lineage**: ties facts back to staging dimensions (users, products, DCs) in docs/DAG.
- **Guardrails on metrics**: assert non-negative counts/revenue to catch upstream drift.

---

## Models

### `order_items` (line-item fact)
- **Grain**: one row per `order_item_id`.
- **Purpose**: core transactional fact enriched with product and DC context.
- **Key tests**
  - `order_item_id` → `not_null`, `unique` (row identity).
  - `order_id` → `not_null` and `relationships` with `stg_look__orders.order_id`.
  - `user_id` → `relationships` with `stg_look__users.user_id`.
  - `product_id` → `relationships` with `stg_look__products.product_id`.
  - `distribution_center_id` → `relationships` with `stg_look__distribution_centers.distribution_center_id`.
  - `sale_price` → `dbt_utils.accepted_range(min_value=0, inclusive=true)` (nullable, never negative).

### `orders` (order-grain fact)
- **Grain**: one row per `order_id`.
- **Purpose**: order-level summary (created time, item count, revenue) with canonical `user_id`.
- **Key tests**
  - `order_id` → `not_null`, `unique`.
  - `user_id` → `not_null` and `relationships` with `stg_look__users.user_id`.
  - `created_at` → `not_null`.
  - `item_count` → `dbt_utils.accepted_range(min_value=0, inclusive=true)`.
  - `order_gross_revenue` → `dbt_utils.accepted_range(min_value=0, inclusive=true)`.

---

## Why it’s important

- **Business-facing reliability**: marts power dashboards; these tests prevent bad data reaching BI.
- **Consistent joins**: enforced relationships align facts with staged dimensions.
- **Metric hygiene**: non-negative constraints catch missing prices or duplication early.

# `models/marts/core/distribution_centers_dim.sql` — Purpose & How It Works

A **dimension table** for distribution centers built from the staged, deduplicated source. It’s materialized as a **table** for fast joins from facts (orders/items).

---

## Why it exists

- Provide a **conformed dimension** of DC attributes for analytics and lookups.
- Ensure **stable naming** and **ready-to-join columns** across marts.
- Keep **ingestion lineage** (`src_ingest_ts`) for troubleshooting/audits.

---

## How it works

### Model config
```jinja
{{ config(materialized='table') }}
```
Persists rows as a physical table (good for frequent joins).

### Select & rename
```sql
select
  distribution_center_id,
  name       as distribution_center_name,
  latitude   as distribution_center_latitude,
  longitude  as distribution_center_longitude,
  distribution_center_geom,
  src_ingest_ts
from {{ ref('stg_look__distribution_centers') }}
```
- Pulls from the **staging** DC model (already deduped & typed).
- Renames columns to a **clear prefixed schema** (`distribution_center_*`) for self-documenting joins.

---

## Output columns

- `distribution_center_id` — natural/business key used by facts.
- `distribution_center_name` — human-readable name.
- `distribution_center_latitude`, `distribution_center_longitude` — geo coordinates.
- `distribution_center_geom` — original geometry string (if available).
- `src_ingest_ts` — ingestion lineage from staging.

---

## Why it’s important

- Central, consistent DC attributes for **order_items** and other marts to join.
- Avoids repeated renaming/casting in every downstream model.
- Keeps provenance for data governance.


# `models/marts/core/events.sql` — Purpose & How It Works

A **fact table at event grain (`event_id`)** built from the staged events model. It’s materialized as a **table** (modest volume, frequently queried), exposing user/session context and normalized `event_type` for downstream analytics.

---

## Why it exists

- Provide a **clean, query-ready event fact** with stable types and enums.
- Keep high-signal web/app fields (source, browser, URI, geo) for funnels and attribution.
- Persist as a table to speed up joins and BI queries.

---

## How it works

### Model config
```jinja
{{ config(materialized='table') }}
```
Persists the dataset as a physical table for performance.

### Selection from staging
```sql
select
  event_id,
  user_id,
  event_type,
  created_at,
  browser,
  traffic_source,
  uri,
  city, state, postal_code, ip_address,
  src_ingest_ts
from {{ ref('stg_look__events') }}
```
- **Source**: `stg_look__events` is already **deduped and typed**, with `event_type` normalized to a fixed set.
- Passes through key attributes and `src_ingest_ts` for lineage.

---

## Output columns

- `event_id` — primary key (event grain).
- `user_id` — nullable FK (anonymous events allowed in staging/tests).
- `event_type` — controlled enum (e.g., `product`, `cart`, `home`, `cancel`, `purchase`, `department`, `unknown`).
- `created_at` — event timestamp.
- `browser`, `traffic_source`, `uri` — high-signal attribution/context.
- `city`, `state`, `postal_code`, `ip_address` — geo/network context.
- `src_ingest_ts` — ingestion lineage from staging.

---

## Why it’s important

- **Ready-made event fact** for funnels, sessions, and attribution models.
- **Consistency** with staging enum/testing means fewer surprises in marts/BI.
- **Performance**: table materialization avoids recomputing the staging logic for every query.

# `models/marts/core/order_items.sql` — Purpose & How It Works

A **wide line‑item fact** that surfaces enriched order items (with product & distribution center context) to the **Core** layer. It’s **incremental** and prunes new loads by `src_ingest_ts` for efficient refreshes.

---

## Why it exists

- Provide a **query‑ready fact at item grain** with all common attributes in one table.
- **Reuse** the heavy joins done in `int_order_items_enriched` instead of repeating them in marts/BI.
- **Efficient loads** via incremental append using a timestamp watermark (`src_ingest_ts`).

---

## How it works

### Model config
```jinja
{{ config(
  materialized = 'incremental',
  unique_key   = 'order_item_id',
  on_schema_change = 'append_new_columns'
) }}
```
- **incremental**: append new rows only.
- **unique_key**: `order_item_id` is the row identity.
- **append_new_columns**: tolerate upstream schema evolution without rebuilds.

### Query pattern
```sql
select * from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where src_ingest_ts > coalesce(
  (select max(src_ingest_ts) from {{ this }}),
  timestamp'1970-01-01 00:00:00'
)
{% endif %}
```
- **Base**: select all columns from the **intermediate enriched items**.
- **Incremental filter**: load only rows with `src_ingest_ts` **greater than the current max** in the target table.
  - First run falls back to the epoch timestamp.

---

## Output

All columns from `int_order_items_enriched`, e.g.:
- Keys: `order_item_id` (grain), `order_id`, `user_id`, `product_id`, `inventory_item_id`
- Item facts: `item_status`, `sale_price`, `item_created_at`
- Product attrs: `product_name`, `product_brand`, `product_category`, `retail_price`, `product_sku`
- DC attrs: `distribution_center_id`, `distribution_center_name`, `distribution_center_latitude`, `distribution_center_longitude`
- Lineage: `src_ingest_ts`

---

## Why it’s important

- **Fast incremental loads** using a robust timestamp watermark.
- **Single source of truth** for item‑level analytics across dashboards and downstream models.
- **Schema resilience** as new columns appear upstream.

**Tips**
- If upstream can **update existing items** (not just append), switch to an **incremental MERGE** keyed on `order_item_id` instead of a pure timestamp filter.
- Consider adding **numeric precision** on money fields at the mart layer if your warehouse benefits from it.

# `models/marts/core/orders.sql` — Purpose & How It Works

An **incremental Core fact** at **order grain** that combines canonical order attributes with **item-level aggregates** (count, gross revenue). It prunes loads by an **ingestion watermark** (`src_ingest_ts`) from the staged orders table.

---

## Why it exists

- Provide a **query-ready order table** with the most-used KPIs.
- **Reuse** intermediate rollups instead of recalculating per mart/BI query.
- **Efficient refreshes** using an incremental timestamp watermark.

---

## How it works

### Model config
```jinja
{{ config(
  materialized = 'incremental',
  unique_key   = 'order_id',
  on_schema_change = 'append_new_columns'
) }}
```
- **incremental**: append new/updated orders only.
- **unique_key**: `order_id` is the order identity.
- **append_new_columns**: allow upstream schema evolution without full rebuilds.

### Inputs
- `stg_look__orders` → **canonical** one row per order (status, user_id, created_at, `src_ingest_ts`).
- `int_orders_aggregated_from_items` → item-level rollups per order (`item_count`, `items_gross_revenue`).

### Assembly
```sql
select
  o.order_id,
  o.user_id,
  o.status,
  o.created_at,
  coalesce(a.item_count, 0)            as item_count,
  coalesce(a.items_gross_revenue, 0.0) as order_gross_revenue,
  o.src_ingest_ts
from {{ ref('stg_look__orders') }} o
left join {{ ref('int_orders_aggregated_from_items') }} a
  on o.order_id = a.order_id
```
- **`COALESCE`** ensures orders with no items still have `item_count = 0` and revenue `= 0.0`.

### Incremental pruning
```jinja
{% if is_incremental() %}
where src_ingest_ts > coalesce(
  (select max(src_ingest_ts) from {{ this }}),
  timestamp'1970-01-01 00:00:00'
)
{% endif %}
```
- Load only orders **newer than** the current max ingestion timestamp in the target table.
- First run falls back to epoch.

---

## Output columns

- `order_id` — primary key (order grain).
- `user_id`, `status`, `created_at` — canonical attributes from staging.
- `item_count` — number of items on the order.
- `order_gross_revenue` — sum of item sale prices.
- `src_ingest_ts` — ingestion watermark enabling incremental loads.

---

## Why it’s important

- **BI-ready** order table with consistent metrics.
- **Performance-friendly** incremental updates; avoids full table recompute.
- **Robust to schema evolution** via `append_new_columns`.

---

**Tips**
- If upstream can **modify existing orders**, consider an **incremental MERGE** keyed on `order_id` to upsert changes.
- Align tests in `_core__models.yml` (`item_count`/`order_gross_revenue` non-negative) with this model’s null-handling.

# `models/marts/core/products_dim.sql` — Purpose & How It Works

A **conformed Product dimension** materialized as a **table**. It lifts the deduplicated, typed products from staging and provides clear, prefixed attribute names for easy joins from facts.

---

## Why it exists

- Centralize **product attributes** in one dimension for reuse across marts/BI.
- Provide **stable naming** (`product_*`) and consistent typing for joins.
- Preserve **ingestion lineage** (`src_ingest_ts`) for auditability.

---

## How it works

### Model config
```jinja
{{ config(materialized='table') }}
```
Persists rows as a physical table (good for frequent joins).

### Select & rename from staging
```sql
select
  product_id,
  name       as product_name,
  brand      as product_brand,
  category   as product_category,
  department as product_department,
  sku        as product_sku,
  retail_price,
  cost,
  distribution_center_id,
  src_ingest_ts
from {{ ref('stg_look__products') }}
```
- Source is **`stg_look__products`** (already deduped & typed).
- Renames business columns to **`product_*`** for self-documenting joins.
- Exposes `retail_price`, `cost`, and `distribution_center_id` for analytics/enrichment.

---

## Output columns

- `product_id` — product key.
- `product_name`, `product_brand`, `product_category`, `product_department`, `product_sku` — descriptive attributes.
- `retail_price`, `cost` — price/cost measures (typed upstream).
- `distribution_center_id` — linkage to the DC dimension.
- `src_ingest_ts` — ingestion lineage.

---

## Why it’s important

- **Single source of truth** for product attributes.
- **Cleaner downstream SQL**: no repeated renaming or casting in facts/marts.
- **Governance**: lineage timestamp retained.

# `models/marts/core/users_dim.sql` — Purpose & How It Works

A **conformed User dimension** materialized as a **table**. It lifts the deduplicated users from staging and exposes a **safe, commonly used subset** of attributes for analytics (leaving sensitive fields controllable downstream).

---

## Why it exists

- Provide a **canonical user dimension** with clean, typed columns for joins.
- **Scope PII exposure**: default to safe attributes commonly needed by BI.
- Preserve **ingestion lineage** (`src_ingest_ts`) for audits and debugging.

---

## How it works

### Model config
```jinja
{{ config(materialized='table') }}
```
Persists a physical table for frequent joins to facts.

### Selection from staging
```sql
select
  user_id,
  email,
  first_name,
  last_name,
  age,
  gender,
  city,
  state,
  country,
  traffic_source,
  created_at,
  src_ingest_ts
from {{ ref('stg_look__users') }}
```
- Source is **`stg_look__users`** (already deduped/typed and trimmed).
- Passes through standard analytic attributes; additional sensitive fields can be omitted or hashed in a separate dim if required.

---

## Output columns

- `user_id` — user key.
- `email`, `first_name`, `last_name` — identifiable contact name fields.
- `age`, `gender` — demographics.
- `city`, `state`, `country` — location.
- `traffic_source` — acquisition attribution.
- `created_at` — user creation time.
- `src_ingest_ts` — ingestion lineage from staging.

---

## Why it’s important

- **Consistent joins** from facts (orders, events, items) to a single user reference table.
- **Privacy-aware**: separates staging (full PII) from marts (curated subset) with the option to further mask where needed.
- **Operational clarity**: retains lineage for provenance questions.

# `macros/generate_schema_name.sql` — Purpose & How It Works

A tiny dbt **macro override** that controls **which schema** a model is built into. It mirrors dbt’s standard `generate_schema_name` behavior with an explicit, readable implementation: if a model doesn’t specify a custom schema, use the **target schema**; otherwise, use the **custom schema** (trimmed).

---

## Why it exists

- **Consistent schema routing** across environments (dev/prod) without editing SQL.
- **Clarity**: makes schema resolution explicit and easy to reason about in your project.
- **Compatibility**: keeps behavior aligned with dbt’s defaults while giving you a hook to customize later.

---

## The macro

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

**Behavior**
- When a model has no `+schema:` set → resolves to `target.schema` (from your active profile/target).
- When a model sets `+schema: 'some_schema'` → resolves to `'some_schema'` (after trimming whitespace).

---

## How it’s used in this project

In `dbt_project.yml`, folders specify `+schema:` (e.g., `silver_dev`, `gold_dev`). During `dbt run`, dbt calls this macro to compute the final schema per model:
- Staging models → `+schema: '{{ var("silver_schema", "silver_dev") }}'`
- Marts → `+schema: '{{ var("gold_schema", "gold_dev") }}'`

If those configs are omitted for a given model, the macro falls back to **`target.schema`**.

---

## Why it’s important

- **Environment portability**: switch schemas by changing `target` or `vars` without touching model SQL.
- **Predictability**: every model’s landing schema is determined by the same, tiny, auditable function.
- **Future-proofing**: you can extend this macro (e.g., prefix per developer, map by package) without refactoring models.

**Tips**
- To route schemas per developer, check `target.name` or environment variables inside this macro.
- To enforce naming conventions (e.g., lowercase), apply filters like `lower` to the result.

