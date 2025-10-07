# TheLook End-to-End Analytics Pipeline (Databricks + dbt) — README

> Portfolio-ready, production-style project that ingests a public dataset from **BigQuery**, lands immutable **RAW** files to an **External Volume** on S3, promotes to **Bronze** Delta tables with **Auto Loader**, then builds **Silver (base + staging) → Intermediate → Gold (marts)** with **dbt**.  
> Emphasis on **incremental processing**, **data quality**, **clear lineage**, and **operational playbooks**.

---

## Table of contents

1) [Architecture overview](#architecture-overview)  
2) [Data model & grain](#data-model--grain)  
3) [Ingestion jobs](#ingestion-jobs)  
4) [Lakehouse layers](#lakehouse-layers)  
5) [Incremental strategy](#incremental-strategy)  
6) [Project layout](#project-layout)  
7) [dbt: how to run](#dbt-how-to-run)  
8) [Data quality & tests](#data-quality--tests)  
9) [Documentation & lineage](#documentation--lineage)  
10) [Environments & configuration](#environments--configuration)  
11) [Monitoring & operations](#monitoring--operations)  
12) [Cost & performance notes](#cost--performance-notes)  
13) [Security & governance](#security--governance)  
14) [Troubleshooting](#troubleshooting)  
15) [Extending the project](#extending-the-project)  

---

## Architecture overview

```
BigQuery (public) ──► Databricks PySpark job (Storage API)
                      01_ingest_bigquery_to_raw_volume.py
                   └─► RAW files (Parquet) on S3 via UC External Volume
                          /Volumes/.../raw_thelook_files/<table>/
                            ingest_date=YYYY-MM-DD/run_ts=HHMMSS/part-*.parquet
                                        │
                                        ▼
Databricks Auto Loader (cloudFiles) ──► BRONZE (Delta, UC)
  raw_to_bronze_autoloader.py            sujeet_data_analytics_workspace.bronze_dev
                                        │
                                        ▼
dbt (Databricks adapter) ─────────────► SILVER
  base_*  (dedupe only)                  silver_dev.base_look__*
  stg_*   (typing/normalization)         silver_dev.stg_look__*
                                        │
                                        ▼
INTERMEDIATE (dbt) ────────────────────► Enriched & Aggregations
  int_order_items_enriched (incremental)
  int_orders_aggregated_from_items (view)
                                        │
                                        ▼
GOLD MARTS (dbt) ──────────────────────► Core facts & dims (tables/incremental)
  orders (incremental), order_items (incremental),
  events (table), users_dim/products_dim/distribution_centers_dim (tables)
```

**Why this design**

- **RAW** stays immutable & append-only → easy to reason about and replay.
- **Bronze** is the first Delta representation with Auto Loader checkpoints for exactly-once semantics.
- **Silver/Base** only dedupes; **Silver/Staging** applies typing/normalization. Clear separation avoids “hidden” transformations.
- **Intermediate** contains reusable business logic (wide enrichment, aggregations).
- **Gold** exposes well-named facts/dims for BI with strong tests.

---

## Data model & grain

- **orders** (gold): one row per `order_id`. Enriched with item_count and `order_gross_revenue` from items aggregation.  
- **order_items** (gold): one row per `order_item_id`. Enriched with product attributes and resolved distribution center.  
- **events** (gold): one row per `event_id`.  
- **users_dim / products_dim / distribution_centers_dim**: conformed dimensions for analytics.

> All facts/dims carry `src_ingest_ts` (lineage) propagated from upstream so incrementals can scan only new/changed data.

---

## Ingestion jobs

### 1) BigQuery → RAW Volume

**Script:** `01_ingest_bigquery_to_raw_volume.py`

- Reads directly from `bigquery-public-data.thelook_ecommerce.<table>` using the **BigQuery Storage API** through Spark connector.
- Incremental by **high-watermark cursor** across change columns (`created_at`, `shipped_at`, `delivered_at`, `returned_at`, etc.) with a **grace window** for late data.
- Writes **append-only Parquet** to a UC **External Volume** (`/Volumes/.../raw_thelook_files`) partitioned as:

```
/<table>/ingest_date=YYYY-MM-DD/run_ts=HHMMSS/part-*.snappy.parquet
```

- Keeps per-table cursor JSON at `/Volumes/.../raw_thelook_files/_state/<table>.json`.

**Schedule:** every 10–30 minutes (single concurrency).  
**First run:** backfills (with `INCLUDE_NULLS_ON_FIRST_RUN=True` to catch null timestamps).

> Rationale: using a **Volume** avoids managing IAM roles from serverless and provides a single abstraction for partners to drop files while you read/write via UC.

### 2) RAW Volume → BRONZE (Delta)

**Script:** `raw_to_bronze_autoloader.py`

- Uses **Auto Loader** (`cloudFiles`) to incrementally discover and ingest RAW parquet into **Delta tables** in `bronze_dev`.
- Checkpoints + schema tracking live within the same Volume under `_autoloader/`.
- First run uses `includeExistingFiles=true` for backfill; later runs only pick up new partitions.
- Recasts system columns (`ingest_date`, `run_ts`, `ingest_ts_utc`, `source_table`) defensively to stable types.

**Run mode:** job per table (sequential) with `trigger(availableNow=True)` to process the batch then stop.

---

## Lakehouse layers

### Silver: Base (dedupe only)

- **Goal:** remove duplicates caused by overlapping RAW partitions; do **not** change business semantics.
- **How:** windowed `row_number()` per business key (e.g., `order_id`) ordered by derived `src_ingest_ts` (from `ingest_ts_utc` or `ingest_date`+`run_ts`).
- **Output:** `base_look__*` models (views) with a unified `src_ingest_ts`.

### Silver: Staging (typing + normalization)

- **Goal:** present cleaned, typed data to downstream with stable names.
- **How:** casts, trims, controlled vocabularies (status/event_type), selection/rename.
- **Output:** `stg_look__*` (views). All staging rows retain `src_ingest_ts`.

### Intermediate

- `int_order_items_enriched` (**incremental** table): joins items → inventory → product, resolves distribution center (prefer inv DC; fallback product DC), brings product attrs, and keeps `src_ingest_ts` from items.  
- `int_orders_aggregated_from_items` (**view**): aggregates enriched items at order grain and attaches canonical `user_id` from orders.

### Gold (Marts)

- **orders** (**incremental** table): joins `stg_look__orders` with aggregated items; incremental predicate `src_ingest_ts > max(src_ingest_ts)` in target.
- **order_items** (**incremental** table): selects from `int_order_items_enriched` with the same incremental predicate.
- **events, users_dim, products_dim, distribution_centers_dim**: **tables** (small/modest; stable schemas) rebuilt on each run.

---

## Incremental strategy

**Key design**: use a single lineage timestamp `src_ingest_ts` all the way from **Bronze → Base → Staging → Intermediate → Gold**.

- In **Base**, compute `src_ingest_ts = coalesce(ingest_ts_utc, to_timestamp(ingest_date||' '||run_ts))`.
- In **Staging**, carry `src_ingest_ts` unchanged.
- In **Intermediate**, for enriched items, keep the `oi.src_ingest_ts` (the most volatile upstream).
- In **Gold** incrementals, load rows only where `src_ingest_ts > max(src_ingest_ts)` in the target.

This yields efficient **append-only** behavior appropriate for the upstream ingestion pattern.  
If later you need **upserts** (true SCD / late corrections), switch to Databricks `incremental_strategy='merge'` with a predicate on both key and an `updated_at` column.

---

## Project layout

```
look_dbt/
├─ models/
│  ├─ staging/
│  │  └─ look/
│  │     ├─ base/
│  │     │  ├─ base_look__orders.sql
│  │     │  ├─ base_look__order_items.sql
│  │     │  ├─ base_look__events.sql
│  │     │  ├─ base_look__users.sql
│  │     │  ├─ base_look__products.sql
│  │     │  └─ base_look__distribution_centers.sql
│  │     ├─ stg_look__orders.sql
│  │     ├─ stg_look__order_items.sql
│  │     ├─ stg_look__events.sql
│  │     ├─ stg_look__users.sql
│  │     ├─ stg_look__products.sql
│  │     ├─ stg_look__inventory_items.sql
│  │     ├─ stg_look__distribution_centers.sql
│  │     ├─ _look__sources.yml
│  │     └─ _look__models.yml
│  ├─ intermediate/
│  │  └─ commerce/
│  │     ├─ int_order_items_enriched.sql
│  │     ├─ int_orders_aggregated_from_items.sql
│  │     └─ _commerce__models.yml
│  └─ marts/
│     └─ core/
│        ├─ orders.sql
│        ├─ order_items.sql
│        ├─ events.sql
│        ├─ users_dim.sql
│        ├─ products_dim.sql
│        ├─ distribution_centers_dim.sql
│        └─ _core__models.yml
├─ macros/
│  └─ (only small helpers if desired)
├─ ingestion/
│  ├─ 01_ingest_bigquery_to_raw_volume.py
│  └─ raw_to_bronze_autoloader.py
├─ dbt_project.yml
└─ README.md
```

---

## dbt: how to run

### 0) Profiles & adapter
- Use `dbt-databricks` adapter.  
- Example `profiles.yml` (local dev with a SQL Warehouse):

```yaml
look_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: sujeet_data_analytics_workspace
      schema: silver_dev
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <personal-access-token>
      threads: 8
```

> Gold and Bronze schemas are referenced fully-qualified in SQL where needed; the dbt target schema (`silver_dev`) contains the **views/tables** for base, staging, and intermediate unless overridden.

### 1) Build everything needed for gold
```bash
dbt build --selector marts_core_with_ancestors
```

If you don’t have selectors, you can run topologically:
```bash
dbt run -s staging/look
dbt run -s intermediate/commerce
dbt build -s marts/core
```

### 2) Just tests
```bash
dbt test
```

### 3) Docs
```bash
dbt docs generate && dbt docs serve
```

---

## Data quality & tests

- **Base:** `not_null`+`unique` on business keys and `src_ingest_ts` presence (ensures dedupe worked and lineage exists).
- **Staging:** type/semantic tests — accepted values for status/event_type, relationships for FKs (to staging models), and simple numeric constraints (`sale_price >= 0`).
- **Intermediate:** relationships on joins (orders/products/users/DC), uniqueness on `order_item_id`, and non-negative sums.
- **Gold:** key uniqueness, relationship tests to staging FKs, and business constraints (`order_gross_revenue >= 0`).

> Tests are expressed in YAML with the **`arguments:`** nesting to avoid deprecation warnings.

---

## Documentation & lineage

- The YAML files include **descriptions** for every model and most columns.  
- `exposures` in `_core__models.yml` declare the **Executive Revenue Dashboard**, which ties BI assets back to the warehouse lineage.

Run:
```bash
dbt docs generate && dbt docs serve
```
Then explore **lineage graph** for end-to-end traceability: BigQuery → RAW → Bronze → Silver → Intermediate → Gold → Exposure.

---

## Environments & configuration

- **Unity Catalog** Catalog: `sujeet_data_analytics_workspace`
- **Schemas**
  - `bronze_dev`: landing Delta tables via Auto Loader
  - `silver_dev`: dbt target (base + staging + intermediate)
  - `gold_dev`: dbt core marts (configured in SQL files)
- **External Volume**: `/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files` pointing to `s3://analyticsbucketdev-sk/raw`

> For prod, mirror this layout with `bronze_prod`, `silver_prod`, `gold_prod`, and separate Volumes/buckets or prefixes. Use dbt **targets** to switch.

---

## Monitoring & operations

- **Elementary** (installed) can surface freshness, test failures, and anomalies.  
  Initialize models once (as the dbt logs suggested):
  ```bash
  dbt run -s elementary --target dev
  ```
- **Job scheduling**
  - **Ingest BigQuery → RAW**: every 10–30 min, low concurrency (1).
  - **RAW → Bronze Auto Loader**: run after the RAW job (or every 10–30 min).
  - **dbt**: every 30–60 min depending on freshness SLAs.

- **Health signals to watch**
  - Growth in `_rescued_data` column in Bronze (unexpected schema drift).
  - Elevated duplicates detected in Base (windowing issue upstream).
  - Spikes in `Unknown` status/event_type (new enums in source).
  - Elementary dashboard alerts.

---

## Cost & performance notes

- **RAW writer** coalesces to `WRITE_PARTS` (default 16) to avoid many tiny files. Adjust per table size.
- **Auto Loader** uses `availableNow` for batch semantics; very economical for periodic runs.
- **dbt**:
  - Base & Staging are **views** (cheap). If queries become heavy, flip a few hot staging models to **tables**.
  - Intermediate enrichment is **incremental** (append-only) to limit scans.
  - Gold `orders` & `order_items` are **incremental** on `src_ingest_ts` to minimize work.

---

## Security & governance

- Use UC **External Volumes** for controlled access to S3.
- Grant **READ FILES / WRITE FILES** on the Volume to job principals; **CREATE/MODIFY** on schemas for writers.
- Apply **table ACLs** on Gold for consumer groups.  
- Consider masking/hashing email at staging if required (macro or explicit SHA2 in SQL).

---

## Troubleshooting

- **LibreSSL / urllib3 warnings** (local macOS venv): they’re noisy but harmless for Databricks-executed code. Prefer running dbt in a container or managed CI if needed.
- **Test deprecations**: all tests now use `arguments:` nesting — if you add new ones, follow the same pattern.
- **Missing intermediate tables**: ensure `int_orders_aggregated_from_items` stays a **view** and **comes after** `int_order_items_enriched` in DAG (dbt handles this via `ref()`).
- **FK test failures from small mismatches**: some events can be anonymous; relationship tests allow nulls where modeled.

---

## Extending the project

### Add a new source table
1. Ensure it’s added to the RAW ingestion **TABLES** map in `01_ingest_bigquery_to_raw_volume.py` (with change columns).
2. Auto Loader will pick it up once you add it to its **TABLES** list.
3. Create `base_look__<table>.sql` (dedupe) and `stg_look__<table>.sql` (typing/normalization).
4. Add source + model entries in `_look__sources.yml` / `_look__models.yml`.
5. Build intermediate/marts as needed and add tests.

### Switch a gold fact to MERGE upserts
- Add to model config:
  ```sql
  {{ config(
       materialized='incremental',
       unique_key='order_id',
       incremental_strategy='merge',
       merge_update_columns = ['...'],
  ) }}
  ```
- Provide an `updated_at` (or reuse `src_ingest_ts` if that reflects true changes).

---

## Why this is portfolio-ready

- Clear **separation of concerns** (dedupe vs. typing vs. enrichment).
- Robust **incremental design** with a single lineage clock (`src_ingest_ts`).
- Clean, well-commented SQL that is **easy to audit**.
- **Data quality** codified in YAML tests at each layer.
- **Operational realism**: cursor files, grace windows, Auto Loader checkpoints, UC volumes.
- **Documentation & lineage** via dbt docs and exposures.

---

### Quickstart commands

1) Run ingestion (Databricks Jobs):
   - `01_ingest_bigquery_to_raw_volume.py`
   - `raw_to_bronze_autoloader.py`

2) Build warehouse:
```bash
dbt build --selector marts_core_with_ancestors
```

3) Open docs:
```bash
dbt docs generate && dbt docs serve
```
