# The Look – Staging Models (silver_dev)

This folder contains **source-aligned, incremental, deduplicated staging models** for the *TheLook* dataset that lands in `bronze_dev` via Databricks Autoloader. These models publish to the **`silver_dev`** schema and are the clean inputs for the **intermediate (conformed)** and **marts** layers.

> TL;DR: We upsert the latest version of each record (by natural key) using a **2‑day lookback** on `ingest_ts_utc`, normalize key categorical fields, and keep the results partition‑pruned for fast downstream queries.

---

## Models in this folder

| Model | Natural key | Purpose |
|---|---|---|
| `stg_look__orders.sql` | `order_id` | Clean & dedup orders; normalize `order_status` |
| `stg_look__order_items.sql` | `id` | Clean & dedup order line items; normalize `item_status` |
| `stg_look__events.sql` | `id` | Clean & dedup events; normalize `event_type`, `browser`, `traffic_source`; add `event_date` |
| `stg_look__inventory_items.sql` | `id` | Clean & dedup inventory units; retain product attributes as-landed |
| `stg_look__users.sql` | `id` | Clean & dedup users; normalize `gender`, `traffic_source` |
| `stg_look__products.sql` | `product_id` | Clean & dedup products; harmonize naming & types |
| `stg_look__distribution_centers.sql` | `distribution_center_id` | Clean & dedup DCs |

All staging models share these conventions:
- **Materialization:** `incremental` with `incremental_strategy='merge'`
- **Watermark:** reprocess rows where `ingest_ts_utc >= dateadd(day, -2, max(ingest_ts_utc in target))` (configurable)
- **Dedup:** window function keeps the **latest** row per natural key by `ingest_ts_utc`
- **Partition pruning:** `partition_by` a date column (`_ingest_date` or `event_date` for events)
- **Normalization:** controlled vocabularies for categorical fields

## Incremental & dedup strategy (why this is fast/safe)

- **Why incremental MERGE?** Bronze may replay overlapping partitions; late files arrive. We only scan **recent** data, dedup **within** that slice, and **upsert** by the natural key.  
- **Why a lookback (2 days)?** Strict `> max(ingest_ts_utc)` can miss slightly older late-arrivals. A short lookback is both robust and cost‑efficient.  
- **Why partitioning?** To prune scans by date—even within the lookback window.

> Tune the lookback in each model (2–3 days typical). If your source SLA is noisier, widen it.

---

## Data quality & freshness

- **Tests:** `staging.yml` defines `not_null`, `unique`, and `accepted_values` checks; money fields (`sale_price`, `unit_cost`, `retail_price`) use non‑negative assertions.  
- **Source Freshness:** `models/sources.yml` sets `loaded_at_field: ingest_ts_utc` with `warn_after/error_after`. Run:
  ```bash
  dbt source freshness --select source:look
  ```

---

## How to run locally

```bash
# Install packages
dbt deps

# Build only the look staging models
dbt run --select path:models/staging/look

# Or build with tags
dbt run --select tag:staging,look

# Run staging tests
dbt test --select path:models/staging/look
```

> On Databricks, these models target the **`silver_dev`** schema (see `dbt_project.yml`).