# PRD — TheLook Commerce Analytics (End‑to‑End)

> One‑pager + deep details to run this project like a production data product.

---

## 0) Executive Summary
**Business question / decision:** Provide reliable, timely e‑commerce metrics (orders, revenue, items, user behavior) and foundational dimensions (users, products, distribution centers) to power dashboards and ad‑hoc analysis.
**Expected impact:** Faster decision‑making on sales performance and product assortment; consistent definitions across teams; showcase enterprise‑grade portfolio project.
**Go/No‑Go:** Gold facts/dims built & tested; data < 60 minutes fresh; core tests 100% pass; runbook & alerts in place.

---

## 1) Stakeholders & Ownership
- **Sponsor:** (You / Portfolio reviewer)
- **Data Engineering:** You (ingestion, Auto Loader, Bronze)
- **Analytics Engineering:** You (dbt: Silver/Intermediate/Gold)
- **BI Owner:** You (sample dashboards / exposures)
- **RACI (condensed):**
  - Requirements/PRD (A: You; C: Reviewers)
  - Ingestion & Bronze (A/R: You)
  - Modeling & Tests (A/R: You)
  - Docs/Exposure (A/R: You)
  - Ops & Monitoring (A/R: You)

---

## 2) Scope
**In scope:** Ingest TheLook BigQuery public dataset; land RAW files; promote to Bronze (Delta); build Silver (staging), Intermediate, and Gold marts; add tests, freshness, and docs; deliver example dashboards/queries.  
**Out of scope:** Real‑time streaming; ML models; write‑back; PII obfuscation beyond best‑effort masking.  
**Dependencies:** Databricks workspace with Unity Catalog & External Volume; BigQuery access (ADC/OIDC or SA secret); dbt & adapters; Delta Optimization privileges.  
**Assumptions/constraints:** Events are mostly append‑only; orders/items may mutate; late arrivals common within a small window.

---

## 3) Business Requirements
- **User stories**
  - *As a product lead*, I need daily/near‑real‑time revenue & orders by product/brand/DC to adjust inventory and promotions.
  - *As an analyst*, I need clean, deduped events to analyze funnels (product → cart → purchase).
  - *As a data engineer*, I need resilient ingestion with stateful incrementals and backfills.
- **Success metrics**
  - Pipeline SLA: data freshness ≤ **60 minutes** (target 30m) end‑to‑end.
  - Test pass rate: **100% error**, ≥95% warn.
  - Rebuild time (gold facts): ≤ 10 minutes on modest cluster.
- **Backfill/latency:** First run backfills all history; ongoing runs incremental with a **grace window** to capture late data.

---

## 4) Metric Definitions & Semantics
**Subject area:** Commerce transactions + clickstream.  
**Grain of facts:** `order_item_id` (line item), `order_id` (order), `event_id` (web event).  
**Conformed dimensions:** Users, Products, Distribution Centers.

**Metric catalog (core):**
- **Item Count** — count of rows at item grain per order/date.  
  *Formula:* `count(*)` on `gold.order_items` grouped.  
- **Order Gross Revenue** — sum of item `sale_price` per order/date.  
  *Formula:* `sum(coalesce(sale_price,0))` on `gold.order_items`; exposed in `gold.orders`.  
- **Orders** — count of distinct `order_id`.  
  *Formula:* `count(distinct order_id)` on `gold.orders`.  
- **Events by Type** — counts of normalized `event_type`.  
  *Formula:* grouped count on `gold.events` (enum: product, cart, home, cancel, purchase, department, unknown).
- **Funnel Conversion** — event‑based conversion from product→cart→purchase per user/session/date.  
  *Definition notes:* purchase is derived from `events.event_type='purchase'` (not orders table) for funnel integrity; can be reconciled to orders for QA.

**Edge cases:** Negative prices disallowed; unknown statuses bucketed to `Unknown`; anonymous events allowed (`user_id` nullable).

---

## 5) Source Inventory & Contracts
**System:** BigQuery Public Dataset `bigquery-public-data.thelook_ecommerce`  
**Owner:** Google Cloud Public Datasets; read‑only.  
**Access:** Spark BigQuery connector with ADC/OIDC or base64 SA JSON secret.  
**Tables & patterns:**
- `orders`, `order_items`, `events`, `inventory_items`, `users`, `products`, `distribution_centers`
- **Change patterns:** Orders/Items can update (status, timestamps, price); Events append‑only; late arrivals up to **2–7 days** possible.
- **Incremental keys:** Timestamp columns per table (created/updated/shipped etc.); `src_ingest_ts` derived for uniformity.
- **Quality expectations:** Natural PK per table; timestamps present; enums may be messy (normalized in staging).  
**Compliance:** Contains basic PII (`users`); stored in Silver; Gold dims expose curated subset.

---

## 6) Architecture & Flow
**Flow:** BigQuery → **RAW** (Parquet in UC Volume) → **Bronze** (Delta via Auto Loader) → **Silver** (staging) → **Intermediate** (enrichment & rollups) → **Gold** (facts & dims) → BI.

**Storage:** Catalog `sujeet_data_analytics_workspace`; Schemas: `bronze_dev`, `silver_dev`, `gold_dev`; RAW Volume: `/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files`.

**Materializations policy:**
- **Staging (Silver):** **incremental MERGE** per model (dedupe/windowing; unique_key = natural key).
- **Intermediate:** **table**; heavy models (e.g., `int_order_items_enriched`) as **incremental MERGE**.
- **Gold facts:** **incremental MERGE** keyed by natural keys with watermark pruning.
- **Gold dims:** **table**, sourced from **snapshots** (SCD2 current rows).

**Orchestration:** 3‑task job: (1) BQ→RAW, (2) RAW→Bronze, (3) dbt snapshots/run/test/freshness. Schedule: every **30 minutes**; single concurrency; retries=2.

---

## 7) Modeling Contracts (selected highlights)
### Staging
- **Common:** derive `src_ingest_ts` (prefer `ingest_ts_utc`, else from partitions). Deduplicate to latest per key via window rank.
- **Keys:**  
  `orders(order_id)`, `order_items(order_item_id)`, `events(event_id)`, `users(user_id)`, `products(product_id)`, `inventory_items(inventory_item_id)`, `distribution_centers(distribution_center_id)`
- **Enums:** `status` normalized to `{Complete, Shipped, Returned, Cancelled, Processing, Unknown}`; `event_type` to `{product, cart, home, cancel, purchase, department, unknown}`.
- **Materialization:** incremental **MERGE**; predicate: last **7 days** on `src_ingest_ts` to keep MERGE fast.
- **Tests:** PK `unique` + `not_null`; enums `accepted_values`; FK relationships where applicable; ranges on numeric fields.

### Intermediate
- **int_order_items_enriched (item grain):** join items→inventory→products, resolve DC, attach DC attributes; **incremental MERGE** by `order_item_id`; test FKs to orders/users/products/DC; `sale_price >= 0`.
- **int_orders_aggregated_from_items (order grain):** aggregate enriched items; **table**; carries canonical `user_id` from orders.

### Gold
- **order_items (item fact):** **incremental MERGE** by `order_item_id`; ZORDER `(order_id, product_id)`.  
- **orders (order fact):** **incremental MERGE** by `order_id`; includes `item_count` and `order_gross_revenue` from intermediate; ZORDER `(created_at)`.  
- **events (event fact):** **incremental append** if immutable, else **MERGE** by `event_id`; ZORDER `(created_at, user_id, event_type)`.  
- **users_dim / products_dim / distribution_centers_dim (dims):** **table** sourced from **snapshots** (`users_scd`, `products_scd`) or staging latest for DCs.

---

## 8) Ingestion Plan
- **Method:** Spark BigQuery connector → RAW Parquet files in UC Volume; per‑table state JSON (or Delta table) storing cursor & last run.
- **Grace windows:** configurable per table (mins) to re‑read for late rows.
- **Auto Loader:** availableNow=true to Bronze; schema & checkpoints stored in Volume; rescuedDataColumn enabled for drift.
- **Backfill:** first run reads full history; subsequent runs incremental by change timestamps.
- **File sizing:** `WRITE_PARTS` tuned (default 16) to avoid small files.

---

## 9) Data Quality & Validation
- **Source:** `dbt source freshness` on Bronze with `loaded_at_field: src_ingest_ts`, warn 3h / error 6h.
- **Staging:** PK uniqueness, enums, ranges (money non‑negative), FK relationships (warn on early‑arriving where needed).
- **Gold:** integrity FKs to dims; metric non‑negativity; reconciliations (orders vs events purchases where relevant).
- **UAT:** Acceptance queries: daily revenue by DC, top products by revenue, funnel conversion; compare counts across layers to catch duplication/loss.

---

## 10) Security, Privacy, Governance
- **Access:** UC grants — read/write by role per schema (dev/prod split as needed).
- **PII:** Tag email/name fields; optional masking policy on `users_dim.email` for non‑PII roles.
- **Docs & lineage:** `persist_docs: true`; dbt docs site; exposures for BI assets.
- **Retention:** Delta `VACUUM` 7 days; RAW retained per bucket policy.

---

## 11) Observability & Ops
- **Run metrics:** rows read/written, max `src_ingest_ts`, lower/upper bounds, duration; logged to a Delta audit table.
- **Alerts:** job failure; freshness breach; dbt tests with `error` severity.
- **Maintenance:** weekly `OPTIMIZE ... ZORDER`; `VACUUM` 168h; monitor small files.
- **Cost:** autoOptimize properties on hot gold tables.

---

## 12) Delivery & Exposure
- **Semantic/BI:** Example dataset “Commerce Core”; dashboards: Sales Overview, Product Performance, Events Funnel.
- **Exposures:** dbt exposures linking dashboards to `gold.orders`, `gold.order_items`, `gold.events`, `gold.*_dim`.
- **SLA:** Data ready by :10 and :40 each hour (30‑min cadence).

---

## 13) Timeline & Milestones
- **Week 1:** Ingestion to RAW/Bronze; staging models & tests.  
- **Week 2:** Intermediate & Gold facts/dims; docs; OPTIMIZE/VACUUM; UAT.  
- **Week 3:** Snapshots enabled; exposures; alerting; polish and handoff.

---

## 14) Open Questions & Risks
- Events mutability? If corrected after load, ensure MERGE on events.  
- How large can late arrivals be? Adjust pruning window accordingly.  
- Future multi‑env promotion (dev→prod) and SLAs as data grows.

---

### Appendices

**dbt incremental MERGE header (template):**
```sql
{{ config(
  materialized='incremental',
  unique_key='<<PK>>',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  incremental_predicates=["src_ingest_ts >= (select coalesce(dateadd('day', -7, max(src_ingest_ts)), timestamp '1970-01-01') from {{ this }})"]
) }}
```

**Snapshot (SCD2) template:**
```sql
{% snapshot dim_scd %}
{{
  config(target_schema=var('silver_schema','silver_dev'), unique_key='<<PK>>', strategy='timestamp', updated_at='src_ingest_ts')
}}
select * from {{ ref('<<staging_model>>') }}
{% endsnapshot %}
```

**Delta maintenance:**
```sql
OPTIMIZE gold_dev.order_items ZORDER BY (order_id, product_id);
OPTIMIZE gold_dev.orders ZORDER BY (created_at);
OPTIMIZE gold_dev.events ZORDER BY (created_at, user_id, event_type);
VACUUM gold_dev.* RETAIN 168 HOURS;
```