# Staging Layer (look)

**Purpose:**  
Single place to **dedupe**, **type/normalize**, and attach a unified ingestion lineage timestamp (`src_ingest_ts`) to each entity.

**Patterns Used:**
- Build `src_ingest_ts` from bronze metadata:  
  `coalesce(ingest_ts_utc, to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss'))`
- Dedupe: `row_number() over (partition by <key> order by src_ingest_ts desc)` → keep `rn = 1`.
- Normalize enums (e.g., order status, event type) to a **controlled vocabulary**.
- Keep columns named consistently across the project.

**Outputs:**  
- `stg_look__orders`, `stg_look__order_items`, `stg_look__events`, `stg_look__users`, `stg_look__products`, `stg_look__inventory_items`, `stg_look__distribution_centers`.
