# Marts (Core)

**Purpose:**  
Curated **facts and dimensions** ready for BI and downstream consumption.

**Facts:**
- `orders` *(incremental)*: one row per order; includes `item_count` and `order_gross_revenue`.
- `order_items` *(incremental)*: wide item fact with product + distribution center context.
- `events` *(table)*: event grain.

**Dimensions (tables):**
- `products_dim`, `users_dim`, `distribution_centers_dim`.

**Incremental Strategy Recap:**
- **Orders / Order Items:** prune by `src_ingest_ts` (ensures efficient appends while staying audit-friendly).
