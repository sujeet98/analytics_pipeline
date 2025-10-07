# Intermediate Layer

**Purpose:**  
Business-level transformations that require **joins/aggregations** but should remain reusable and composable.

**Key Models:**
- `int_order_items_enriched` *(incremental)*:  
  Enriches items with product attributes and resolves a **single** distribution center id (prefer inventory DC, else product DC). Appends new `order_item_id` only.
- `int_orders_aggregated_from_items` *(view)*:  
  Aggregates item facts to order grain and attaches the canonical `user_id` from `stg_look__orders`.

**Why a view for aggregations?**  
Keeps numbers current without managing another storage layer; upstream staging + items enrichment do the heavy lifting.
