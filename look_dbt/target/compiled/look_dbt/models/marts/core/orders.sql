

-- Orders fact (one row per order) with item_count and order_gross_revenue.
-- Incremental pruning uses the order's src_ingest_ts (latest deduped ingestion for that order).

with orders as (
  select order_id, user_id, status, created_at, src_ingest_ts
  from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
),
items_agg as (
  select * from sujeet_data_analytics_workspace.silver_dev.int_orders_aggregated_from_items
),
final as (
  select
    o.order_id,
    o.user_id,
    o.status,
    o.created_at,
    coalesce(a.item_count, 0)            as item_count,
    coalesce(a.items_gross_revenue, 0.0) as order_gross_revenue,
    o.src_ingest_ts
  from orders o
  left join items_agg a on o.order_id = a.order_id
)

select * from final
