

with base as (
  select
    o.order_id,
    o.user_id,
    o.status,
    o.created_at,
    coalesce(a.item_count, 0)            as item_count,
    coalesce(a.items_gross_revenue, 0.0) as order_gross_revenue,
    o.src_ingest_ts
  from sujeet_data_analytics_workspace.silver_dev.stg_look__orders o
  left join sujeet_data_analytics_workspace.silver_dev.int_orders_aggregated_from_items a
    on o.order_id = a.order_id
),
src as (
  select *
  from base
  
)

select * from src