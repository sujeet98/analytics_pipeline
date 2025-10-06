

with orders as (
  select
    order_id,
    user_id,
    status,
    created_at
  from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
),
items as (
  select
    order_id,
    count(*) as item_count,
    sum(sale_price) as order_gross_revenue
  from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
  group by 1
)

select
  o.order_id,
  o.user_id,
  o.created_at,
  o.status,
  coalesce(i.item_count, 0) as item_count,
  coalesce(i.order_gross_revenue, 0.0) as order_gross_revenue
from orders o
left join items i using (order_id)

