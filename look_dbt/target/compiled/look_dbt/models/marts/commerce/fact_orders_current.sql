



with base as (
  select
    source_system,
    order_id,
    user_id,
    order_status,
    order_date,
    created_at, shipped_at, delivered_at, returned_at,
    num_of_item
  from sujeet_data_analytics_workspace.silver_dev.core_commerce_orders
  
)

select
  b.order_id,
  dc.customer_sk,
  b.order_status,
  b.order_date,
  b.created_at, b.shipped_at, b.delivered_at, b.returned_at,
  b.num_of_item
from base b
left join sujeet_data_analytics_workspace.gold_dev.dim_customer_current dc
  on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string));