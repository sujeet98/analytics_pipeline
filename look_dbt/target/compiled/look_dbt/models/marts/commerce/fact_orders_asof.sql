



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
  
),

cust_first as (
  select global_customer_id, min(valid_from) as first_valid_from
  from sujeet_data_analytics_workspace.gold_dev.dim_customer group by 1
),
cust_earliest as (
  select d.*
  from sujeet_data_analytics_workspace.gold_dev.dim_customer d
  join cust_first f
    on d.global_customer_id = f.global_customer_id
   and d.valid_from = f.first_valid_from
),

cust as (
  select
    b.*,
    coalesce(dc.customer_sk, dce.customer_sk) as customer_sk
  from base b
  left join sujeet_data_analytics_workspace.gold_dev.dim_customer dc
    on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and b.created_at >= dc.valid_from
   and b.created_at <  dc.valid_to
  left join cust_earliest dce
    on dce.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and dc.customer_sk is null
)

select
  order_id,
  customer_sk,
  order_status,
  order_date,
  created_at, shipped_at, delivered_at, returned_at,
  num_of_item
from cust;