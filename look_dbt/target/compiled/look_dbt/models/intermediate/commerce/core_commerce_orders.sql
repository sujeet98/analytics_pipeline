

  -- slightly larger cushion for late updates

with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

src_thelook as (
  select
    'look'        as source_system,
    order_id,
    user_id,
    order_status,
    order_date,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_orders__look
  
)

-- add more sources with the same filter and columns, then UNION ALL

select
  concat(source_system, ':', cast(order_id as string)) as global_order_id,
  source_system,
  order_id,
  user_id,
  order_status,
  order_date,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  num_of_item,
  canonical_updated_at,
  ingest_ts_utc
from src_thelook;