



with tgt_max as (
  select
    
      timestamp('1900-01-01')
     as max_ts
  from  (select 1) _ 
),

-- ===== Add per-source aligned inputs (one CTE per source) =====
src_look as (
  select
    'look'            as source_system,
    order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    item_status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    item_date,                -- analytic convenience
    created_date,             -- partition key (stable)
    cast(sale_price as numeric) as sale_price,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items__look
  
),

unioned as (
  select * from src_look
  -- union all select * from src_<others> with the same filter
)

select
  concat(source_system, ':', cast(order_item_id as string)) as global_order_item_id,
  source_system,
  order_item_id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  item_status,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  item_date,
  created_date,
  sale_price,
  canonical_updated_at,
  ingest_ts_utc
from unioned;