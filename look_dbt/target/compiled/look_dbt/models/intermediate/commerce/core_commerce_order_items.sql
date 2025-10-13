



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

-- ===== Per-source aligned inputs (add more sources as needed) =====
src_thelook as (
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
    item_date,                       -- already chosen in int layer
    cast(sale_price as numeric) as sale_price,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
),

-- src_other here

unioned as (
  select * from src_thelook
  -- union all select * from src_other
)

select
  concat(source_system, ':', cast(order_item_id as string)) as global_order_item_id, -- global BK
  source_system,
  order_item_id,       -- source BKs (kept for audit/drill)
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
  sale_price,
  canonical_updated_at,
  ingest_ts_utc
from unioned;