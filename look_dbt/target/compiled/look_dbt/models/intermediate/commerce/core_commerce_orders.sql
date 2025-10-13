



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

-- ===== Per-source aligned inputs (add more sources as needed) =====
src_thelook as (
  select
    'look'        as source_system,
    order_id,
    user_id,
    order_status,
    order_date,                 -- already produced in int layer
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_orders__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
),

-- src_other here

unioned as (
  select * from src_thelook
  -- union all select * from src_other
)

select
  concat(source_system, ':', cast(order_id as string)) as global_order_id,  -- global BK
  source_system,
  order_id,                 -- source BKs (kept for audit/drill)
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
from unioned;