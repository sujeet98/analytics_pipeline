



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

src_look as (
  select
    'look'              as source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,
    created_at,
    sold_at,
    created_date,
    unit_cost,
    retail_price,
    product_category,
    product_name,
    product_brand,
    product_department,
    product_sku,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items__look
  
),

unioned as (
  select * from src_look
  -- union all select * from other sources (same columns & filter)
)

select
  concat(source_system, ':', cast(inventory_item_id as string)) as global_inventory_item_id,
  source_system,
  inventory_item_id,
  product_id,
  distribution_center_id,
  created_at,
  sold_at,
  created_date,
  unit_cost,
  retail_price,
  product_category,
  product_name,
  product_brand,
  product_department,
  product_sku,
  canonical_updated_at,
  ingest_ts_utc
from unioned;