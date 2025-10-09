



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
src as (
  select
    id as order_item_id,
    order_id, user_id, product_id, inventory_item_id,
    item_status,
    cast(created_at as timestamp)   as created_at,
    cast(shipped_at as timestamp)   as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(returned_at as timestamp)  as returned_at,
    sale_price,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max))
)
select
  order_item_id, order_id, user_id, product_id, inventory_item_id,
  item_status,
  created_at, shipped_at, delivered_at, returned_at,
  date(coalesce(delivered_at, created_at)) as item_date,
  sale_price,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;