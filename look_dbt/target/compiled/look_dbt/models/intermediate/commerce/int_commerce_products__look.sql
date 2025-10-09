



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
src as (
  select
    product_id,
    product_name, brand, category, department, sku,
    unit_cost, retail_price,
    distribution_center_id,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__products
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max))
)
select
  product_id,
  product_name, brand, category, department, sku,
  unit_cost, retail_price,
  distribution_center_id,
  date(ingest_ts_utc) as product_snap_date,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;