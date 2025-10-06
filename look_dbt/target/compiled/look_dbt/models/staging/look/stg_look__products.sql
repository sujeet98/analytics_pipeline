

with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__products)
select
  id as product_id,
  cast(cost as decimal(18, 2))           as cost,
  nullif(trim(category), '')  as category,
  nullif(trim(name), '')      as name,
  nullif(trim(brand), '')     as brand,
  cast(retail_price as decimal(18, 2))   as retail_price,
  nullif(trim(department), '') as department,
  nullif(trim(sku), '')        as sku,
  distribution_center_id,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b