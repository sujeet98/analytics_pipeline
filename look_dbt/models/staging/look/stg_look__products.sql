{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__products') }})
select
  id as product_id,
  {{ money_2('cost') }}           as cost,
  {{ clean_string('category') }}  as category,
  {{ clean_string('name') }}      as name,
  {{ clean_string('brand') }}     as brand,
  {{ money_2('retail_price') }}   as retail_price,
  {{ clean_string('department') }} as department,
  {{ clean_string('sku') }}        as sku,
  distribution_center_id,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
