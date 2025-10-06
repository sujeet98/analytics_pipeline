{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__inventory_items') }})
select
  id as inventory_item_id,
  product_id,
  {{ parse_ts('created_at') }} as created_at,
  {{ parse_ts('sold_at') }}    as sold_at,
  {{ money_2('cost') }}        as cost,
  {{ clean_string('product_category') }}       as product_category,
  {{ clean_string('product_name') }}           as product_name,
  {{ clean_string('product_brand') }}          as product_brand,
  {{ money_2('product_retail_price') }}        as product_retail_price,
  {{ clean_string('product_department') }}     as product_department,
  {{ clean_string('product_sku') }}            as product_sku,
  product_distribution_center_id,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
