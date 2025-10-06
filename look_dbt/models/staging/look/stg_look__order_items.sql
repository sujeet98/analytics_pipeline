{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__order_items') }})
select
  id        as order_item_id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  {{ normalize_order_status('status') }} as status,
  {{ parse_ts('created_at') }}   as created_at,
  {{ parse_ts('shipped_at') }}   as shipped_at,
  {{ parse_ts('delivered_at') }} as delivered_at,
  {{ parse_ts('returned_at') }}  as returned_at,
  {{ money_2('sale_price') }}    as sale_price,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
