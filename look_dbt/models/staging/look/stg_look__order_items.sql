{{ config(materialized='view') }}

-- Purpose: Stage order_items; normalize statuses and amounts. No joins.
-- Grain: 1 row per order_item_id (latest by ingest_ts_utc)

with source as (
  select * from {{ source('look','order_items') }}
), renamed as (
  select
    cast(id as bigint)                as order_item_id,
    cast(order_id as bigint)          as order_id,
    cast(user_id as bigint)           as user_id,
    cast(product_id as bigint)        as product_id,
    cast(inventory_item_id as bigint) as inventory_item_id,
    {{ clean_lower('status') }}       as status_raw,
    {{ clean_ts('created_at') }}      as created_at,
    {{ clean_ts('shipped_at') }}      as shipped_at,
    {{ clean_ts('delivered_at') }}    as delivered_at,
    {{ clean_ts('returned_at') }}     as returned_at,
    {{ as_money_2('sale_price') }}    as sale_price,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select
  order_item_id, order_id, user_id, product_id, inventory_item_id,
  {{ normalize_status('status_raw') }} as status,
  created_at, shipped_at, delivered_at, returned_at,
  sale_price                                          as item_revenue,
  {{ to_date_utc('created_at') }}                     as created_date,
  {{ to_date_utc('delivered_at') }}                   as delivered_date,
  {{ to_date_utc('returned_at') }}                    as returned_date
from renamed
where _rn = 1
