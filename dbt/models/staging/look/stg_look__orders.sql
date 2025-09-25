{{ config(materialized='view') }}

-- Purpose: Stage orders as atoms for downstream modeling. No joins.
-- Grain: 1 row per order_id (latest by ingest_ts_utc)

with source as (
  select * from {{ source('look','orders') }}
), renamed as (
  select
    cast(order_id as bigint)       as order_id,
    cast(user_id as bigint)        as user_id,
    {{ clean_lower('status') }}    as status_raw,
    {{ clean_lower('gender') }}    as gender,
    {{ clean_ts('created_at') }}   as created_at,
    {{ clean_ts('returned_at') }}  as returned_at,
    {{ clean_ts('shipped_at') }}   as shipped_at,
    {{ clean_ts('delivered_at') }} as delivered_at,
    cast(coalesce(num_of_item, 0) as int) as num_of_item,
    ingest_ts_utc,
    row_number() over (partition by order_id order by ingest_ts_utc desc) as _rn
  from source
)
select
  order_id,
  user_id,
  {{ normalize_status('status_raw') }} as status,
  gender,
  created_at, returned_at, shipped_at, delivered_at,
  num_of_item,
  {{ to_date_utc('created_at') }} as created_date
from renamed
where _rn = 1
