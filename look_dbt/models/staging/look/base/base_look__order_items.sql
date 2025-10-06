{# Base: 1 row per order item id #}
with source as (
  select * from {{ source('look', 'order_items') }}
),
dedup as (
  select
    *,
    row_number() over (
      partition by id
      order by ingest_ts_utc desc, to_timestamp_ntz(ingest_date) desc nulls last
    ) as _rn
  from source
)
select
  id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  status,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  sale_price,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
