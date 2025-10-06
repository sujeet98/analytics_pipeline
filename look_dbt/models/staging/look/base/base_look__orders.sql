{# Base: bring bronze orders to 1 row per order_id #}
with source as (
  select * from {{ source('look', 'orders') }}
),
dedup as (
  select
    *,
    row_number() over (
      partition by order_id
      order by ingest_ts_utc desc, to_timestamp_ntz(ingest_date) desc nulls last
    ) as _rn
  from source
)
select
  order_id,
  user_id,
  status,
  gender,
  created_at,
  returned_at,
  shipped_at,
  delivered_at,
  num_of_item,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
