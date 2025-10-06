{# Base: 1 row per product id #}
with source as (
  select * from {{ source('look', 'products') }}
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
  cost,
  category,
  name,
  brand,
  retail_price,
  department,
  sku,
  distribution_center_id,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
