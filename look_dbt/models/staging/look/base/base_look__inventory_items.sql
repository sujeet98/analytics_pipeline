{# Base: 1 row per inventory_item id #}
with source as (
  select * from {{ source('look', 'inventory_items') }}
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
  product_id,
  created_at,
  sold_at,
  cost,
  product_category,
  product_name,
  product_brand,
  product_retail_price,
  product_department,
  product_sku,
  product_distribution_center_id,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
