{# Base: 1 row per distribution_center id #}
with source as (
  select * from {{ source('look', 'distribution_centers') }}
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
  name,
  latitude,
  longitude,
  distribution_center_geom,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
