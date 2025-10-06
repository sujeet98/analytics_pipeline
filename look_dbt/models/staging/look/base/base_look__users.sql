{# Base: 1 row per user id #}
with source as (
  select * from {{ source('look', 'users') }}
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
  first_name,
  last_name,
  email,
  age,
  gender,
  state,
  street_address,
  postal_code,
  city,
  country,
  latitude,
  longitude,
  traffic_source,
  created_at,
  user_geom,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
