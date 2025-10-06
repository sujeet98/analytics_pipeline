{# Base: 1 row per event id #}
with source as (
  select * from {{ source('look', 'events') }}
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
  user_id,
  sequence_number,
  session_id,
  created_at,
  ip_address,
  city,
  state,
  postal_code,
  browser,
  traffic_source,
  uri,
  event_type,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from dedup
where _rn = 1
