{{ config(materialized='view') }}

-- Purpose: Stage events for behavioral analysis. No joins.
-- Grain: 1 row per event_id (latest by ingest_ts_utc)

with source as (
  select * from {{ source('look','events') }}
), renamed as (
  select
    cast(id as bigint)           as event_id,
    cast(user_id as bigint)      as user_id,
    cast(sequence_number as int) as sequence_number,
    session_id,
    {{ clean_ts('created_at') }} as created_at,
    ip_address, city, state, postal_code,
    {{ clean_lower('browser') }} as browser,
    {{ clean_lower('traffic_source') }} as traffic_source,
    uri,
    {{ clean_lower('event_type') }} as event_type,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select
  event_id, user_id, sequence_number, session_id, created_at,
  ip_address, city, state, postal_code, browser, traffic_source, uri, event_type,
  {{ to_date_utc('created_at') }} as event_date
from renamed
where _rn = 1
