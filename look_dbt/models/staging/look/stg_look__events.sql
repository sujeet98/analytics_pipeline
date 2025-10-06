{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__events') }})
select
  id as event_id,
  user_id,
  sequence_number,
  session_id,
  {{ parse_ts('created_at') }} as created_at,
  {{ clean_string('ip_address') }} as ip_address,
  {{ clean_string('city') }}       as city,
  {{ clean_string('state') }}      as state,
  {{ clean_string('postal_code') }} as postal_code,
  {{ clean_string('browser') }}     as browser,
  {{ clean_string('traffic_source') }} as traffic_source,
  {{ clean_string('uri') }}            as uri,
  {{ normalize_event_type('event_type') }} as event_type,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
