



with src_thelook as (
  select
    'look' as source_system,
    event_id,
    user_id,
    session_id,
    sequence_number,
    event_ts,
    event_date,
    ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    uri,
    event_type,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_events__look
  
),

unioned as (
  select * from src_thelook
  -- union all select * from other sources with the same columns & filter
)

select
  concat(source_system, ':', cast(event_id as string)) as global_event_id,
  source_system,
  event_id,
  user_id,
  session_id,
  sequence_number,
  event_ts,
  event_date,
  ip_address,
  city,
  state,
  postal_code,
  browser,
  traffic_source,
  uri,
  event_type,
  canonical_updated_at,
  ingest_ts_utc
from unioned;