



with src as (
  select
    event_id,
    user_id, sequence_number, session_id,
    event_ts, event_date,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri, event_type,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__events
  
)

select
  event_id, user_id, sequence_number, session_id,
  event_ts, event_date,
  ip_address, city, state, postal_code,
  browser, traffic_source, uri, event_type,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;