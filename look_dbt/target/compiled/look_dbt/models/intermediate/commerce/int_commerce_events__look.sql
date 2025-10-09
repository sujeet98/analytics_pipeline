



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
src as (
  select
    id as event_id,
    user_id, sequence_number, session_id,
    event_ts, event_date,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri, event_type,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__events
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max))
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