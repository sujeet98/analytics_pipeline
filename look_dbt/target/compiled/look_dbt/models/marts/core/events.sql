-- Event fact at event_id grain.
-- Table materialization is fine (modest volume; simple shape).



select
  event_id,
  user_id,
  event_type,
  created_at,
  browser,
  traffic_source,
  uri,
  city, state, postal_code, ip_address,
  src_ingest_ts
from sujeet_data_analytics_workspace.silver_dev.stg_look__events;