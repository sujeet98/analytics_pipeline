

select
  event_id,
  user_id,
  session_id,
  sequence_number,
  created_at,
  event_type,
  city,
  state,
  postal_code,
  browser,
  traffic_source,
  uri,
  ip_address
from sujeet_data_analytics_workspace.silver_dev.stg_look__events