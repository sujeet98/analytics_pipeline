
      create or replace temporary view events__dbt_tmp as
      -- Purpose: Entity table for events at event_id grain
select
  event_id,
  user_id,
  session_id,
  sequence_number,
  event_type,
  browser,
  traffic_source,
  uri,
  created_at,
  event_date
from sujeet_data_analytics_workspace.silver_dev.stg_look__events
    