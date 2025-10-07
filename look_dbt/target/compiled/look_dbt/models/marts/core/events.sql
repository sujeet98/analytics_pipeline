

with src as (
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
  from sujeet_data_analytics_workspace.silver_dev.stg_look__events
  
)

select * from src