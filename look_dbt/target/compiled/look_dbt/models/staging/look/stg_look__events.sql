

with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__events)
select
  id as event_id,
  user_id,
  sequence_number,
  session_id,
  cast(created_at as timestamp) as created_at,
  nullif(trim(ip_address), '') as ip_address,
  nullif(trim(city), '')       as city,
  nullif(trim(state), '')      as state,
  nullif(trim(postal_code), '') as postal_code,
  nullif(trim(browser), '')     as browser,
  nullif(trim(traffic_source), '') as traffic_source,
  nullif(trim(uri), '')            as uri,
  case lower(event_type)
  when 'product'         then 'product'
  when 'cart'            then 'cart'
  when 'home'            then 'home'
  when 'cancel'          then 'cancel'
  when 'purchase'        then 'purchase'
  when 'department'      then 'department'
  else 'unknown'
end as event_type,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b