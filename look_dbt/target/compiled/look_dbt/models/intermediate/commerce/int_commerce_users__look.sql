



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
)

select
  id as user_id,
  first_name, last_name, email,
  age, gender,
  state, street_address, postal_code, city, country,
  latitude, longitude,
  traffic_source,
  cast(created_at as timestamp)  as created_at,
  date(created_at)               as user_created_date,
  'look'                         as source_system,
  cast(ingest_ts_utc as timestamp) as canonical_updated_at,
  cast(ingest_ts_utc as timestamp) as ingest_ts_utc
from sujeet_data_analytics_workspace.silver_dev.stg_look__users
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max));