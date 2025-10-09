



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
unioned as (
  select * from sujeet_data_analytics_workspace.silver_dev.int_commerce_users__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
),
ranked as (
  select u.*,
         row_number() over (partition by user_id
                            order by canonical_updated_at desc,
                                     case source_system when 'look' then 1 else 99 end) as rn
  from unioned u
)
select
  user_id,
  first_name, last_name, email,
  age, gender,
  state, street_address, postal_code, city, country,
  latitude, longitude,
  traffic_source,
  created_at, user_created_date,
  source_system,
  canonical_updated_at,
  canonical_updated_at as ingest_ts_utc
from ranked
where rn = 1;