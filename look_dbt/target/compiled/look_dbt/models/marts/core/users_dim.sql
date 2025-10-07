-- Conformed User dimension (safe attributes by default).



select
  user_id,
  email,
  first_name,
  last_name,
  age,
  gender,
  city,
  state,
  country,
  traffic_source,
  created_at,
  src_ingest_ts
from sujeet_data_analytics_workspace.silver_dev.stg_look__users;