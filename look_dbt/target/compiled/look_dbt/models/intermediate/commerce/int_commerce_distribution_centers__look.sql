



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
src as (
  select
    distribution_center_id,
    name, latitude, longitude,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max))
)
select
  distribution_center_id,
  name, latitude, longitude,
  date(ingest_ts_utc) as _snap_date,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;