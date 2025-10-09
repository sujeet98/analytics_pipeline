

with src as (
  select
    cast(id as bigint)               as distribution_center_id,
    nullif(name,'')                  as name,
    cast(latitude as double)         as latitude,
    cast(longitude as double)        as longitude,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc,
    cast(ingest_date as string)      as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`distribution_centers`
  
    where ingest_ts_utc >= dateadd(day, -2, (select coalesce(max(ingest_ts_utc), '1900-01-01') from sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers))
  
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by distribution_center_id order by ingest_ts_utc desc nulls last) as rn
    from src
  ) where rn = 1
)
select
  distribution_center_id,
  name,
  latitude,
  longitude,
  ingest_ts_utc,
  _ingest_date
from dedup;