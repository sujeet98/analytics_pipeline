

with src as (
  select
    cast(id as bigint)                 as product_id,
    cast(cost as double)               as unit_cost,
    nullif(category,'')                as category,
    nullif(name,'')                    as product_name,
    nullif(brand,'')                   as brand,
    cast(retail_price as double)       as retail_price,
    nullif(department,'')              as department,
    nullif(sku,'')                     as sku,
    cast(distribution_center_id as bigint) as distribution_center_id,
    cast(ingest_ts_utc as timestamp)   as ingest_ts_utc,
    cast(ingest_date as string)        as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`products`
  
    where ingest_ts_utc >= dateadd(day, -2, (select coalesce(max(ingest_ts_utc), '1900-01-01') from sujeet_data_analytics_workspace.silver_dev.stg_look__products))
  
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by product_id order by ingest_ts_utc desc nulls last) as rn
    from src
  ) where rn = 1
)
select
  product_id,
  unit_cost,
  category,
  product_name,
  brand,
  retail_price,
  department,
  sku,
  distribution_center_id,
  ingest_ts_utc,
  _ingest_date
from dedup;