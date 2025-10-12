



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

src as (
  select
    cast(id as bigint)                       as id,
    cast(product_id as bigint)               as product_id,
    cast(created_at as timestamp)            as created_at,
    cast(sold_at as timestamp)               as sold_at,
    cast(cost as double)                     as unit_cost,
    nullif(product_category,'')              as product_category,
    nullif(product_name,'')                  as product_name,
    nullif(product_brand,'')                 as product_brand,
    cast(product_retail_price as double)     as retail_price,
    nullif(product_department,'')            as product_department,
    nullif(product_sku,'')                   as product_sku,
    cast(product_distribution_center_id as bigint) as product_distribution_center_id,
    cast(ingest_ts_utc as timestamp)         as ingest_ts_utc,
    cast(ingest_date as string)              as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`inventory_items`
  
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by id order by ingest_ts_utc desc nulls last) as rn
    from src
  ) where rn = 1
)
select
  id,
  product_id,
  created_at,
  sold_at,
  unit_cost,
  product_category,
  product_name,
  product_brand,
  retail_price,
  product_department,
  product_sku,
  product_distribution_center_id,
  ingest_ts_utc,
  _ingest_date
from dedup;