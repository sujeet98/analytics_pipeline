



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

src as (
  select
    cast(id as bigint)                       as product_id,
    cast(cost as double)                     as unit_cost,
    nullif(trim(category),'')                as category_raw,
    nullif(trim(name),'')                    as product_name_raw,
    nullif(trim(brand),'')                   as brand_raw,
    cast(retail_price as double)             as retail_price_raw,
    nullif(trim(department),'')              as department_raw,
    nullif(trim(sku),'')                     as sku_raw,
    cast(distribution_center_id as bigint)   as distribution_center_id,
    cast(ingest_ts_utc as timestamp)         as ingest_ts_utc,
    cast(ingest_date as string)              as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`products`
  
),

normalized as (
  select
    product_id,
    unit_cost,
    upper(category_raw)            as category,
    product_name_raw               as product_name,   -- keep case as-is for display
    upper(brand_raw)               as brand,
    round(retail_price_raw, 2)     as retail_price,
    upper(department_raw)          as department,
    upper(sku_raw)                 as sku,
    distribution_center_id,
    ingest_ts_utc,
    _ingest_date
  from src
),

dedup as (
  select *
  from (
    select *,
      row_number() over (partition by product_id order by ingest_ts_utc desc nulls last) as rn
    from normalized
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