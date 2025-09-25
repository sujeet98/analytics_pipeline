{{ config(materialized='view') }}

-- Purpose: Stage products with pricing and DC id. No joins.
-- Grain: 1 row per product_id (latest)

with source as (
  select * from {{ source('look','products') }}
), renamed as (
  select
    cast(id as bigint)             as product_id,
    {{ as_money_2('cost') }}       as unit_cost,
    {{ clean_lower('category') }}  as category,
    name,
    brand,
    {{ as_money_2('retail_price') }} as retail_price,
    {{ clean_lower('department') }} as department,
    sku,
    cast(distribution_center_id as bigint) as distribution_center_id,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select * from renamed where _rn = 1
