{{ config(materialized='view') }}

-- Purpose: Stage inventory snapshots with costs and DC info. No joins.
-- Grain: 1 row per inventory_item_id (latest)

with source as (
  select * from {{ source('look','inventory_items') }}
), renamed as (
  select
    cast(id as bigint)                   as inventory_item_id,
    cast(product_id as bigint)           as product_id,
    {{ clean_ts('created_at') }}         as created_at,
    {{ clean_ts('sold_at') }}            as sold_at,
    {{ as_money_2('cost') }}             as unit_cost,
    {{ clean_lower('product_category') }}    as product_category,
    product_name,
    product_brand,
    {{ as_money_2('product_retail_price') }} as product_retail_price,
    {{ clean_lower('product_department') }}  as product_department,
    product_sku,
    cast(coalesce(product_distribution_center_id, distribution_center_id) as bigint)
      as distribution_center_id,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select * from renamed where _rn = 1
