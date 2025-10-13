{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_product_id','valid_from'],
  schema='silver_dev',
  partition_by=['valid_from_date'],
  cluster_by=['global_product_id'],
  on_schema_change='sync_all_columns',
  tags=['core','commerce','scd2','products']
) }}

-- 1) Versioned rows from per-source snapshots
with snap as (
  select
    source_system,
    product_id,
    product_name,
    category,
    brand,
    department,
    sku,
    cast(retail_price as numeric)          as retail_price,
    distribution_center_id,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('snap_products__look') }}

  -- union all other sources here
),

-- 2) Global BK across sources
keyed as (
  select
    concat(source_system, ':', cast(product_id as string)) as global_product_id,
    *
  from snap
),

-- 3) Normalize windows + convenience date
normalized as (
  select
    global_product_id,
    source_system,
    product_id,
    product_name,
    category,
    brand,
    department,
    sku,
    retail_price,
    distribution_center_id,
    dbt_valid_from                                          as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31'))         as valid_to,
    dbt_valid_to is null                                    as is_current,
    date(dbt_valid_from)                                    as valid_from_date
  from keyed
)

-- 4) Emit one row per version with SCD2 surrogate key
select
  {{ dbt_utils.generate_surrogate_key(['global_product_id','valid_from']) }} as product_sk,
  global_product_id,
  source_system,
  product_id,
  product_name,
  category,
  brand,
  department,
  sku,
  retail_price,
  distribution_center_id,
  valid_from,
  valid_to,
  is_current,
  valid_from_date
from normalized;
