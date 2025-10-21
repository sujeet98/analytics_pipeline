{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_inventory_item_id'],
  schema='silver_dev',
  partition_by=['created_date'],                 
  cluster_by=['product_id','distribution_center_id'],  
  tags=['core','commerce','inventory']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

src_look as (
  select
    'look'              as source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,
    created_at,
    sold_at,
    created_date,
    unit_cost,
    retail_price,
    product_category,
    product_name,
    product_brand,
    product_department,
    product_sku,
    canonical_updated_at,
    ingest_ts_utc
  from {{ ref('int_commerce_inventory_items__look') }}
  {% if is_incremental() %}
    where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
),

unioned as (
  select * from src_look
  -- union all select * from other sources (same columns & filter)
)

select
  concat(source_system, ':', cast(inventory_item_id as string)) as global_inventory_item_id,
  source_system,
  inventory_item_id,
  product_id,
  distribution_center_id,
  created_at,
  sold_at,
  created_date,
  unit_cost,
  retail_price,
  product_category,
  product_name,
  product_brand,
  product_department,
  product_sku,
  canonical_updated_at,
  ingest_ts_utc
from unioned;
