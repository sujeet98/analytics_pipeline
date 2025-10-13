{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_order_item_id'],
  schema='silver_dev',
  partition_by=['created_date'],              
  cluster_by=['product_id','user_id','order_id'], 
  on_schema_change='sync_all_columns',
  tags=['core','commerce','orders']
) }}

{% set lookback_days = 14 %}

with tgt_max as (
  select
    {% if is_incremental() %}
      coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}
      timestamp('1900-01-01')
    {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

-- ===== Add per-source aligned inputs (one CTE per source) =====
src_look as (
  select
    'look'            as source_system,
    order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    item_status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    item_date,                -- analytic convenience
    created_date,             -- partition key (stable)
    cast(sale_price as numeric) as sale_price,
    canonical_updated_at,
    ingest_ts_utc
  from {{ ref('int_commerce_order_items__look') }}
  {% if is_incremental() %}
    where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
),

unioned as (
  select * from src_look
  -- union all select * from src_<others> with the same filter
)

select
  concat(source_system, ':', cast(order_item_id as string)) as global_order_item_id,
  source_system,
  order_item_id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  item_status,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  item_date,
  created_date,
  sale_price,
  canonical_updated_at,
  ingest_ts_utc
from unioned;
