{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_order_item_id'],
  schema='silver_dev',
  partition_by=['item_date'],
  cluster_by=['product_id','user_id','order_id'],
  on_schema_change='sync_all_columns',
  tags=['core','commerce','orders']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

-- ===== Per-source aligned inputs (add more sources as needed) =====
src_thelook as (
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
    item_date,                       -- already chosen in int layer
    cast(sale_price as numeric) as sale_price,
    canonical_updated_at,
    ingest_ts_utc
  from {{ ref('int_commerce_order_items__look') }}
  where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
),

-- src_other here

unioned as (
  select * from src_thelook
  -- union all select * from src_other
)

select
  concat(source_system, ':', cast(order_item_id as string)) as global_order_item_id, -- global BK
  source_system,
  order_item_id,       -- source BKs (kept for audit/drill)
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
  sale_price,
  canonical_updated_at,
  ingest_ts_utc
from unioned;
