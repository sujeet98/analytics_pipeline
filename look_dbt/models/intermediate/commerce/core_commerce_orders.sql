{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_order_id'],
  schema='silver_dev',
  partition_by=['order_date'],      
  cluster_by=['user_id'],         
  on_schema_change='sync_all_columns',
  tags=['core','commerce','orders']
) }}

{% set lookback_days = 7 %}  -- slightly larger cushion for late updates

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

src_thelook as (
  select
    'look'        as source_system,
    order_id,
    user_id,
    order_status,
    order_date,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item,
    canonical_updated_at,
    ingest_ts_utc
  from {{ ref('int_commerce_orders__look') }}
  {% if is_incremental() %}
    where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
)

-- add more sources with the same filter and columns, then UNION ALL

select
  concat(source_system, ':', cast(order_id as string)) as global_order_id,
  source_system,
  order_id,
  user_id,
  order_status,
  order_date,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  num_of_item,
  canonical_updated_at,
  ingest_ts_utc
from src_thelook;
