{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_item_id',
  schema='silver_dev',
  partition_by=['created_date'],  
  tags=['intermediate','commerce','look']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(ingest_ts_utc), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

src as (
  select
    id                             as order_item_id,
    order_id, user_id, product_id, inventory_item_id,
    item_status,
    cast(created_at   as timestamp) as created_at,
    cast(shipped_at   as timestamp) as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(returned_at  as timestamp) as returned_at,
    sale_price,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc,
    created_date
  from {{ ref('stg_look__order_items') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)

select
  order_item_id, order_id, user_id, product_id, inventory_item_id,
  item_status,
  created_at, shipped_at, delivered_at, returned_at,
  -- keep analytic date, but don't partition on it
  date(coalesce(delivered_at, created_at)) as item_date,
  created_date,
  sale_price,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
