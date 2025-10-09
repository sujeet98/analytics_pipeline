{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='inventory_item_id',
  schema='silver_dev',
  partition_by=['created_date'],       
  on_schema_change='sync_all_columns',
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
    id as inventory_item_id,
    product_id,
    cast(created_at as timestamp) as created_at,
    cast(sold_at as timestamp)    as sold_at,
    unit_cost,
    product_category, product_name, product_brand,
    retail_price,
    product_department, product_sku,
    product_distribution_center_id as distribution_center_id,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__inventory_items') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)
select
  inventory_item_id, product_id,
  created_at, sold_at,
  date(created_at) as created_date,
  unit_cost, retail_price,
  product_category, product_name, product_brand,
  product_department, product_sku,
  distribution_center_id,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
