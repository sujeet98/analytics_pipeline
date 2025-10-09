{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='product_id',
  schema='silver_dev',
  partition_by=['product_snap_date'], 
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
    product_id,
    product_name, brand, category, department, sku,
    unit_cost, retail_price,
    distribution_center_id,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__products') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)
select
  product_id,
  product_name, brand, category, department, sku,
  unit_cost, retail_price,
  distribution_center_id,
  date(ingest_ts_utc) as product_snap_date,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
