{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_id',
  schema='silver_dev',
  partition_by=['order_date'],      
  tags=['intermediate','commerce','look']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %}
      coalesce(max(ingest_ts_utc), timestamp('1900-01-01'))
    {% else %}
      timestamp('1900-01-01')
    {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) as _ {% endif %}
),

src as (
  select
    order_id,
    user_id,
    order_status,
    cast(created_at   as timestamp) as created_at,
    cast(shipped_at   as timestamp) as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(returned_at  as timestamp) as returned_at,
    num_of_item,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__orders') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)

select
  order_id, user_id, order_status,
  date(created_at) as order_date,
  created_at, shipped_at, delivered_at, returned_at,
  num_of_item,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
