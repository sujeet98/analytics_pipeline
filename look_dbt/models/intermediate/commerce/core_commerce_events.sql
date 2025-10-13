{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['global_event_id'],
  schema='silver_dev',
  partition_by=['event_date'],
  cluster_by=['user_id','session_id'],
  on_schema_change='sync_all_columns',
  tags=['core','commerce','events']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

-- === Per-source aligned inputs (add sources as they come) ===
src_thelook as (
  select
    'look' as source_system,
    event_id,
    user_id,
    session_id,
    sequence_number,
    event_ts,
    event_date,
    ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    uri,
    event_type,
    canonical_updated_at,   -- from int layer
    ingest_ts_utc
  from {{ ref('int_commerce_events__look') }}
  where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
),

-- Add more sources here

unioned as (
  select * from src_thelook
  -- union all select * from src_shopify
)

select
  concat(source_system, ':', cast(event_id as string)) as global_event_id,  -- GLOBAL BK
  source_system,
  event_id,                               -- source BK (keep for audit)
  user_id,
  session_id,
  sequence_number,
  event_ts,
  event_date,
  ip_address,
  city,
  state,
  postal_code,
  browser,
  traffic_source,
  uri,
  event_type,
  canonical_updated_at,
  ingest_ts_utc
from unioned;
