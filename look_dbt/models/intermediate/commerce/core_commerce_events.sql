{{ config(
  materialized='incremental',
  incremental_strategy='insert_overwrite',
  unique_key=['global_event_id'],          
  schema='silver_dev',
  partition_by=['event_date'],
  cluster_by=['user_id','session_id'],    
  on_schema_change='sync_all_columns',
  tags=['core','commerce','events']
) }}

{% set lookback_days = 7 %}

with src_thelook as (
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
    canonical_updated_at,
    ingest_ts_utc
  from {{ ref('int_commerce_events__look') }}
  {% if is_incremental() %}
    where event_date >= dateadd(day, -{{ lookback_days }}, current_date())
  {% endif %}
),

unioned as (
  select * from src_thelook
  -- union all select * from other sources with the same columns & filter
)

select
  concat(source_system, ':', cast(event_id as string)) as global_event_id,
  source_system,
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
  canonical_updated_at,
  ingest_ts_utc
from unioned;
