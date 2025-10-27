{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='event_id',
  partition_by=['event_date'],
  cluster_by=['user_id','session_id'],  
  tags=['intermediate','commerce','look']
) }}

{% set lookback_days = 7 %}

with src as (
  select
    event_id,
    user_id, sequence_number, session_id,
    event_ts, event_date,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri, event_type,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__events') }}
  {% if is_incremental() %}
    where event_date >= dateadd(day, -{{ lookback_days }}, current_date())
  {% endif %}
)

select
  event_id, user_id, sequence_number, session_id,
  event_ts, event_date,
  ip_address, city, state, postal_code,
  browser, traffic_source, uri, event_type,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
