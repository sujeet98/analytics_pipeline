{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='event_id',
  schema='silver_dev',
  partition_by=['event_date'],        
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
    id as event_id,
    user_id, sequence_number, session_id,
    event_ts, event_date,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri, event_type,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__events') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
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
