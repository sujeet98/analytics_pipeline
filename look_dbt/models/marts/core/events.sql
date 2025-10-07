{{ config(
  materialized='incremental',
  unique_key='event_id',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns'
) }}

with src as (
  select
    event_id,
    user_id,
    event_type,
    created_at,
    browser,
    traffic_source,
    uri,
    city, state, postal_code, ip_address,
    src_ingest_ts
  from {{ ref('stg_look__events') }}
  {% if is_incremental() %}
    where src_ingest_ts >= (
      select coalesce(dateadd('day', -2, max(src_ingest_ts)), timestamp '1970-01-01')
      from {{ this }}
    )
  {% endif %}
)

select * from src
