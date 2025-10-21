{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='user_id',
  schema='silver_dev',
  tags=['intermediate','commerce','look']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(ingest_ts_utc), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
)

select
  id as user_id,
  first_name, last_name, email,
  age, gender,
  state, street_address, postal_code, city, country,
  latitude, longitude,
  traffic_source,
  cast(created_at as timestamp)  as created_at,
  date(created_at)               as user_created_date,
  'look'                         as source_system,
  cast(ingest_ts_utc as timestamp) as canonical_updated_at,
  cast(ingest_ts_utc as timestamp) as ingest_ts_utc
from {{ ref('stg_look__users') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max));