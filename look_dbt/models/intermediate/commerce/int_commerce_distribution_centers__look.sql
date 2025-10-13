{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='distribution_center_id',
  schema='silver_dev',        
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
    distribution_center_id,
    name, latitude, longitude,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ ref('stg_look__distribution_centers') }}
  where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)
select
  distribution_center_id,
  name, latitude, longitude,
  date(ingest_ts_utc) as _snap_date,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;
