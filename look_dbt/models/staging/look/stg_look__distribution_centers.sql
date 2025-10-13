{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='distribution_center_id',
    tags=['staging','look']
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
    cast(id as bigint)               as distribution_center_id,
    nullif(trim(name),'')            as name,
    cast(latitude as double)         as latitude,
    cast(longitude as double)        as longitude,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc,
    cast(ingest_date as string)      as _ingest_date
  from {{ source('look','distribution_centers') }}

  {% if is_incremental() %}
    where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by distribution_center_id order by ingest_ts_utc desc nulls last) as rn
    from src
  ) where rn = 1
)
select
  distribution_center_id,
  name,
  latitude,
  longitude,
  ingest_ts_utc,
  _ingest_date
from dedup;
